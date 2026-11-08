package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

type CodexAdapter struct {
	Binary string
	Args   []string

	mu       sync.Mutex
	sessions map[string]*codexSession
}

type codexSession struct {
	client *rpcClient

	runsMu       sync.RWMutex
	byThread     map[string]*codexInvocation
	byTurn       map[orchestration.ProviderTurnID]*codexInvocation
	knownThreads map[string]bool
	threadModels map[string]string
}

type codexInvocation struct {
	request        RunRequest
	emit           EmitFunc
	nativeThreadID string
	nativeTurnID   string
	done           chan struct{}
	err            error
	finishOnce     sync.Once
}

func (a *CodexAdapter) Driver() orchestration.ProviderDriverKind { return "codex" }
func (a *CodexAdapter) Capabilities() orchestration.ProviderCapabilities {
	return orchestration.ProviderCapabilities{
		Threads: orchestration.ThreadCapabilities{CanForkThread: true, CanForkFromTurn: true, ExposesNativeThreadID: true},
		Turns: orchestration.TurnCapabilities{ExposesNativeTurnID: true, EmitsTurnStarted: true,
			EmitsTurnCompleted: true, SupportsInterrupt: true, SupportsQueuedMessages: true, TerminalStatusQuality: "strong"},
		Streaming: orchestration.StreamingCapabilities{StreamsAssistantText: true, EmitsMessageCompleted: true},
		Identity: orchestration.IdentityCapabilities{NativeThreadIDs: orchestration.NativeRefStrong,
			NativeTurnIDs: orchestration.NativeRefStrong, NativeItemIDs: orchestration.NativeRefStrong},
	}
}

func (a *CodexAdapter) Run(ctx context.Context, request RunRequest, emit EmitFunc) error {
	session, nativeThreadID, model, err := a.sessionFor(ctx, request, emit)
	if err != nil {
		return err
	}
	invocation := &codexInvocation{request: request, emit: emit, nativeThreadID: nativeThreadID, done: make(chan struct{})}
	session.runsMu.Lock()
	session.byThread[nativeThreadID] = invocation
	session.byTurn[request.ProviderTurn.ID] = invocation
	session.runsMu.Unlock()
	defer func() {
		session.runsMu.Lock()
		delete(session.byThread, nativeThreadID)
		delete(session.byTurn, request.ProviderTurn.ID)
		session.runsMu.Unlock()
	}()

	params := codexTurnStartParams(request, nativeThreadID, model)
	var started struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err := session.client.call(ctx, "turn/start", params, &started); err != nil {
		return err
	}
	if started.Turn.ID != "" {
		request.StateMu.Lock()
		invocation.nativeTurnID = started.Turn.ID
		request.StateMu.Unlock()
		turn := request.currentTurn()
		turn.NativeTurnRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &started.Turn.ID, Strength: orchestration.NativeRefStrong}
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}, json.RawMessage(`{"method":"turn/start","kind":"response"}`)); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-session.client.done:
		return fmt.Errorf("Codex app-server closed during turn: %w", err)
	case <-invocation.done:
		return invocation.err
	}
}

func (a *CodexAdapter) sessionFor(ctx context.Context, request RunRequest, emit EmitFunc) (*codexSession, string, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sessions == nil {
		a.sessions = make(map[string]*codexSession)
	}
	key := request.Session.CWD
	session := a.sessions[key]
	if session == nil {
		binary := a.Binary
		if binary == "" {
			binary = "codex"
		}
		args := append([]string{"app-server"}, a.Args...)
		session = &codexSession{byThread: make(map[string]*codexInvocation),
			byTurn: make(map[orchestration.ProviderTurnID]*codexInvocation), knownThreads: make(map[string]bool),
			threadModels: make(map[string]string)}
		client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: request.Session.CWD,
			Env:            append(os.Environ(), "CODEX_INTERNAL_ORIGINATOR_OVERRIDE=T3 Code"),
			OnNotification: func(raw json.RawMessage) { a.handleNotification(session, raw) },
			OnRequest: func(_ context.Context, request rpcRequest) (any, *rpcError) {
				return map[string]any{"decision": "decline"}, nil
			}})
		if err != nil {
			return nil, "", "", err
		}
		session.client = client
		var initialized struct {
			UserAgent string `json:"userAgent"`
		}
		if err := client.call(ctx, "initialize", map[string]any{
			"clientInfo": map[string]any{"name": "T3 Code", "title": "T3 Code", "version": "0.1.7"},
			"capabilities": map[string]any{"experimentalApi": true,
				"optOutNotificationMethods": []string{"turn/diff/updated"},
				"extensions":                map[string]any{"io.modelcontextprotocol/ui": map[string]any{"mimeTypes": []string{"text/html;profile=mcp-app"}}}},
		}, &initialized); err != nil {
			client.close()
			return nil, "", "", err
		}
		if err := client.notify("initialized", nil); err != nil {
			client.close()
			return nil, "", "", err
		}
		a.sessions[key] = session
	}

	nativeThreadID := ""
	if request.ProviderThread.NativeThreadRef != nil && request.ProviderThread.NativeThreadRef.NativeID != nil {
		nativeThreadID = *request.ProviderThread.NativeThreadRef.NativeID
	}
	model := request.Start.Run.ModelSelection.Model
	method := "thread/start"
	params := map[string]any{"cwd": request.Session.CWD,
		"config": map[string]any{"tools.update_plan.enabled": true, "mcp_optional_startup_grace_ms": 10000}}
	if model != "" && model != "default" {
		params["model"] = model
	}
	if nativeThreadID != "" {
		method = "thread/resume"
		params["threadId"] = nativeThreadID
		params["excludeTurns"] = true
	}
	var response struct {
		Thread struct {
			ID    string `json:"id"`
			Model string `json:"model"`
		} `json:"thread"`
	}
	// A persistent app-server retains an existing native thread between turns.
	if nativeThreadID != "" && sessionHasNativeThread(session, nativeThreadID) {
		if model == "default" {
			model = nativeThreadModel(session, nativeThreadID)
		}
		return session, nativeThreadID, model, nil
	}
	if err := session.client.call(ctx, method, params, &response); err != nil {
		return nil, "", "", err
	}
	if response.Thread.ID == "" {
		return nil, "", "", fmt.Errorf("Codex %s returned no thread id", method)
	}
	nativeThreadID = response.Thread.ID
	if model == "default" && response.Thread.Model != "" {
		model = response.Thread.Model
	}
	markNativeThread(session, nativeThreadID)
	rememberNativeThreadModel(session, nativeThreadID, response.Thread.Model)
	thread := *request.ProviderThread
	thread.NativeThreadRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &nativeThreadID, Strength: orchestration.NativeRefStrong}
	thread.UpdatedAt = time.Now().UTC()
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated, ProviderThread: &thread},
		json.RawMessage(`{"method":"`+method+`","kind":"response"}`)); err != nil {
		return nil, "", "", err
	}
	return session, nativeThreadID, model, nil
}

func sessionHasNativeThread(session *codexSession, nativeID string) bool {
	session.runsMu.RLock()
	known := session.knownThreads[nativeID]
	session.runsMu.RUnlock()
	return known
}

func markNativeThread(session *codexSession, nativeID string) {
	session.runsMu.Lock()
	session.knownThreads[nativeID] = true
	session.runsMu.Unlock()
}

func nativeThreadModel(session *codexSession, nativeID string) string {
	session.runsMu.RLock()
	defer session.runsMu.RUnlock()
	return session.threadModels[nativeID]
}

func rememberNativeThreadModel(session *codexSession, nativeID, model string) {
	session.runsMu.Lock()
	session.threadModels[nativeID] = model
	session.runsMu.Unlock()
}

func (a *CodexAdapter) handleNotification(session *codexSession, raw json.RawMessage) {
	var frame codexFrame
	if json.Unmarshal(raw, &frame) != nil {
		return
	}
	nativeThreadID := frame.Params.ThreadID
	if nativeThreadID == "" {
		nativeThreadID = frame.Params.Thread.ID
	}
	session.runsMu.RLock()
	invocation := session.byThread[nativeThreadID]
	session.runsMu.RUnlock()
	if invocation == nil {
		return
	}
	if frame.Params.Turn.ID != "" {
		invocation.request.StateMu.Lock()
		invocation.nativeTurnID = frame.Params.Turn.ID
		invocation.request.StateMu.Unlock()
	}
	if err := a.handleFrame(context.Background(), invocation.request, raw, invocation.emit); err != nil {
		invocation.finishOnce.Do(func() {
			invocation.err = err
			close(invocation.done)
		})
		return
	}
	if frame.Method == "turn/completed" || frame.Type == "turn.completed" || frame.Type == "turn.failed" {
		invocation.finishOnce.Do(func() { close(invocation.done) })
	}
}

func codexTurnStartParams(request RunRequest, nativeThreadID, model string) map[string]any {
	approvalPolicy := "untrusted"
	approvalsReviewer := "user"
	sandboxType := "readOnly"
	switch request.Start.Thread.RuntimeMode {
	case orchestration.RuntimeAutoAcceptEdits:
		approvalPolicy, sandboxType = "on-request", "workspaceWrite"
	case orchestration.RuntimeAuto:
		approvalPolicy, approvalsReviewer, sandboxType = "on-request", "auto_review", "workspaceWrite"
	case orchestration.RuntimeFullAccess:
		approvalPolicy, sandboxType = "never", "dangerFullAccess"
	}
	interactionMode := request.Start.Thread.InteractionMode
	if interactionMode == "" {
		interactionMode = orchestration.InteractionDefault
	}
	return map[string]any{
		"threadId": nativeThreadID,
		"input":    []any{map[string]any{"type": "text", "text": promptText(request.Start)}},
		"cwd":      request.Session.CWD, "model": model, "summary": "detailed",
		"approvalPolicy": approvalPolicy, "approvalsReviewer": approvalsReviewer,
		"sandboxPolicy":     map[string]any{"type": sandboxType},
		"collaborationMode": map[string]any{"mode": interactionMode, "settings": map[string]any{"model": model}},
	}
}

func (a *CodexAdapter) Interrupt(ctx context.Context, effect orchestration.TurnInterruptEffect) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.runsMu.RLock()
		invocation := session.byTurn[effect.ProviderTurnID]
		session.runsMu.RUnlock()
		if invocation != nil {
			invocation.request.StateMu.Lock()
			nativeTurnID := invocation.nativeTurnID
			invocation.request.StateMu.Unlock()
			if nativeTurnID != "" {
				return session.client.call(ctx, "turn/interrupt", map[string]any{
					"threadId": invocation.nativeThreadID, "turnId": nativeTurnID,
				}, nil)
			}
		}
	}
	return fmt.Errorf("Codex turn %s is not active: %w", effect.ProviderTurnID, orchestration.ErrUnsupportedCapability)
}

func (a *CodexAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.client.close()
	}
	a.sessions = nil
	return nil
}

type codexFrame struct {
	Type     string `json:"type"`
	ThreadID string `json:"thread_id"`
	Method   string `json:"method"`
	Params   struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		ItemID   string `json:"itemId"`
		Delta    string `json:"delta"`
		Thread   struct {
			ID string `json:"id"`
		} `json:"thread"`
		Turn struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"turn"`
		Item struct {
			ID   string `json:"id"`
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"item"`
		TokenUsage struct {
			Last struct {
				TotalTokens           int `json:"totalTokens"`
				InputTokens           int `json:"inputTokens"`
				CachedInputTokens     int `json:"cachedInputTokens"`
				OutputTokens          int `json:"outputTokens"`
				ReasoningOutputTokens int `json:"reasoningOutputTokens"`
			} `json:"last"`
			ModelContextWindow int `json:"modelContextWindow"`
		} `json:"tokenUsage"`
	} `json:"params"`
	Result struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	} `json:"result"`
	Item struct {
		ID   string `json:"id"`
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"item"`
	Usage struct {
		InputTokens           int `json:"input_tokens"`
		CachedInputTokens     int `json:"cached_input_tokens"`
		OutputTokens          int `json:"output_tokens"`
		ReasoningOutputTokens int `json:"reasoning_output_tokens"`
	} `json:"usage"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (a *CodexAdapter) handleFrame(ctx context.Context, request RunRequest, raw json.RawMessage, emit EmitFunc) error {
	var frame codexFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return err
	}
	threadNativeID := frame.ThreadID
	if frame.Params.Thread.ID != "" {
		threadNativeID = frame.Params.Thread.ID
	}
	if frame.Result.Thread.ID != "" {
		threadNativeID = frame.Result.Thread.ID
	}
	if threadNativeID != "" {
		thread := request.currentProviderThread()
		thread.NativeThreadRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &threadNativeID, Strength: orchestration.NativeRefStrong}
		thread.UpdatedAt = time.Now().UTC()
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated, ProviderThread: &thread}, raw); err != nil {
			return err
		}
	}
	turnNativeID := frame.Params.TurnID
	if frame.Params.Turn.ID != "" {
		turnNativeID = frame.Params.Turn.ID
	}
	if frame.Result.Turn.ID != "" {
		turnNativeID = frame.Result.Turn.ID
	}
	if turnNativeID != "" {
		turn := request.currentTurn()
		turn.NativeTurnRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &turnNativeID, Strength: orchestration.NativeRefStrong}
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}, raw); err != nil {
			return err
		}
	}

	item := frame.Item
	if frame.Params.Item.ID != "" {
		item = frame.Params.Item
	}
	if item.ID != "" && (item.Type == "agent_message" || item.Type == "agentMessage") && item.Text != "" {
		if err := emitAssistant(ctx, request, emit, item.ID, item.Text, false, raw); err != nil {
			return err
		}
	}
	if frame.Method == "item/agentMessage/delta" && frame.Params.ItemID != "" {
		// The completed item carries the full response; deltas are diagnostics in
		// this text-only adapter and do not duplicate assistant content.
		return nil
	}

	usage := frame.Usage
	if frame.Params.TokenUsage.Last.TotalTokens != 0 {
		usage.InputTokens = frame.Params.TokenUsage.Last.InputTokens
		usage.CachedInputTokens = frame.Params.TokenUsage.Last.CachedInputTokens
		usage.OutputTokens = frame.Params.TokenUsage.Last.OutputTokens
		usage.ReasoningOutputTokens = frame.Params.TokenUsage.Last.ReasoningOutputTokens
	}
	if usage.InputTokens != 0 || usage.OutputTokens != 0 {
		turn := request.currentTurn()
		turn.TokenUsage = &orchestration.TokenUsage{UsedTokens: usage.InputTokens + usage.OutputTokens,
			InputTokens: usage.InputTokens, CachedInputTokens: usage.CachedInputTokens,
			OutputTokens: usage.OutputTokens, ReasoningOutputTokens: usage.ReasoningOutputTokens,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if frame.Params.TokenUsage.ModelContextWindow != 0 {
			turn.TokenUsage.MaxTokens = ref(frame.Params.TokenUsage.ModelContextWindow)
		}
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}, raw); err != nil {
			return err
		}
	}
	if frame.Type == "turn.failed" || frame.Type == "error" || frame.Params.Turn.Status == "failed" {
		message := frame.Error.Message
		if frame.Params.Turn.Error != nil {
			message = frame.Params.Turn.Error.Message
		}
		if strings.TrimSpace(message) == "" {
			message = "Codex turn failed."
		}
		return &TurnError{Class: "unknown", Message: message}
	}
	if frame.Type == "turn.completed" || frame.Method == "turn/completed" {
		if frame.Params.Turn.Status == "interrupted" || frame.Params.Turn.Status == "cancelled" {
			request.setTurnStatus(orchestration.ProviderTurnInterrupted)
		}
		return nil
	}
	return nil
}
