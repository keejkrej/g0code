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

type PiAdapter struct {
	Binary     string
	SessionDir string

	mu               sync.Mutex
	sessions         map[orchestration.ProviderThreadID]*piSession
	modelsDiscovered bool
}

type piSession struct {
	client      *rpcClient
	nativeID    string
	mu          sync.Mutex
	currentMu   sync.RWMutex
	current     *piInvocation
	model       string
	sessionName string
	runtimeMode orchestration.RuntimeMode
	broker      *requestBroker
}

type piInvocation struct {
	request    RunRequest
	emit       EmitFunc
	done       chan struct{}
	err        error
	finishOnce sync.Once
}

func (a *PiAdapter) Driver() orchestration.ProviderDriverKind { return "pi" }
func (a *PiAdapter) Capabilities() orchestration.ProviderCapabilities {
	return orchestration.ProviderCapabilities{
		Threads: orchestration.ThreadCapabilities{CanReadThreadSnapshot: true, ExposesNativeThreadID: true},
		Turns: orchestration.TurnCapabilities{EmitsTurnStarted: true, EmitsTurnCompleted: true,
			SupportsInterrupt: true, SupportsQueuedMessages: true, TerminalStatusQuality: "strong"},
		Streaming: orchestration.StreamingCapabilities{StreamsAssistantText: true, StreamsReasoning: true, EmitsMessageCompleted: true},
		Identity: orchestration.IdentityCapabilities{NativeThreadIDs: orchestration.NativeRefStrong,
			NativeTurnIDs: orchestration.NativeRefStrong, NativeItemIDs: orchestration.NativeRefStrong},
	}
}

func (a *PiAdapter) Run(ctx context.Context, request RunRequest, emit EmitFunc) error {
	session, err := a.sessionFor(ctx, request, emit)
	if err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := a.applySelection(ctx, session, request); err != nil {
		return err
	}
	invocation := &piInvocation{request: request, emit: emit, done: make(chan struct{})}
	session.currentMu.Lock()
	session.current = invocation
	session.currentMu.Unlock()
	defer func() {
		session.currentMu.Lock()
		session.current = nil
		session.currentMu.Unlock()
	}()
	if err := session.client.sendRecord(map[string]any{"type": "prompt", "message": promptText(request.Start),
		"streamingBehavior": "steer"}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-session.client.done:
		return fmt.Errorf("Pi RPC closed during turn: %w", err)
	case <-invocation.done:
	}
	if invocation.err != nil {
		return invocation.err
	}
	for _, command := range []string{"get_state", "get_entries", "get_session_stats"} {
		var frame piFrame
		if err := session.client.requestRecord(ctx, map[string]any{"type": command}, &frame); err != nil {
			return err
		}
		raw, _ := json.Marshal(frame)
		if err := a.handleFrame(ctx, request, raw, emit); err != nil {
			return err
		}
	}
	return nil
}

func (a *PiAdapter) sessionFor(ctx context.Context, request RunRequest, emit EmitFunc) (*piSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sessions == nil {
		a.sessions = make(map[orchestration.ProviderThreadID]*piSession)
	}
	if existing := a.sessions[request.ProviderThread.ID]; existing != nil {
		if existing.runtimeMode == request.Start.Thread.RuntimeMode {
			return existing, nil
		}
		existing.broker.cancelAll()
		existing.client.close()
		delete(a.sessions, request.ProviderThread.ID)
	}
	binary := a.Binary
	if binary == "" {
		binary = "pi"
	}
	args := []string{"--mode", "rpc", "--tools", "read,grep,find,ls,bash,edit,write", "--no-extensions", "--no-mcp", "--no-skills",
		"--no-prompt-templates", "--no-context-files", "--no-approve", "--offline"}
	extensionPath, err := a.permissionExtension()
	if err != nil {
		return nil, err
	}
	args = append(args, "--extension", extensionPath)
	if a.SessionDir != "" {
		args = append(args, "--session-dir", a.SessionDir)
	}
	session := &piSession{runtimeMode: request.Start.Thread.RuntimeMode, broker: newRequestBroker()}
	client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: request.Session.CWD, IDPrefix: "g0code-",
		Env:            append(os.Environ(), "G0CODE_RUNTIME_MODE="+string(request.Start.Thread.RuntimeMode)),
		OnNotification: func(raw json.RawMessage) { a.handleNotification(session, raw) },
		OnResponse:     func(raw json.RawMessage) { a.handleNotification(session, raw) }})
	if err != nil {
		return nil, err
	}
	session.client = client
	fail := func(err error) (*piSession, error) {
		client.close()
		return nil, err
	}
	if request.ProviderThread.NativeThreadRef != nil && request.ProviderThread.NativeThreadRef.NativeID != nil {
		var response piFrame
		if err := client.requestRecord(ctx, map[string]any{"type": "switch_session",
			"sessionPath": *request.ProviderThread.NativeThreadRef.NativeID}, &response); err != nil {
			return fail(err)
		}
		if !response.Success {
			return fail(fmt.Errorf("Pi switch_session failed"))
		}
	}
	var state piFrame
	if err := client.requestRecord(ctx, map[string]any{"type": "get_state"}, &state); err != nil {
		return fail(err)
	}
	if !state.Success || state.Data.SessionFile == "" {
		return fail(fmt.Errorf("Pi get_state returned no persisted sessionFile"))
	}
	if !a.modelsDiscovered {
		var models piFrame
		if err := client.requestRecord(ctx, map[string]any{"type": "get_available_models"}, &models); err == nil {
			a.modelsDiscovered = true
		}
	}
	var baseline piFrame
	if err := client.requestRecord(ctx, map[string]any{"type": "get_entries"}, &baseline); err != nil {
		return fail(err)
	}
	session.nativeID = state.Data.SessionFile
	session.model = ""
	thread := *request.ProviderThread
	thread.NativeThreadRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &session.nativeID, Strength: orchestration.NativeRefStrong}
	thread.UpdatedAt = time.Now().UTC()
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated, ProviderThread: &thread},
		json.RawMessage(`{"type":"response","command":"get_state"}`)); err != nil {
		return fail(err)
	}
	a.sessions[request.ProviderThread.ID] = session
	return session, nil
}

func (a *PiAdapter) applySelection(ctx context.Context, session *piSession, request RunRequest) error {
	model := request.Start.Run.ModelSelection.Model
	if model != "" && model != "default" && model != session.model {
		provider, modelID, ok := strings.Cut(model, "/")
		if !ok || provider == "" || modelID == "" {
			return fmt.Errorf("Pi model %q must use provider/model format", model)
		}
		var response piFrame
		if err := session.client.requestRecord(ctx, map[string]any{"type": "set_model",
			"provider": provider, "modelId": modelID}, &response); err != nil {
			return err
		}
		if !response.Success {
			return fmt.Errorf("Pi set_model failed for %s", model)
		}
		session.model = model
	}
	if title := request.Start.Thread.Title; title != "" && title != session.sessionName {
		var response piFrame
		if err := session.client.requestRecord(ctx, map[string]any{"type": "set_session_name", "name": title}, &response); err != nil {
			return err
		}
		session.sessionName = title
	}
	return nil
}

func (a *PiAdapter) handleNotification(session *piSession, raw json.RawMessage) {
	session.currentMu.RLock()
	invocation := session.current
	session.currentMu.RUnlock()
	if invocation == nil {
		return
	}
	var frame piFrame
	if json.Unmarshal(raw, &frame) != nil {
		return
	}
	if frame.Type == "extension_ui_request" && frame.Method == "confirm" {
		var extension struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &extension)
		frame.Text = extension.Message
		go a.answerExtensionConfirmation(session, invocation, frame)
		return
	}
	if frame.Type == "turn_end" && frame.Message.StopReason == "aborted" {
		invocation.request.setTurnStatus(orchestration.ProviderTurnInterrupted)
	}
	if frame.Type == "agent_settled" && frame.Aborted {
		invocation.request.setTurnStatus(orchestration.ProviderTurnInterrupted)
	}
	if err := a.handleFrame(context.Background(), invocation.request, raw, invocation.emit); err != nil {
		invocation.finishOnce.Do(func() {
			invocation.err = err
			close(invocation.done)
		})
		return
	}
	if frame.Type == "agent_settled" {
		invocation.finishOnce.Do(func() { close(invocation.done) })
	}
}

func (a *PiAdapter) answerExtensionConfirmation(session *piSession, invocation *piInvocation, frame piFrame) {
	response, err := session.broker.ask(context.Background(), invocation.request, invocation.emit,
		frame.ID, orchestration.RequestPermission, frame.Title, frame.Text)
	confirmed := false
	cancelled := false
	if err != nil {
		cancelled = true
	} else if response.Decision != nil {
		confirmed = *response.Decision == "accept" || *response.Decision == "acceptForSession" || *response.Decision == "acceptAlways"
		cancelled = *response.Decision == "cancel"
	}
	_ = session.client.sendRecord(map[string]any{"type": "extension_ui_response", "id": frame.ID,
		"confirmed": confirmed, "cancelled": cancelled})
}

func (a *PiAdapter) RespondRuntimeRequest(_ context.Context, response orchestration.RuntimeRequestRespondEffect) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		if response.Request.NativeRequestRef != nil && response.Request.NativeRequestRef.NativeID != nil {
			session.broker.mu.Lock()
			_, ok := session.broker.pending[*response.Request.NativeRequestRef.NativeID]
			session.broker.mu.Unlock()
			if ok {
				return session.broker.respond(response)
			}
		}
	}
	return fmt.Errorf("Pi runtime request is not live: %w", orchestration.ErrRequestNotRespondable)
}

func (a *PiAdapter) Interrupt(ctx context.Context, effect orchestration.TurnInterruptEffect) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.currentMu.RLock()
		current := session.current
		session.currentMu.RUnlock()
		if current != nil && current.request.currentTurn().ID == effect.ProviderTurnID {
			return session.client.requestRecord(ctx, map[string]any{"type": "abort"}, nil)
		}
	}
	return fmt.Errorf("Pi turn %s is not active: %w", effect.ProviderTurnID, orchestration.ErrUnsupportedCapability)
}

func (a *PiAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.broker.cancelAll()
		session.client.close()
	}
	a.sessions = nil
	return nil
}

type piContent struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	Thinking string `json:"thinking"`
}

func (c *piContent) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return nil
	}
	type contentAlias piContent
	return json.Unmarshal(data, (*contentAlias)(c))
}

type piContentList []piContent

func (c *piContentList) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		return nil
	}
	type listAlias piContentList
	return json.Unmarshal(data, (*listAlias)(c))
}

type piMessage struct {
	Role       string        `json:"role"`
	ResponseID string        `json:"responseId"`
	StopReason string        `json:"stopReason"`
	Content    piContentList `json:"content"`
	Usage      struct {
		Input       int `json:"input"`
		Output      int `json:"output"`
		CacheRead   int `json:"cacheRead"`
		TotalTokens int `json:"totalTokens"`
		Reasoning   int `json:"reasoning"`
	} `json:"usage"`
}

func (m *piMessage) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		// Extension UI requests use message as plain text rather than a chat message.
		return nil
	}
	type messageAlias piMessage
	return json.Unmarshal(data, (*messageAlias)(m))
}

type piFrame struct {
	Type          string          `json:"type"`
	ID            string          `json:"id"`
	Command       string          `json:"command"`
	Success       bool            `json:"success"`
	Aborted       bool            `json:"aborted"`
	Method        string          `json:"method"`
	Title         string          `json:"title"`
	Text          string          `json:"-"`
	ToolCallID    string          `json:"toolCallId"`
	ToolName      string          `json:"toolName"`
	Args          json.RawMessage `json:"args"`
	PartialResult json.RawMessage `json:"partialResult"`
	Result        json.RawMessage `json:"result"`
	IsError       bool            `json:"isError"`
	Message       piMessage       `json:"message"`
	Usage         struct {
		Input       int `json:"input"`
		Output      int `json:"output"`
		CacheRead   int `json:"cacheRead"`
		TotalTokens int `json:"totalTokens"`
		Reasoning   int `json:"reasoning"`
	} `json:"usage"`
	AssistantMessageEvent struct {
		Type         string `json:"type"`
		ContentIndex int    `json:"contentIndex"`
		Content      string `json:"content"`
	} `json:"assistantMessageEvent"`
	Data struct {
		SessionID   string `json:"sessionId"`
		SessionFile string `json:"sessionFile"`
		Entries     []struct {
			ID      string    `json:"id"`
			Type    string    `json:"type"`
			Message piMessage `json:"message"`
		} `json:"entries"`
		Tokens struct {
			Input     int `json:"input"`
			Output    int `json:"output"`
			CacheRead int `json:"cacheRead"`
		} `json:"tokens"`
		ContextUsage struct {
			Tokens        int `json:"tokens"`
			ContextWindow int `json:"contextWindow"`
		} `json:"contextUsage"`
	} `json:"data"`
}

func (a *PiAdapter) handleFrame(ctx context.Context, request RunRequest, raw json.RawMessage, emit EmitFunc) error {
	var frame piFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return err
	}
	if strings.HasPrefix(frame.Type, "tool_execution_") && frame.ToolCallID != "" {
		itemType := "dynamic_tool"
		switch frame.ToolName {
		case "bash":
			itemType = "command_execution"
		case "edit", "write":
			itemType = "file_change"
		}
		status := orchestration.TurnItemRunning
		output := frame.PartialResult
		if frame.Type == "tool_execution_end" {
			status = orchestration.TurnItemCompleted
			output = frame.Result
			if frame.IsError {
				status = orchestration.TurnItemFailed
			}
		}
		return emitToolCall(ctx, request, emit, ToolCall{ID: frame.ToolCallID, Type: itemType,
			Title: frame.ToolName, Status: status, Input: frame.Args, Output: output}, raw)
	}
	nativeSessionID := frame.ID
	if frame.Type != "session" {
		nativeSessionID = ""
	}
	if frame.Data.SessionID != "" {
		nativeSessionID = frame.Data.SessionID
	}
	if nativeSessionID != "" {
		thread := request.currentProviderThread()
		thread.NativeThreadRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &nativeSessionID, Strength: orchestration.NativeRefStrong}
		thread.UpdatedAt = time.Now().UTC()
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated, ProviderThread: &thread}, raw); err != nil {
			return err
		}
	}
	if frame.Type == "message_update" && frame.AssistantMessageEvent.Type == "thinking_end" {
		nativeID := fmt.Sprintf("%s:reasoning:%d", request.Start.Attempt.ID, frame.AssistantMessageEvent.ContentIndex)
		item := orchestration.TurnItem{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
			ID:       orchestration.DeriveTurnItemFromProviderItemID(a.Driver(), nativeID),
			ThreadID: request.Start.Thread.ID, RunID: &request.Start.Run.ID, NodeID: request.Start.Run.RootNodeID,
			ProviderThreadID: currentProviderThreadID(request), ProviderTurnID: currentTurnID(request),
			Ordinal: frame.AssistantMessageEvent.ContentIndex + 1, Status: orchestration.TurnItemCompleted,
			Type: "reasoning", Text: frame.AssistantMessageEvent.Content, UpdatedAt: time.Now().UTC()}
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated, TurnItem: &item}, raw); err != nil {
			return err
		}
	}
	if frame.Type == "message_update" && frame.Usage.TotalTokens > 0 {
		if err := emitPiUsage(ctx, request, emit, raw, frame.Usage.TotalTokens, 0, frame.Usage.Input, frame.Usage.CacheRead, frame.Usage.Output, frame.Usage.Reasoning); err != nil {
			return err
		}
	}
	if frame.Type == "message_end" && frame.Message.Role == "assistant" {
		text := ""
		for _, content := range frame.Message.Content {
			if content.Type == "text" {
				text += content.Text
			}
		}
		nativeItemID := frame.Message.ResponseID
		if nativeItemID == "" {
			nativeItemID = fmt.Sprintf("%s:assistant", request.Start.Attempt.ID)
		}
		if text != "" {
			if err := emitAssistant(ctx, request, emit, nativeItemID, text, false, raw); err != nil {
				return err
			}
		}
		turn := request.currentTurn()
		turn.NativeTurnRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &nativeItemID, Strength: orchestration.NativeRefStrong}
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}, raw); err != nil {
			return err
		}
		if frame.Message.StopReason == "error" {
			return &TurnError{Class: "unknown", Message: "Pi assistant turn ended with an error."}
		}
	}
	if frame.Type == "response" && frame.Command == "get_entries" && frame.Success {
		for _, entry := range frame.Data.Entries {
			if entry.Type == "message" && entry.Message.Role == "user" {
				turn := request.currentTurn()
				turn.NativeTurnRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &entry.ID, Strength: orchestration.NativeRefStrong}
				if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}, raw); err != nil {
					return err
				}
				break
			}
		}
	}
	if frame.Type == "response" && frame.Command == "get_session_stats" && frame.Success {
		used := frame.Data.ContextUsage.Tokens
		maxTokens := frame.Data.ContextUsage.ContextWindow
		if err := emitPiUsage(ctx, request, emit, raw, used, maxTokens, frame.Data.Tokens.Input,
			frame.Data.Tokens.CacheRead, frame.Data.Tokens.Output, 0); err != nil {
			return err
		}
	}
	if frame.Type == "response" && !frame.Success && frame.Command == "prompt" {
		return &TurnError{Class: "unknown", Message: "Pi rejected the prompt.", Code: "prompt_rejected"}
	}
	return nil
}

func emitPiUsage(ctx context.Context, request RunRequest, emit EmitFunc, raw json.RawMessage, used, maxTokens, input, cached, output, reasoning int) error {
	turn := request.currentTurn()
	turn.TokenUsage = &orchestration.TokenUsage{UsedTokens: used, InputTokens: input, CachedInputTokens: cached,
		OutputTokens: output, ReasoningOutputTokens: reasoning, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if maxTokens > 0 {
		turn.TokenUsage.MaxTokens = &maxTokens
	}
	return emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}, raw)
}
