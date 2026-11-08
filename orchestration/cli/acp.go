package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

type ACPAdapter struct {
	Binary     string
	Args       []string
	DriverKind orchestration.ProviderDriverKind
	LoadMethod string
	Mode       string

	stateMu  sync.Mutex
	states   map[orchestration.ProviderTurnID]*acpTurnState
	mu       sync.Mutex
	sessions map[orchestration.ProviderThreadID]*acpSession
}

type acpTurnState struct {
	mu            sync.Mutex
	assistantText map[string]string
	reasoningText map[string]string
	prompting     bool
}

type acpSession struct {
	client    *rpcClient
	nativeID  string
	mu        sync.Mutex
	currentMu sync.RWMutex
	current   *acpInvocation
}

type acpInvocation struct {
	request RunRequest
	emit    EmitFunc
}

func (a *ACPAdapter) Driver() orchestration.ProviderDriverKind {
	if a.DriverKind == "" {
		return "acp"
	}
	return a.DriverKind
}
func turnID(request RunRequest) orchestration.ProviderTurnID { return request.currentTurn().ID }

func (a *ACPAdapter) Capabilities() orchestration.ProviderCapabilities {
	return orchestration.ProviderCapabilities{
		Sessions: orchestration.SessionCapabilities{SupportsProviderSwitchingViaHandoff: true},
		Threads:  orchestration.ThreadCapabilities{CanReadThreadSnapshot: true, ExposesNativeThreadID: true},
		Turns: orchestration.TurnCapabilities{EmitsTurnStarted: true, EmitsTurnCompleted: true,
			SupportsInterrupt: true, SupportsQueuedMessages: true, TerminalStatusQuality: "strong"},
		Streaming: orchestration.StreamingCapabilities{StreamsAssistantText: true, StreamsReasoning: true, EmitsMessageCompleted: true},
		Identity: orchestration.IdentityCapabilities{NativeThreadIDs: orchestration.NativeRefStrong,
			NativeTurnIDs: orchestration.NativeRefNone, NativeItemIDs: orchestration.NativeRefStrong},
	}
}

func (a *ACPAdapter) Run(ctx context.Context, request RunRequest, emit EmitFunc) error {
	session, err := a.sessionFor(ctx, request, emit)
	if err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	invocation := &acpInvocation{request: request, emit: emit}
	session.currentMu.Lock()
	session.current = invocation
	session.currentMu.Unlock()
	defer func() {
		session.currentMu.Lock()
		session.current = nil
		session.currentMu.Unlock()
	}()
	state := a.state(turnID(request))
	state.mu.Lock()
	state.prompting = true
	state.mu.Unlock()
	var promptResult struct {
		StopReason string `json:"stopReason"`
		Usage      struct {
			InputTokens         int `json:"inputTokens"`
			OutputTokens        int `json:"outputTokens"`
			CachedReadTokens    int `json:"cacheReadTokens"`
			CachedReadTokensAlt int `json:"cachedReadTokens"`
			ReasoningTokens     int `json:"reasoningTokens"`
			ThoughtTokens       int `json:"thoughtTokens"`
		} `json:"usage"`
	}
	err = session.client.call(ctx, "session/prompt", map[string]any{"sessionId": session.nativeID,
		"prompt": []any{map[string]any{"type": "text", "text": promptText(request.Start)}}}, &promptResult)
	if err != nil {
		return err
	}
	if promptResult.StopReason == "cancelled" {
		request.setTurnStatus(orchestration.ProviderTurnInterrupted)
	}
	if promptResult.Usage.InputTokens != 0 || promptResult.Usage.OutputTokens != 0 {
		cached := promptResult.Usage.CachedReadTokens + promptResult.Usage.CachedReadTokensAlt
		reasoning := promptResult.Usage.ReasoningTokens + promptResult.Usage.ThoughtTokens
		turn := request.currentTurn()
		turn.TokenUsage = &orchestration.TokenUsage{UsedTokens: promptResult.Usage.InputTokens + promptResult.Usage.OutputTokens,
			InputTokens: promptResult.Usage.InputTokens, CachedInputTokens: cached, OutputTokens: promptResult.Usage.OutputTokens,
			ReasoningOutputTokens: reasoning, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn},
			json.RawMessage(`{"method":"session/prompt","kind":"response"}`)); err != nil {
			return err
		}
	}
	state = a.state(turnID(request))
	state.mu.Lock()
	defer state.mu.Unlock()
	nativeItemIDs := make([]string, 0, len(state.assistantText))
	for nativeItemID := range state.assistantText {
		nativeItemIDs = append(nativeItemIDs, nativeItemID)
	}
	sort.Strings(nativeItemIDs)
	for _, nativeItemID := range nativeItemIDs {
		text := state.assistantText[nativeItemID]
		if err := emitAssistant(ctx, request, emit, nativeItemID, text, false, json.RawMessage(`{"synthetic":"acp.message_end"}`)); err != nil {
			return err
		}
	}
	return nil
}

func (a *ACPAdapter) sessionFor(ctx context.Context, request RunRequest, emit EmitFunc) (*acpSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sessions == nil {
		a.sessions = make(map[orchestration.ProviderThreadID]*acpSession)
	}
	if existing := a.sessions[request.ProviderThread.ID]; existing != nil {
		return existing, nil
	}
	binary := a.Binary
	if binary == "" {
		return nil, fmt.Errorf("ACP binary is required")
	}
	args := append([]string(nil), a.Args...)
	if len(args) == 0 {
		args = []string{"acp"}
	}
	session := &acpSession{}
	client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: request.Session.CWD,
		OnNotification: func(raw json.RawMessage) {
			session.currentMu.RLock()
			current := session.current
			session.currentMu.RUnlock()
			if current != nil {
				_ = a.handleFrame(context.Background(), current.request, raw, current.emit)
			}
		}, OnRequest: func(_ context.Context, request rpcRequest) (any, *rpcError) {
			if request.Method == "session/request_permission" {
				return map[string]any{"outcome": map[string]any{"outcome": "cancelled"}}, nil
			}
			return nil, &rpcError{Code: -32601, Message: "callback disabled by read-only g0code client"}
		}})
	if err != nil {
		return nil, err
	}
	session.client = client
	fail := func(err error) (*acpSession, error) {
		client.close()
		return nil, err
	}
	var initializeResult struct {
		ProtocolVersion int `json:"protocolVersion"`
	}
	if err := client.call(ctx, "initialize", map[string]any{
		"protocolVersion": 2,
		"clientCapabilities": map[string]any{
			"fs": map[string]any{"readTextFile": false, "writeTextFile": false}, "terminal": false,
			"session": map[string]any{"notices": map[string]any{}}, "elicitation": map[string]any{"form": map[string]any{}},
		},
		"clientInfo": map[string]any{"name": "g0code", "title": "g0code", "version": "0.0.0"},
	}, &initializeResult); err != nil {
		return fail(err)
	}
	var sessionResult struct {
		SessionID string `json:"sessionId"`
	}
	method := "session/new"
	params := map[string]any{"cwd": request.Session.CWD, "mcpServers": []any{}}
	if request.ProviderThread.NativeThreadRef != nil && request.ProviderThread.NativeThreadRef.NativeID != nil {
		method = a.LoadMethod
		if method == "" {
			method = "session/load"
		}
		params["sessionId"] = *request.ProviderThread.NativeThreadRef.NativeID
	}
	if err := client.call(ctx, method, params, &sessionResult); err != nil && method != "session/new" {
		fallback := "session/load"
		if method == fallback {
			fallback = "session/resume"
		}
		err = client.call(ctx, fallback, params, &sessionResult)
	}
	if err != nil {
		return fail(err)
	}
	session.nativeID = sessionResult.SessionID
	if session.nativeID == "" && request.ProviderThread.NativeThreadRef != nil && request.ProviderThread.NativeThreadRef.NativeID != nil {
		session.nativeID = *request.ProviderThread.NativeThreadRef.NativeID
	}
	if session.nativeID == "" {
		return fail(fmt.Errorf("ACP %s returned no sessionId", method))
	}
	thread := *request.ProviderThread
	thread.NativeThreadRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &session.nativeID, Strength: orchestration.NativeRefStrong}
	thread.UpdatedAt = time.Now().UTC()
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated, ProviderThread: &thread},
		json.RawMessage(`{"method":"`+method+`"}`)); err != nil {
		return fail(err)
	}
	if effort, _ := request.Start.Run.ModelSelection.Options["reasoningEffort"].(string); effort != "" {
		if err := client.call(ctx, "session/set_config_option", map[string]any{"sessionId": session.nativeID,
			"configId": "effort", "value": effort}, nil); err != nil {
			return fail(err)
		}
	}
	if a.Mode != "" {
		if err := client.call(ctx, "session/set_config_option", map[string]any{"sessionId": session.nativeID,
			"configId": "mode", "value": a.Mode}, nil); err != nil {
			return fail(err)
		}
	}
	if model := request.Start.Run.ModelSelection.Model; model != "" && model != "default" {
		if err := client.call(ctx, "session/set_config_option", map[string]any{"sessionId": session.nativeID,
			"configId": "model", "value": model}, nil); err != nil {
			return fail(err)
		}
	}
	a.sessions[request.ProviderThread.ID] = session
	return session, nil
}

// Interrupt matches ACP's native session/cancel notification.
func (a *ACPAdapter) Interrupt(ctx context.Context, effect orchestration.TurnInterruptEffect) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.currentMu.RLock()
		current := session.current
		turnID := orchestration.ProviderTurnID("")
		if current != nil {
			turnID = current.request.currentTurn().ID
		}
		session.currentMu.RUnlock()
		if turnID == effect.ProviderTurnID {
			return session.client.notify("session/cancel", map[string]any{"sessionId": session.nativeID})
		}
	}
	return fmt.Errorf("ACP turn %s is not active: %w", effect.ProviderTurnID, orchestration.ErrUnsupportedCapability)
}

func (a *ACPAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		_ = session.client.call(context.Background(), "session/close", map[string]any{"sessionId": session.nativeID}, nil)
		session.client.close()
	}
	a.sessions = nil
	return nil
}

func (a *ACPAdapter) state(id orchestration.ProviderTurnID) *acpTurnState {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	if a.states == nil {
		a.states = make(map[orchestration.ProviderTurnID]*acpTurnState)
	}
	state := a.states[id]
	if state == nil {
		state = &acpTurnState{assistantText: make(map[string]string), reasoningText: make(map[string]string)}
		a.states[id] = state
	}
	return state
}

func (a *ACPAdapter) handleFrame(ctx context.Context, request RunRequest, raw json.RawMessage, emit EmitFunc) error {
	var frame struct {
		Kind   string `json:"kind"`
		Method string `json:"method"`
		Params struct {
			SessionID string `json:"sessionId"`
			Update    struct {
				SessionUpdate string `json:"sessionUpdate"`
				MessageID     string `json:"messageId"`
				Content       struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"update"`
		} `json:"params"`
		Result struct {
			SessionID  string `json:"sessionId"`
			StopReason string `json:"stopReason"`
			Usage      struct {
				InputTokens         int `json:"inputTokens"`
				OutputTokens        int `json:"outputTokens"`
				CachedReadTokens    int `json:"cacheReadTokens"`
				CachedReadTokensAlt int `json:"cachedReadTokens"`
				ReasoningTokens     int `json:"reasoningTokens"`
				ThoughtTokens       int `json:"thoughtTokens"`
			} `json:"usage"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return err
	}
	if frame.Method == "session/prompt" && frame.Kind != "response" {
		a.state(turnID(request)).prompting = true
	}
	if frame.Method == "session/prompt" && frame.Kind == "response" && frame.Result.StopReason == "cancelled" {
		request.setTurnStatus(orchestration.ProviderTurnInterrupted)
	}
	if frame.Result.SessionID != "" {
		thread := request.currentProviderThread()
		thread.NativeThreadRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &frame.Result.SessionID, Strength: orchestration.NativeRefStrong}
		thread.UpdatedAt = time.Now().UTC()
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated, ProviderThread: &thread}, raw); err != nil {
			return err
		}
	}
	state := a.state(turnID(request))
	state.mu.Lock()
	defer state.mu.Unlock()
	if frame.Method != "session/update" || !state.prompting {
		return nil
	}
	update := frame.Params.Update
	if update.Content.Type != "text" {
		return nil
	}
	switch update.SessionUpdate {
	case "agent_message_chunk":
		nativeItemID := update.MessageID
		if nativeItemID == "" {
			nativeItemID = string(request.Start.Attempt.ID) + ":assistant"
		}
		state.assistantText[nativeItemID] += update.Content.Text
		return emitAssistant(ctx, request, emit, nativeItemID, state.assistantText[nativeItemID], true, raw)
	case "agent_thought_chunk":
		nativeItemID := update.MessageID
		if nativeItemID == "" {
			nativeItemID = string(request.Start.Attempt.ID) + ":reasoning"
		}
		state.reasoningText[nativeItemID] += update.Content.Text
		item := orchestration.TurnItem{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
			ID:       orchestration.DeriveTurnItemFromProviderItemID(a.Driver(), nativeItemID),
			ThreadID: request.Start.Thread.ID, RunID: &request.Start.Run.ID, NodeID: request.Start.Run.RootNodeID,
			ProviderThreadID: currentProviderThreadID(request), ProviderTurnID: currentTurnID(request),
			Ordinal: 1, Status: orchestration.TurnItemRunning, Type: "reasoning", Text: state.reasoningText[nativeItemID],
			UpdatedAt: time.Now().UTC()}
		return emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated, TurnItem: &item}, raw)
	}
	return nil
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("JSON-RPC error %d: %s", e.Code, e.Message) }
