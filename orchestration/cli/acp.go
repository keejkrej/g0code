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
	client     *rpcClient
	nativeID   string
	mu         sync.Mutex
	currentMu  sync.RWMutex
	current    *acpInvocation
	broker     *requestBroker
	configured map[string]any
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
	if err := a.applyACPOptions(ctx, session, request); err != nil {
		return err
	}
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
	session := &acpSession{broker: newRequestBroker(), configured: make(map[string]any)}
	client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: request.Session.CWD,
		OnNotification: func(raw json.RawMessage) {
			session.currentMu.RLock()
			current := session.current
			session.currentMu.RUnlock()
			if current != nil {
				_ = a.handleFrame(context.Background(), current.request, raw, current.emit)
			}
		}, OnRequest: func(_ context.Context, request rpcRequest) (any, *rpcError) {
			return a.handleNativeRequest(session, request)
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
	if effort := acpEffort(request.Start.Run.ModelSelection); effort != "" {
		if err := a.setConfig(ctx, session, "effort", effort); err != nil {
			return fail(err)
		}
	}
	if a.Mode != "" {
		if err := a.setConfig(ctx, session, "mode", a.Mode); err != nil {
			return fail(err)
		}
	}
	if model := request.Start.Run.ModelSelection.Model; model != "" && model != "default" {
		if err := a.setConfig(ctx, session, "model", model); err != nil {
			return fail(err)
		}
	}
	a.sessions[request.ProviderThread.ID] = session
	return session, nil
}

func acpEffort(selection orchestration.ModelSelection) string {
	if effort, _ := selection.Options["effort"].(string); effort != "" {
		return effort
	}
	effort, _ := selection.Options["reasoningEffort"].(string)
	return effort
}

func (a *ACPAdapter) setConfig(ctx context.Context, session *acpSession, id string, value any) error {
	if text, ok := value.(string); ok && text == "" {
		return nil
	}
	if current, ok := session.configured[id]; ok && current == value {
		return nil
	}
	if err := session.client.call(ctx, "session/set_config_option", map[string]any{"sessionId": session.nativeID,
		"configId": id, "value": value}, nil); err != nil {
		return err
	}
	session.configured[id] = value
	return nil
}

func (a *ACPAdapter) applyACPOptions(ctx context.Context, session *acpSession, request RunRequest) error {
	if err := a.setConfig(ctx, session, "effort", acpEffort(request.Start.Run.ModelSelection)); err != nil {
		return err
	}
	if model := request.Start.Run.ModelSelection.Model; model != "" && model != "default" {
		if err := a.setConfig(ctx, session, "model", model); err != nil {
			return err
		}
	}
	for id, value := range request.Start.Run.ModelSelection.Options {
		switch id {
		case "model", "effort", "reasoningEffort":
			continue
		default:
			if err := a.setConfig(ctx, session, id, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *ACPAdapter) handleNativeRequest(session *acpSession, request rpcRequest) (any, *rpcError) {
	if request.Method != "session/request_permission" {
		return nil, &rpcError{Code: -32601, Message: "unsupported ACP client callback: " + request.Method}
	}
	var payload struct {
		ToolCall struct {
			ToolCallID string `json:"toolCallId"`
			Name       string `json:"name"`
			Title      string `json:"title"`
			RawInput   any    `json:"rawInput"`
		} `json:"toolCall"`
		Options []struct {
			OptionID string `json:"optionId"`
			Kind     string `json:"kind"`
		} `json:"options"`
	}
	if err := json.Unmarshal(request.Params, &payload); err != nil {
		return nil, &rpcError{Code: -32602, Message: err.Error()}
	}
	session.currentMu.RLock()
	invocation := session.current
	session.currentMu.RUnlock()
	if invocation == nil {
		return nil, &rpcError{Code: -32000, Message: "ACP permission request has no active turn"}
	}
	kind := orchestration.RequestPermission
	if payload.ToolCall.Name == "bash" || payload.ToolCall.Name == "execute" {
		kind = orchestration.RequestCommand
	} else if payload.ToolCall.Name == "read_file" {
		kind = orchestration.RequestFileRead
	} else if payload.ToolCall.Name == "write_file" || payload.ToolCall.Name == "edit" {
		kind = orchestration.RequestFileChange
	}
	response, err := session.broker.ask(context.Background(), invocation.request, invocation.emit,
		payload.ToolCall.ToolCallID, kind, payload.ToolCall.Title, payload.ToolCall.RawInput)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if response.Decision != nil && *response.Decision == "cancel" {
		return map[string]any{"outcome": map[string]any{"outcome": "cancelled"}}, nil
	}
	wanted := "allow_once"
	if response.Decision != nil && (*response.Decision == "acceptForSession" || *response.Decision == "acceptAlways") {
		wanted = "allow_always"
	} else if response.Decision != nil && *response.Decision == "decline" {
		wanted = "reject_once"
	}
	optionID := ""
	for _, option := range payload.Options {
		if option.Kind == wanted || option.OptionID == wanted {
			optionID = option.OptionID
			break
		}
	}
	if optionID == "" && len(payload.Options) > 0 {
		optionID = payload.Options[0].OptionID
	}
	return map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": optionID}}, nil
}

func (a *ACPAdapter) RespondRuntimeRequest(_ context.Context, response orchestration.RuntimeRequestRespondEffect) error {
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
	return fmt.Errorf("ACP runtime request is not live: %w", orchestration.ErrRequestNotRespondable)
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
		session.broker.cancelAll()
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
				SessionUpdate string          `json:"sessionUpdate"`
				MessageID     string          `json:"messageId"`
				ToolCallID    string          `json:"toolCallId"`
				Name          string          `json:"name"`
				Title         string          `json:"title"`
				Kind          string          `json:"kind"`
				Status        string          `json:"status"`
				RawInput      json.RawMessage `json:"rawInput"`
				Content       json.RawMessage `json:"content"`
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
	if update.SessionUpdate == "tool_call" || update.SessionUpdate == "tool_call_update" {
		itemType := "dynamic_tool"
		if update.Kind == "edit" {
			itemType = "file_change"
		} else if update.Kind == "execute" {
			itemType = "command_execution"
		}
		status := orchestration.TurnItemStatus(update.Status)
		if status == "" {
			status = orchestration.TurnItemRunning
		}
		return emitToolCall(ctx, request, emit, ToolCall{ID: update.ToolCallID, Type: itemType,
			Title: update.Title, Status: status, Input: update.RawInput, Output: update.Content}, raw)
	}
	var content struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if len(update.Content) > 0 {
		if err := json.Unmarshal(update.Content, &content); err != nil {
			return err
		}
	}
	if content.Type != "text" {
		return nil
	}
	switch update.SessionUpdate {
	case "agent_message_chunk":
		nativeItemID := update.MessageID
		if nativeItemID == "" {
			nativeItemID = string(request.Start.Attempt.ID) + ":assistant"
		}
		state.assistantText[nativeItemID] += content.Text
		return emitAssistant(ctx, request, emit, nativeItemID, state.assistantText[nativeItemID], true, raw)
	case "agent_thought_chunk":
		nativeItemID := update.MessageID
		if nativeItemID == "" {
			nativeItemID = string(request.Start.Attempt.ID) + ":reasoning"
		}
		state.reasoningText[nativeItemID] += content.Text
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
