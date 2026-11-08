package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

type ClaudeAdapter struct {
	Binary string

	mu       sync.Mutex
	sessions map[orchestration.ProviderThreadID]*claudeSession
}

type claudeSession struct {
	client    *rpcClient
	nativeID  string
	mu        sync.Mutex
	currentMu sync.RWMutex
	current   *claudeInvocation
	model     string
}

type claudeInvocation struct {
	request    RunRequest
	emit       EmitFunc
	done       chan struct{}
	err        error
	finishOnce sync.Once
}

func (a *ClaudeAdapter) Driver() orchestration.ProviderDriverKind { return "claudeAgent" }
func (a *ClaudeAdapter) Capabilities() orchestration.ProviderCapabilities {
	return orchestration.ProviderCapabilities{
		Threads: orchestration.ThreadCapabilities{CanReadThreadSnapshot: true, ExposesNativeThreadID: true},
		Turns: orchestration.TurnCapabilities{EmitsTurnStarted: true, EmitsTurnCompleted: true,
			SupportsInterrupt: true, SupportsQueuedMessages: true, TerminalStatusQuality: "strong"},
		Streaming: orchestration.StreamingCapabilities{StreamsAssistantText: true, EmitsMessageCompleted: true},
		Identity: orchestration.IdentityCapabilities{NativeThreadIDs: orchestration.NativeRefStrong,
			NativeTurnIDs: orchestration.NativeRefNone, NativeItemIDs: orchestration.NativeRefStrong},
	}
}

func (a *ClaudeAdapter) Run(ctx context.Context, request RunRequest, emit EmitFunc) error {
	session, err := a.sessionFor(ctx, request)
	if err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	model := request.Start.Run.ModelSelection.Model
	if model != "" && model != "default" && model != session.model {
		if err := session.client.controlRequest(ctx, map[string]any{"subtype": "set_model", "model": model}); err != nil {
			return err
		}
		session.model = model
	}
	invocation := &claudeInvocation{request: request, emit: emit, done: make(chan struct{})}
	session.currentMu.Lock()
	session.current = invocation
	session.currentMu.Unlock()
	defer func() {
		session.currentMu.Lock()
		session.current = nil
		session.currentMu.Unlock()
	}()
	// This is the Claude Agent SDK streaming-query input shape, not --print text input.
	if err := session.client.sendRecord(map[string]any{"type": "user",
		"message":            map[string]any{"role": "user", "content": promptText(request.Start)},
		"parent_tool_use_id": nil, "session_id": session.nativeID}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-session.client.done:
		return fmt.Errorf("Claude query closed during turn: %w", err)
	case <-invocation.done:
		return invocation.err
	}
}

func (a *ClaudeAdapter) sessionFor(ctx context.Context, request RunRequest) (*claudeSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sessions == nil {
		a.sessions = make(map[orchestration.ProviderThreadID]*claudeSession)
	}
	if existing := a.sessions[request.ProviderThread.ID]; existing != nil {
		return existing, nil
	}
	binary := a.Binary
	if binary == "" {
		binary = "claude"
	}
	args := []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
		"--safe-mode", "--restricted", "--tools", "", "--permission-mode", "plan",
		"--permission-prompts", "none", "--strict-mcp-config"}
	nativeID := ""
	if request.ProviderThread.NativeThreadRef != nil && request.ProviderThread.NativeThreadRef.NativeID != nil {
		nativeID = *request.ProviderThread.NativeThreadRef.NativeID
		args = append(args, "--resume", nativeID)
	} else {
		nativeID = newUUID()
		args = append(args, "--session-id", nativeID)
	}
	session := &claudeSession{nativeID: nativeID, model: request.Start.Run.ModelSelection.Model}
	client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: request.Session.CWD,
		OnNotification: func(raw json.RawMessage) { a.handleNotification(session, raw) }})
	if err != nil {
		return nil, err
	}
	session.client = client
	a.sessions[request.ProviderThread.ID] = session
	return session, nil
}

func newUUID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func (a *ClaudeAdapter) handleNotification(session *claudeSession, raw json.RawMessage) {
	session.currentMu.RLock()
	invocation := session.current
	session.currentMu.RUnlock()
	if invocation == nil {
		return
	}
	var frame claudeFrame
	if json.Unmarshal(raw, &frame) != nil {
		return
	}
	if frame.Type == "control_request" {
		_ = session.client.sendRecord(map[string]any{"type": "control_response",
			"response": map[string]any{"request_id": frame.RequestID, "subtype": "error", "error": "control callback disabled by read-only g0code client"}})
		return
	}
	if frame.Type == "result" && (frame.StopReason == "interrupted" || frame.TerminalReason == "interrupted") {
		invocation.request.setTurnStatus(orchestration.ProviderTurnInterrupted)
	}
	if err := a.handleFrame(context.Background(), invocation.request, raw, invocation.emit); err != nil {
		invocation.finishOnce.Do(func() {
			invocation.err = err
			close(invocation.done)
		})
		return
	}
	if frame.Type == "result" {
		invocation.finishOnce.Do(func() { close(invocation.done) })
	}
}

func (a *ClaudeAdapter) Interrupt(ctx context.Context, effect orchestration.TurnInterruptEffect) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.currentMu.RLock()
		current := session.current
		session.currentMu.RUnlock()
		if current != nil && current.request.currentTurn().ID == effect.ProviderTurnID {
			return session.client.controlRequest(ctx, map[string]any{"subtype": "interrupt"})
		}
	}
	return fmt.Errorf("Claude turn %s is not active: %w", effect.ProviderTurnID, orchestration.ErrUnsupportedCapability)
}

func (a *ClaudeAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.client.close()
	}
	a.sessions = nil
	return nil
}

type claudeFrame struct {
	Type              string          `json:"type"`
	Subtype           string          `json:"subtype"`
	SessionID         string          `json:"session_id"`
	UUID              string          `json:"uuid"`
	RequestID         json.RawMessage `json:"request_id"`
	IsError           bool            `json:"is_error"`
	IsAPIErrorMessage bool            `json:"is_api_error_message"`
	Result            string          `json:"result"`
	StopReason        string          `json:"stop_reason"`
	TerminalReason    string          `json:"terminal_reason"`
	APIError          string          `json:"api_error"`
	APIErrorStatus    int             `json:"api_error_status"`
	Message           struct {
		ID      string `json:"id"`
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"message"`
	RateLimitInfo struct {
		ResetsAt int64 `json:"resetsAt"`
	} `json:"rate_limit_info"`
	APIErrorParams struct {
		RateLimitInfo struct {
			ResetsAt int64 `json:"resetsAt"`
		} `json:"rate_limit_info"`
	} `json:"api_error_params"`
	Usage struct {
		InputTokens       int `json:"input_tokens"`
		OutputTokens      int `json:"output_tokens"`
		CachedInputTokens int `json:"cache_read_input_tokens"`
	} `json:"usage"`
	ModelUsage map[string]struct {
		ContextWindow int `json:"contextWindow"`
	} `json:"modelUsage"`
}

func (a *ClaudeAdapter) handleFrame(ctx context.Context, request RunRequest, raw json.RawMessage, emit EmitFunc) error {
	var frame claudeFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return err
	}
	if frame.Type == "query.interrupt" {
		request.setTurnStatus(orchestration.ProviderTurnInterrupted)
	}
	if frame.SessionID != "" {
		thread := request.currentProviderThread()
		thread.NativeThreadRef = &orchestration.ProviderRef{Driver: a.Driver(), NativeID: &frame.SessionID, Strength: orchestration.NativeRefStrong}
		thread.UpdatedAt = time.Now().UTC()
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated, ProviderThread: &thread}, raw); err != nil {
			return err
		}
	}
	if frame.Type == "assistant" && frame.Message.ID != "" {
		text := ""
		for _, content := range frame.Message.Content {
			if content.Type == "text" {
				text += content.Text
			}
		}
		if text != "" {
			if err := emitAssistant(ctx, request, emit, frame.Message.ID, text, false, raw); err != nil {
				return err
			}
		}
	}
	if frame.Usage.OutputTokens != 0 || frame.Usage.InputTokens != 0 {
		turn := request.currentTurn()
		turn.TokenUsage = &orchestration.TokenUsage{UsedTokens: frame.Usage.InputTokens + frame.Usage.OutputTokens,
			InputTokens: frame.Usage.InputTokens, CachedInputTokens: frame.Usage.CachedInputTokens,
			OutputTokens: frame.Usage.OutputTokens, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		for _, usage := range frame.ModelUsage {
			if usage.ContextWindow > 0 {
				turn.TokenUsage.MaxTokens = ref(usage.ContextWindow)
				break
			}
		}
		if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}, raw); err != nil {
			return err
		}
	}
	if frame.Type == "assistant" && frame.IsAPIErrorMessage {
		class := "unknown"
		if frame.APIError == "usage_limit_reached" || frame.APIErrorStatus == 429 {
			class = "usage_limit"
		}
		resetAt := ""
		seconds := frame.APIErrorParams.RateLimitInfo.ResetsAt
		if seconds > 0 {
			resetAt = time.Unix(seconds, 0).UTC().Format(time.RFC3339)
		}
		message := frame.Result
		if message == "" {
			for _, content := range frame.Message.Content {
				message += content.Text
			}
		}
		return &TurnError{Class: class, Message: message, Code: frame.APIError, ResetAt: resetAt}
	}
	if frame.Type == "result" && frame.IsError {
		class := "unknown"
		if frame.APIError == "usage_limit_reached" || frame.APIErrorStatus == 429 {
			class = "usage_limit"
		}
		resetAt := ""
		if frame.RateLimitInfo.ResetsAt > 0 {
			resetAt = time.Unix(frame.RateLimitInfo.ResetsAt, 0).UTC().Format(time.RFC3339)
		}
		message := frame.Result
		if message == "" {
			message = fmt.Sprintf("Claude turn failed (%s).", frame.APIError)
		}
		return &TurnError{Class: class, Message: message, Code: frame.APIError, ResetAt: resetAt}
	}
	return nil
}
