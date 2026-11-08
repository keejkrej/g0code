// Package cli connects local coding-agent CLIs to the orchestration package.
// The initial adapters intentionally implement a read-only, text-only turn path.
package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

type EmitFunc func(context.Context, orchestration.ProviderEvent, json.RawMessage) error

type RunRequest struct {
	Start          orchestration.TurnStartEffect
	Session        *orchestration.ProviderSession
	ProviderThread *orchestration.ProviderThread
	ProviderTurn   *orchestration.ProviderTurn
	StateMu        *sync.Mutex
}

func (r RunRequest) currentTurn() orchestration.ProviderTurn {
	if r.StateMu != nil {
		r.StateMu.Lock()
		defer r.StateMu.Unlock()
	}
	return *r.ProviderTurn
}

func (r RunRequest) currentProviderThread() orchestration.ProviderThread {
	if r.StateMu != nil {
		r.StateMu.Lock()
		defer r.StateMu.Unlock()
	}
	return *r.ProviderThread
}

func (r RunRequest) setTurnStatus(status orchestration.ProviderTurnStatus) {
	if r.StateMu != nil {
		r.StateMu.Lock()
		defer r.StateMu.Unlock()
	}
	r.ProviderTurn.Status = status
}

func currentTurnID(request RunRequest) *orchestration.ProviderTurnID {
	turn := request.currentTurn()
	return &turn.ID
}

func currentProviderThreadID(request RunRequest) *orchestration.ProviderThreadID {
	thread := request.currentProviderThread()
	return &thread.ID
}

type Adapter interface {
	Driver() orchestration.ProviderDriverKind
	Capabilities() orchestration.ProviderCapabilities
	Run(context.Context, RunRequest, EmitFunc) error
}

type interruptAdapter interface {
	Interrupt(context.Context, orchestration.TurnInterruptEffect) error
}

type closeAdapter interface {
	Close() error
}

type activeTurn struct {
	cancel  context.CancelFunc
	adapter Adapter
}

type TurnError struct {
	Class   string
	Message string
	Code    string
	ResetAt string
}

func (e *TurnError) Error() string { return e.Message }

type Executor struct {
	orchestrator *orchestration.Orchestrator
	adapters     map[orchestration.ProviderInstanceID]Adapter

	mu     sync.Mutex
	active map[orchestration.ProviderTurnID]activeTurn
}

func NewExecutor(o *orchestration.Orchestrator) *Executor {
	return &Executor{orchestrator: o, adapters: make(map[orchestration.ProviderInstanceID]Adapter),
		active: make(map[orchestration.ProviderTurnID]activeTurn)}
}

func (e *Executor) Register(instanceID orchestration.ProviderInstanceID, adapter Adapter) {
	e.adapters[instanceID] = adapter
}

func (e *Executor) Close() error {
	var errs []error
	for _, adapter := range e.adapters {
		if closer, ok := adapter.(closeAdapter); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (e *Executor) RegisterDefaults() {
	e.Register("codex", &CodexAdapter{})
	e.Register("claude", &ClaudeAdapter{})
	e.Register("claudeAgent", &ClaudeAdapter{})
	e.Register("pi", &PiAdapter{})
	e.Register("agy", &ACPAdapter{Binary: "agy", Args: []string{"acp", "--sandbox", "--disable-telemetry"}, DriverKind: "agy", LoadMethod: "session/load", Mode: "plan"})
	e.Register("fx", &ACPAdapter{Binary: "fx", Args: []string{"acp"}, DriverKind: "fx", LoadMethod: "session/resume", Mode: "ask"})
}

// Execute runs one post-commit effect. Start effects block until the CLI turn
// reaches a terminal state. Interrupt effects cancel an active local process.
func (e *Executor) Execute(ctx context.Context, effect orchestration.Effect) error {
	switch effect.Kind {
	case orchestration.EffectProviderTurnStart:
		start, ok := effectPayload[orchestration.TurnStartEffect](effect.Payload)
		if !ok {
			return fmt.Errorf("provider.turn.start payload is %T", effect.Payload)
		}
		return e.startTurn(ctx, effect, start)
	case orchestration.EffectProviderTurnInterrupt:
		interrupt, ok := effectPayload[orchestration.TurnInterruptEffect](effect.Payload)
		if !ok {
			return fmt.Errorf("provider.turn.interrupt payload is %T", effect.Payload)
		}
		e.mu.Lock()
		active := e.active[interrupt.ProviderTurnID]
		e.mu.Unlock()
		if active.adapter == nil {
			return fmt.Errorf("provider turn %s is not active: %w", interrupt.ProviderTurnID, orchestration.ErrUnsupportedCapability)
		}
		if adapter, ok := active.adapter.(interruptAdapter); ok {
			return adapter.Interrupt(ctx, interrupt)
		}
		active.cancel()
		return nil
	default:
		return fmt.Errorf("effect %s is not supported by the CLI executor: %w", effect.Kind, orchestration.ErrUnsupportedCapability)
	}
}

func effectPayload[T any](payload any) (T, bool) {
	switch value := payload.(type) {
	case T:
		return value, true
	case *T:
		if value != nil {
			return *value, true
		}
	}
	var zero T
	return zero, false
}

func (e *Executor) startTurn(ctx context.Context, effect orchestration.Effect, start orchestration.TurnStartEffect) error {
	adapter := e.adapters[start.Run.ProviderInstanceID]
	if adapter == nil {
		return fmt.Errorf("no CLI adapter registered for %s", start.Run.ProviderInstanceID)
	}
	cwd := ""
	if start.Thread.WorktreePath != nil {
		cwd = *start.Thread.WorktreePath
	} else {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	sessionID := orchestration.ProviderSessionID(stableID("provider-session:cli:", string(start.ProviderThread.ID)))
	turnID := orchestration.ProviderTurnID(stableID("provider-turn:cli:", string(start.Attempt.ID)))
	model := start.Run.ModelSelection.Model
	session := orchestration.ProviderSession{ID: sessionID, Driver: adapter.Driver(),
		ProviderInstanceID: start.Run.ProviderInstanceID, Status: orchestration.SessionRunning,
		CWD: cwd, Model: &model, Capabilities: adapter.Capabilities(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	providerThread := start.ProviderThread
	providerThread.Driver = adapter.Driver()
	providerThread.ProviderSessionID = &sessionID
	providerThread.Status = orchestration.ProviderThreadActive
	providerThread.UpdatedAt = time.Now().UTC()
	turn := orchestration.ProviderTurn{ID: turnID, ProviderThreadID: providerThread.ID,
		NodeID: *start.Run.RootNodeID, RunAttemptID: &start.Attempt.ID, Ordinal: 1, Status: orchestration.ProviderTurnRunning}
	request := RunRequest{Start: start, Session: &session, ProviderThread: &providerThread, ProviderTurn: &turn,
		StateMu: &sync.Mutex{}}

	runCtx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	e.active[turnID] = activeTurn{cancel: cancel, adapter: adapter}
	e.mu.Unlock()
	defer func() {
		cancel()
		e.mu.Lock()
		delete(e.active, turnID)
		e.mu.Unlock()
	}()

	sequence := 0
	var pending []orchestration.Effect
	assistantMessages := make(map[orchestration.MessageID]bool)
	itemOrdinals := make(map[orchestration.TurnItemID]int)
	nextItemOrdinal := 1
	if snapshot, err := e.orchestrator.Snapshot(ctx, start.Thread.ID); err == nil {
		for _, item := range snapshot.Projection.TurnItems {
			itemOrdinals[item.ID] = item.Ordinal
			if item.Ordinal >= nextItemOrdinal {
				nextItemOrdinal = item.Ordinal + 1
			}
		}
	}
	emit := func(ctx context.Context, event orchestration.ProviderEvent, raw json.RawMessage) error {
		request.StateMu.Lock()
		defer request.StateMu.Unlock()
		sequence++
		sequenceAt := sequence
		if event.Driver == "" {
			event.Driver = adapter.Driver()
		}
		if event.ProviderTurn != nil {
			turn = *event.ProviderTurn
			request.ProviderTurn = &turn
		}
		if event.ProviderThread != nil {
			providerThread = *event.ProviderThread
			request.ProviderThread = &providerThread
		}
		if event.Message != nil && event.Message.Role == orchestration.MessageAssistant {
			assistantMessages[event.Message.ID] = event.Message.Streaming
		}
		if event.TurnItem != nil {
			item := *event.TurnItem
			ordinal, ok := itemOrdinals[item.ID]
			if !ok {
				ordinal = nextItemOrdinal
				nextItemOrdinal++
				itemOrdinals[item.ID] = ordinal
			}
			item.Ordinal = ordinal
			event.TurnItem = &item
		}
		hash := sha256.Sum256(append([]byte(fmt.Sprintf("%s:%d:", effect.ID, sequenceAt)), raw...))
		rawID := orchestration.RawEventID("raw-event:cli:" + hex.EncodeToString(hash[:]))
		result, err := e.orchestrator.Ingest(ctx, orchestration.IngestInput{
			ProviderSessionID: sessionID, ProviderInstanceID: start.Run.ProviderInstanceID,
			CommandID: &effect.CommandID, ThreadID: start.Thread.ID, RunID: &start.Run.ID,
			NodeID: start.Run.RootNodeID, RawEventID: &rawID, Event: event,
			Guard: &orchestration.RunGuard{RunID: start.Run.ID, ActiveAttemptID: start.Attempt.ID},
		})
		if err == nil {
			pending = append(pending, result.Effects...)
		}
		return err
	}

	if err := emit(runCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventSessionUpdated,
		ProviderSession: &session}, json.RawMessage(`{"synthetic":"session.running"}`)); err != nil {
		return err
	}
	if err := emit(runCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated,
		ProviderThread: &providerThread}, json.RawMessage(`{"synthetic":"provider-thread.active"}`)); err != nil {
		return err
	}
	if err := emit(runCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated,
		ProviderTurn: &turn}, json.RawMessage(`{"synthetic":"provider-turn.running"}`)); err != nil {
		return err
	}

	runErr := adapter.Run(runCtx, request, emit)
	finalCtx := context.WithoutCancel(runCtx)
	messageIDs := make([]orchestration.MessageID, 0, len(assistantMessages))
	for messageID := range assistantMessages {
		messageIDs = append(messageIDs, messageID)
	}
	sort.Slice(messageIDs, func(i, j int) bool { return messageIDs[i] < messageIDs[j] })
	for _, messageID := range messageIDs {
		streaming := assistantMessages[messageID]
		if !streaming {
			continue
		}
		snapshot, err := e.orchestrator.Snapshot(finalCtx, start.Thread.ID)
		if err != nil {
			return err
		}
		for _, message := range snapshot.Projection.Messages {
			if message.ID != messageID {
				continue
			}
			message.Streaming = false
			message.UpdatedAt = time.Now().UTC()
			if err := emit(finalCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventMessageUpdated,
				Message: &message}, json.RawMessage(`{"synthetic":"message.completed"}`)); err != nil {
				return err
			}
			if message.NodeID != nil {
				if node, ok := snapshot.Projection.Node(*message.NodeID); ok {
					node.Status = orchestration.NodeCompleted
					node.CompletedAt = ref(time.Now().UTC())
					if err := emit(finalCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventNodeUpdated,
						Node: &node}, json.RawMessage(`{"synthetic":"node.completed"}`)); err != nil {
						return err
					}
				}
			}
			for _, item := range snapshot.Projection.TurnItems {
				if item.MessageID != messageID {
					continue
				}
				item.Status = orchestration.TurnItemCompleted
				item.CompletedAt = ref(time.Now().UTC())
				item.UpdatedAt = time.Now().UTC()
				if err := emit(finalCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated,
					TurnItem: &item}, json.RawMessage(`{"synthetic":"turn-item.completed"}`)); err != nil {
					return err
				}
			}
		}
	}

	status := orchestration.ProviderTurnCompleted
	nativeStatus := request.currentTurn().Status
	if nativeStatus == orchestration.ProviderTurnInterrupted || nativeStatus == orchestration.ProviderTurnCancelled {
		status = nativeStatus
	}
	if runErr != nil {
		status = orchestration.ProviderTurnFailed
		if errors.Is(runCtx.Err(), context.Canceled) {
			status = orchestration.ProviderTurnInterrupted
		}
		failure := failureFromError(runErr)
		item := orchestration.TurnItem{ID: orchestration.TurnItemID("turn-item:cli-failure:" + string(turnID)),
			ThreadID: start.Thread.ID, RunID: &start.Run.ID, NodeID: start.Run.RootNodeID,
			ProviderThreadID: &providerThread.ID, ProviderTurnID: &turnID, Ordinal: 1,
			Status: orchestration.TurnItemFailed, Title: failure.Message, Type: "provider_failure",
			Data: map[string]any{"failure": failure}, UpdatedAt: time.Now().UTC()}
		if err := emit(finalCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated,
			TurnItem: &item}, json.RawMessage(`{"synthetic":"provider-turn.failure"}`)); err != nil {
			return err
		}
	}
	session = *request.Session
	session.Status = orchestration.SessionStopped
	session.UpdatedAt = time.Now().UTC()
	if err := emit(finalCtx, orchestration.ProviderEvent{Type: orchestration.ProviderEventSessionUpdated,
		ProviderSession: &session}, json.RawMessage(`{"synthetic":"session.stopped"}`)); err != nil {
		return err
	}
	turn = request.currentTurn()
	turn.Status = status
	turn.CompletedAt = ref(time.Now().UTC())
	if err := emit(context.WithoutCancel(runCtx), orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated,
		ProviderTurn: &turn}, json.RawMessage(`{"synthetic":"provider-turn.terminal"}`)); err != nil {
		return err
	}
	for _, next := range pending {
		if err := e.Execute(context.WithoutCancel(runCtx), next); err != nil {
			return err
		}
	}
	return runErr
}

func failureFromError(err error) orchestration.ProviderFailure {
	var turnErr *TurnError
	if errors.As(err, &turnErr) {
		return orchestration.ProviderFailure{Class: turnErr.Class, Message: turnErr.Message, Code: turnErr.Code, ResetAt: turnErr.ResetAt}
	}
	return orchestration.ProviderFailure{Class: "unknown", Message: err.Error()}
}

func stableID(prefix, scope string) string {
	hash := sha256.Sum256([]byte(scope))
	return prefix + hex.EncodeToString(hash[:16])
}

func ref[T any](value T) *T { return &value }

func emitAssistant(ctx context.Context, request RunRequest, emit EmitFunc, nativeItemID, text string, streaming bool, raw json.RawMessage) error {
	now := time.Now().UTC()
	providerThread := request.currentProviderThread()
	providerTurn := request.currentTurn()
	driver := providerThread.Driver
	nodeID := orchestration.DeriveNodeFromProviderItemID(driver, nativeItemID)
	messageID := orchestration.DeriveMessageFromProviderItemID(driver, nativeItemID)
	nativeRef := &orchestration.ProviderRef{Driver: driver, NativeID: &nativeItemID, Strength: orchestration.NativeRefStrong}
	nodeStatus := orchestration.NodeRunning
	if !streaming {
		nodeStatus = orchestration.NodeCompleted
	}
	node := orchestration.ExecutionNode{ID: nodeID, ThreadID: request.Start.Thread.ID, RunID: &request.Start.Run.ID,
		ParentNodeID: request.Start.Run.RootNodeID, RootNodeID: *request.Start.Run.RootNodeID,
		Kind: orchestration.NodeAssistantMessage, Status: nodeStatus, CountsForRun: false,
		ProviderThreadID: &providerThread.ID, ProviderTurnID: &providerTurn.ID,
		NativeItemRef: nativeRef, StartedAt: &now}
	if !streaming {
		node.CompletedAt = &now
	}
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventNodeUpdated, Node: &node}, raw); err != nil {
		return err
	}
	message := orchestration.ConversationMessage{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
		ID: messageID, ThreadID: request.Start.Thread.ID, RunID: &request.Start.Run.ID, NodeID: &nodeID,
		Role: orchestration.MessageAssistant, Text: text, Attachments: []orchestration.Attachment{}, Streaming: streaming,
		CreatedAt: now, UpdatedAt: now}
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventMessageUpdated,
		Message: &message}, raw); err != nil {
		return err
	}
	status := orchestration.TurnItemRunning
	if !streaming {
		status = orchestration.TurnItemCompleted
	}
	item := orchestration.TurnItem{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
		ID:       orchestration.DeriveTurnItemFromProviderItemID(driver, nativeItemID),
		ThreadID: message.ThreadID, RunID: message.RunID, NodeID: &nodeID, ProviderThreadID: &providerThread.ID,
		ProviderTurnID: &providerTurn.ID, NativeItemRef: nativeRef, Ordinal: 1, Status: status,
		Type: "assistant_message", MessageID: messageID, Text: text, UpdatedAt: now}
	if !streaming {
		item.CompletedAt = &now
	}
	return emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated, TurnItem: &item}, raw)
}

func promptText(start orchestration.TurnStartEffect) string {
	if start.ContextHandoff == nil {
		return start.Message.Text
	}
	return "<context_handoff id=\"" + string(start.ContextHandoff.ID) + "\" strategy=\"" + start.ContextHandoff.Strategy + "\">\n" +
		start.ContextHandoff.SummaryText + "\n</context_handoff>\n\n" + start.Message.Text
}
