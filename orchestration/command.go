package orchestration

import (
	"encoding/json"
	"fmt"
	"strings"
)

type CommandType string

const (
	CommandThreadCreate          CommandType = "thread.create"
	CommandThreadArchive         CommandType = "thread.archive"
	CommandThreadUnarchive       CommandType = "thread.unarchive"
	CommandThreadDelete          CommandType = "thread.delete"
	CommandThreadRuntimeModeSet  CommandType = "thread.runtime-mode.set"
	CommandMessageDispatch       CommandType = "message.dispatch"
	CommandRunInterrupt          CommandType = "run.interrupt"
	CommandRuntimeRequestRespond CommandType = "runtime-request.respond"
	CommandThreadFork            CommandType = "thread.fork"
	CommandDelegatedTaskRequest  CommandType = "delegated_task.request"
	CommandQueuedRunCancel       CommandType = "queued-run.cancel"
	CommandQueueResume           CommandType = "queue.resume"
	CommandProviderSwitch        CommandType = "provider.switch"
)

type Command struct {
	Type      CommandType `json:"type"`
	CommandID CommandID   `json:"commandId"`
	ThreadID  ThreadID    `json:"threadId"`
	Payload   any         `json:"payload"`
}

type CreateThread struct {
	ProjectID       ProjectID       `json:"projectId"`
	Title           string          `json:"title"`
	ModelSelection  ModelSelection  `json:"modelSelection"`
	RuntimeMode     RuntimeMode     `json:"runtimeMode"`
	InteractionMode InteractionMode `json:"interactionMode"`
	Branch          *string         `json:"branch"`
	WorktreePath    *string         `json:"worktreePath"`
	CreatedBy       Actor           `json:"createdBy"`
	CreationSource  CreationSource  `json:"creationSource"`
}

type DispatchModeType string

const (
	DispatchDeferStart       DispatchModeType = "defer_start"
	DispatchSteerActive      DispatchModeType = "steer_active"
	DispatchRestartActive    DispatchModeType = "restart_active"
	DispatchQueueAfterActive DispatchModeType = "queue_after_active"
	DispatchStartImmediately DispatchModeType = "start_immediately"
)

type DispatchMode struct {
	Type        DispatchModeType `json:"type"`
	TargetRunID RunID            `json:"targetRunId,omitempty"`
}

type DispatchMessage struct {
	MessageID      MessageID       `json:"messageId"`
	Text           string          `json:"text"`
	Attachments    []Attachment    `json:"attachments"`
	TitleSeed      string          `json:"titleSeed,omitempty"`
	ModelSelection *ModelSelection `json:"modelSelection,omitempty"`
	DispatchMode   DispatchMode    `json:"dispatchMode"`
	CreatedBy      Actor           `json:"createdBy"`
	CreationSource CreationSource  `json:"creationSource"`
}

type InterruptRun struct {
	RunID     RunID  `json:"runId"`
	Reason    string `json:"reason,omitempty"`
	HoldQueue bool   `json:"holdQueue,omitempty"`
}

type RuntimeRequestResponse struct {
	RequestID RuntimeRequestID `json:"requestId"`
	Decision  *string          `json:"decision,omitempty"`
	Feedback  *string          `json:"feedback,omitempty"`
	Answers   map[string]any   `json:"answers,omitempty"`
}

type ForkSourcePoint struct {
	Type         string       `json:"type"`
	RunID        RunID        `json:"runId,omitempty"`
	CheckpointID CheckpointID `json:"checkpointId,omitempty"`
}

type DelegateTask struct {
	ParentThreadID  ThreadID        `json:"parentThreadId"`
	ParentRunID     RunID           `json:"parentRunId"`
	ParentNodeID    NodeID          `json:"parentNodeId"`
	Task            string          `json:"task"`
	Title           string          `json:"title,omitempty"`
	ModelSelection  ModelSelection  `json:"modelSelection"`
	RuntimeMode     RuntimeMode     `json:"runtimeMode"`
	InteractionMode InteractionMode `json:"interactionMode"`
	CreatedBy       Actor           `json:"createdBy"`
	CreationSource  CreationSource  `json:"creationSource"`
}

type ForkThread struct {
	SourceThreadID ThreadID        `json:"sourceThreadId"`
	SourcePoint    ForkSourcePoint `json:"sourcePoint"`
	Title          string          `json:"title,omitempty"`
	SideChat       bool            `json:"sideChat,omitempty"`
	CreatedBy      Actor           `json:"createdBy"`
	CreationSource CreationSource  `json:"creationSource"`
}

type CancelQueuedRun struct {
	RunID RunID `json:"runId"`
}

type ProviderSwitch struct {
	ModelSelection ModelSelection `json:"modelSelection"`
}

type RuntimeModeChange struct {
	RuntimeMode RuntimeMode `json:"runtimeMode"`
}

func commandPayload[T any](command Command) (T, bool) {
	switch payload := command.Payload.(type) {
	case T:
		return payload, true
	case *T:
		if payload != nil {
			return *payload, true
		}
	}
	var zero T
	return zero, false
}

func validateCommand(command Command) error {
	if err := requireID("commandId", command.CommandID); err != nil {
		return err
	}
	if err := requireID("threadId", command.ThreadID); err != nil {
		return err
	}
	switch command.Type {
	case CommandThreadCreate:
		payload, ok := commandPayload[CreateThread](command)
		if !ok {
			return fmt.Errorf("thread.create payload must be CreateThread: %w", ErrInvalidCommand)
		}
		if err := requireID("projectId", payload.ProjectID); err != nil {
			return err
		}
		if strings.TrimSpace(payload.Title) == "" || strings.TrimSpace(payload.ModelSelection.Model) == "" ||
			payload.ModelSelection.InstanceID == "" {
			return fmt.Errorf("title and model selection are required: %w", ErrInvalidCommand)
		}
	case CommandMessageDispatch:
		payload, ok := commandPayload[DispatchMessage](command)
		if !ok {
			return fmt.Errorf("message.dispatch payload must be DispatchMessage: %w", ErrInvalidCommand)
		}
		if payload.MessageID == "" {
			return fmt.Errorf("messageId is required: %w", ErrInvalidCommand)
		}
		switch payload.DispatchMode.Type {
		case "", DispatchDeferStart, DispatchQueueAfterActive, DispatchStartImmediately:
		case DispatchSteerActive, DispatchRestartActive:
			if payload.DispatchMode.TargetRunID == "" {
				return fmt.Errorf("targetRunId is required for %s: %w", payload.DispatchMode.Type, ErrInvalidCommand)
			}
		default:
			return fmt.Errorf("unknown dispatch mode %q: %w", payload.DispatchMode.Type, ErrInvalidCommand)
		}
	case CommandRunInterrupt:
		payload, ok := commandPayload[InterruptRun](command)
		if !ok || payload.RunID == "" {
			return fmt.Errorf("run.interrupt requires InterruptRun with runId: %w", ErrInvalidCommand)
		}
	case CommandRuntimeRequestRespond:
		payload, ok := commandPayload[RuntimeRequestResponse](command)
		if !ok || payload.RequestID == "" {
			return fmt.Errorf("runtime-request.respond requires RuntimeRequestResponse with requestId: %w", ErrInvalidCommand)
		}
	case CommandDelegatedTaskRequest:
		payload, ok := commandPayload[DelegateTask](command)
		if !ok || payload.ParentThreadID != command.ThreadID || payload.ParentRunID == "" || payload.ParentNodeID == "" ||
			strings.TrimSpace(payload.Task) == "" || payload.ModelSelection.InstanceID == "" || payload.ModelSelection.Model == "" {
			return fmt.Errorf("delegated_task.request requires an active parent, task, and model selection: %w", ErrInvalidCommand)
		}
	case CommandThreadFork:
		payload, ok := commandPayload[ForkThread](command)
		if !ok || payload.SourceThreadID == "" || payload.SourceThreadID == command.ThreadID {
			return fmt.Errorf("thread.fork requires a different sourceThreadId: %w", ErrInvalidCommand)
		}
		switch payload.SourcePoint.Type {
		case "latest_stable", "run", "checkpoint":
		default:
			return fmt.Errorf("unknown fork source point %q: %w", payload.SourcePoint.Type, ErrInvalidCommand)
		}
	case CommandQueuedRunCancel:
		payload, ok := commandPayload[CancelQueuedRun](command)
		if !ok || payload.RunID == "" {
			return fmt.Errorf("queued-run.cancel requires CancelQueuedRun with runId: %w", ErrInvalidCommand)
		}
	case CommandThreadRuntimeModeSet:
		payload, ok := commandPayload[RuntimeModeChange](command)
		if !ok || payload.RuntimeMode != RuntimeApprovalRequired && payload.RuntimeMode != RuntimeAutoAcceptEdits && payload.RuntimeMode != RuntimeAuto && payload.RuntimeMode != RuntimeFullAccess {
			return fmt.Errorf("thread.runtime-mode.set requires a known mode: %w", ErrInvalidCommand)
		}
	case CommandProviderSwitch:
		payload, ok := commandPayload[ProviderSwitch](command)
		if !ok || payload.ModelSelection.InstanceID == "" || strings.TrimSpace(payload.ModelSelection.Model) == "" {
			return fmt.Errorf("provider.switch requires a model selection: %w", ErrInvalidCommand)
		}
	case CommandThreadArchive, CommandThreadUnarchive, CommandThreadDelete, CommandQueueResume:
		if command.Payload != nil {
			return fmt.Errorf("%s does not take a payload: %w", command.Type, ErrInvalidCommand)
		}
	default:
		return fmt.Errorf("unknown command type %q: %w", command.Type, ErrInvalidCommand)
	}
	return nil
}

func creationDefaults(actor Actor, source CreationSource) (Actor, CreationSource) {
	if actor == "" {
		actor = ActorUser
	}
	if source == "" {
		source = CreationWeb
	}
	return actor, source
}

func threadDefaults(thread *Thread) {
	if thread.RuntimeMode == "" {
		thread.RuntimeMode = RuntimeFullAccess
	}
	if thread.InteractionMode == "" {
		thread.InteractionMode = InteractionDefault
	}
}

type EffectKind string

const (
	EffectProviderTurnStart             EffectKind = "provider.turn.start"
	EffectProviderTurnInterrupt         EffectKind = "provider.turn.interrupt"
	EffectProviderRuntimeRequestRespond EffectKind = "provider.runtime-request.respond"
)

type TurnStartEffect struct {
	Thread         Thread              `json:"thread"`
	Run            Run                 `json:"run"`
	Attempt        RunAttempt          `json:"attempt"`
	ProviderThread ProviderThread      `json:"providerThread"`
	Message        ConversationMessage `json:"message"`
	ContextHandoff *ContextHandoff     `json:"contextHandoff,omitempty"`
}

type TurnInterruptEffect struct {
	ProviderSessionID ProviderSessionID `json:"providerSessionId"`
	ProviderThreadID  ProviderThreadID  `json:"providerThreadId"`
	ProviderTurnID    ProviderTurnID    `json:"providerTurnId"`
	Reason            string            `json:"reason,omitempty"`
}

type RuntimeRequestRespondEffect struct {
	Request  RuntimeRequest         `json:"request"`
	Response RuntimeRequestResponse `json:"response"`
}

// Effect is post-commit provider work. Its ID is deterministic so an executor
// can deduplicate retries. A durable adapter should persist these in an outbox.
type Effect struct {
	ID        string     `json:"id"`
	Kind      EffectKind `json:"kind"`
	CommandID CommandID  `json:"commandId"`
	ThreadID  ThreadID   `json:"threadId"`
	Payload   any        `json:"payload"`
}

func (e Effect) MarshalJSON() ([]byte, error) {
	type effectAlias Effect
	return json.Marshal(effectAlias(e))
}

func (e *Effect) UnmarshalJSON(data []byte) error {
	type effectAlias Effect
	var decoded struct {
		effectAlias
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*e = Effect(decoded.effectAlias)
	var target any
	switch e.Kind {
	case EffectProviderTurnStart:
		target = &TurnStartEffect{}
	case EffectProviderTurnInterrupt:
		target = &TurnInterruptEffect{}
	case EffectProviderRuntimeRequestRespond:
		target = &RuntimeRequestRespondEffect{}
	default:
		var unknown any
		if err := json.Unmarshal(decoded.Payload, &unknown); err != nil {
			return err
		}
		e.Payload = unknown
		return nil
	}
	if err := json.Unmarshal(decoded.Payload, target); err != nil {
		return err
	}
	switch value := target.(type) {
	case *TurnStartEffect:
		e.Payload = *value
	case *TurnInterruptEffect:
		e.Payload = *value
	case *RuntimeRequestRespondEffect:
		e.Payload = *value
	}
	return nil
}

type DispatchResult struct {
	Sequence     uint64        `json:"sequence"`
	StoredEvents []StoredEvent `json:"storedEvents"`
	Replayed     bool          `json:"replayed,omitempty"`
	Effects      []Effect      `json:"effects,omitempty"`
}
