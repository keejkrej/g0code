package orchestration

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	EventThreadCreated                EventType = "thread.created"
	EventThreadArchived               EventType = "thread.archived"
	EventThreadUnarchived             EventType = "thread.unarchived"
	EventThreadDeleted                EventType = "thread.deleted"
	EventThreadSettled                EventType = "thread.settled"
	EventThreadUnsettled              EventType = "thread.unsettled"
	EventThreadSnoozed                EventType = "thread.snoozed"
	EventThreadUnsnoozed              EventType = "thread.unsnoozed"
	EventThreadPinned                 EventType = "thread.pinned"
	EventThreadUnpinned               EventType = "thread.unpinned"
	EventThreadMetadataUpdated        EventType = "thread.metadata-updated"
	EventThreadVisited                EventType = "thread.visited"
	EventThreadMarkedUnread           EventType = "thread.marked-unread"
	EventThreadRuntimeModeUpdated     EventType = "thread.runtime-mode-updated"
	EventThreadInteractionModeUpdated EventType = "thread.interaction-mode-updated"
	EventThreadModelSelectionUpdated  EventType = "thread.model-selection-updated"
	EventThreadProviderSwitched       EventType = "thread.provider-switched"
	EventRunCreated                   EventType = "run.created"
	EventRunUpdated                   EventType = "run.updated"
	EventRunBackgroundWorkCancelled   EventType = "run.background-work-cancelled"
	EventRunAttemptCreated            EventType = "run-attempt.created"
	EventRunAttemptUpdated            EventType = "run-attempt.updated"
	EventNodeUpdated                  EventType = "node.updated"
	EventSubagentUpdated              EventType = "subagent.updated"
	EventProviderSessionAttached      EventType = "provider-session.attached"
	EventProviderSessionUpdated       EventType = "provider-session.updated"
	EventProviderSessionDetached      EventType = "provider-session.detached"
	EventProviderThreadUpdated        EventType = "provider-thread.updated"
	EventProviderTurnUpdated          EventType = "provider-turn.updated"
	EventRuntimeRequestUpdated        EventType = "runtime-request.updated"
	EventMessageUpdated               EventType = "message.updated"
	EventTurnItemUpdated              EventType = "turn-item.updated"
	EventPlanUpdated                  EventType = "plan.updated"
	EventCheckpointScopeCreated       EventType = "checkpoint-scope.created"
	EventCheckpointCaptured           EventType = "checkpoint.captured"
	EventCheckpointRollbackRequested  EventType = "checkpoint.rollback-requested"
	EventContextHandoffUpdated        EventType = "context-handoff.updated"
	EventContextTransferCreated       EventType = "context-transfer.created"
	EventContextTransferUpdated       EventType = "context-transfer.updated"
)

type ProviderSessionDetached struct {
	ProviderSessionID ProviderSessionID `json:"providerSessionId"`
	DetachedAt        time.Time         `json:"detachedAt"`
	Reason            string            `json:"reason,omitempty"`
}

type RunBackgroundWorkCancelled struct {
	RunID RunID `json:"runId"`
}

type CheckpointRollbackRequest struct {
	RequestID    CommandID         `json:"requestId"`
	ThreadID     ThreadID          `json:"threadId"`
	ScopeID      CheckpointScopeID `json:"scopeId"`
	CheckpointID CheckpointID      `json:"checkpointId"`
	RestoreFiles bool              `json:"restoreFiles"`
	RequestedAt  time.Time         `json:"requestedAt"`
}

// Event is a normalized durable domain event. Payload is one of the concrete
// entity types selected by Type; custom JSON keeps the same tagged event shape
// as t3code instead of exposing Go's union representation on the wire.
type Event struct {
	ID                 EventID            `json:"id"`
	ThreadID           ThreadID           `json:"threadId"`
	RunID              *RunID             `json:"runId,omitempty"`
	NodeID             *NodeID            `json:"nodeId,omitempty"`
	Driver             ProviderDriverKind `json:"driver,omitempty"`
	ProviderInstanceID ProviderInstanceID `json:"providerInstanceId,omitempty"`
	RawEventID         *RawEventID        `json:"rawEventId,omitempty"`
	OccurredAt         time.Time          `json:"occurredAt"`
	Type               EventType          `json:"type"`
	Payload            any                `json:"payload"`
}

type StoredEvent struct {
	Sequence  uint64     `json:"sequence"`
	CommandID *CommandID `json:"commandId"`
	Event     Event      `json:"event"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	type eventAlias Event
	return json.Marshal(eventAlias(e))
}

func (e *Event) UnmarshalJSON(data []byte) error {
	type eventAlias Event
	var decoded struct {
		eventAlias
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*e = Event(decoded.eventAlias)
	payload, err := decodeEventPayload(e.Type, decoded.Payload)
	if err != nil {
		return err
	}
	e.Payload = payload
	return nil
}

func decodeEventPayload(eventType EventType, data json.RawMessage) (any, error) {
	var target any
	switch eventType {
	case EventThreadCreated, EventThreadArchived, EventThreadUnarchived,
		EventThreadDeleted, EventThreadSettled, EventThreadUnsettled,
		EventThreadSnoozed, EventThreadUnsnoozed, EventThreadPinned,
		EventThreadUnpinned, EventThreadMetadataUpdated, EventThreadVisited,
		EventThreadMarkedUnread, EventThreadRuntimeModeUpdated,
		EventThreadInteractionModeUpdated, EventThreadModelSelectionUpdated,
		EventThreadProviderSwitched:
		target = &Thread{}
	case EventRunCreated, EventRunUpdated:
		target = &Run{}
	case EventRunBackgroundWorkCancelled:
		target = &RunBackgroundWorkCancelled{}
	case EventRunAttemptCreated, EventRunAttemptUpdated:
		target = &RunAttempt{}
	case EventNodeUpdated:
		target = &ExecutionNode{}
	case EventSubagentUpdated:
		target = &Subagent{}
	case EventProviderSessionAttached, EventProviderSessionUpdated:
		target = &ProviderSession{}
	case EventProviderSessionDetached:
		target = &ProviderSessionDetached{}
	case EventProviderThreadUpdated:
		target = &ProviderThread{}
	case EventProviderTurnUpdated:
		target = &ProviderTurn{}
	case EventRuntimeRequestUpdated:
		target = &RuntimeRequest{}
	case EventMessageUpdated:
		target = &ConversationMessage{}
	case EventTurnItemUpdated:
		target = &TurnItem{}
	case EventPlanUpdated:
		target = &PlanArtifact{}
	case EventCheckpointScopeCreated:
		target = &CheckpointScope{}
	case EventCheckpointCaptured:
		target = &Checkpoint{}
	case EventCheckpointRollbackRequested:
		target = &CheckpointRollbackRequest{}
	case EventContextHandoffUpdated:
		target = &ContextHandoff{}
	case EventContextTransferCreated, EventContextTransferUpdated:
		target = &ContextTransfer{}
	default:
		// Preserve events written by a newer server for forward-compatible
		// replication. Apply ignores an unknown payload rather than guessing.
		var raw any
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		return raw, nil
	}
	if err := json.Unmarshal(data, target); err != nil {
		return nil, fmt.Errorf("decode %s payload: %w", eventType, err)
	}
	switch value := target.(type) {
	case *Thread:
		return *value, nil
	case *Run:
		return *value, nil
	case *RunBackgroundWorkCancelled:
		return *value, nil
	case *RunAttempt:
		return *value, nil
	case *ExecutionNode:
		return *value, nil
	case *Subagent:
		return *value, nil
	case *ProviderSession:
		return *value, nil
	case *ProviderSessionDetached:
		return *value, nil
	case *ProviderThread:
		return *value, nil
	case *ProviderTurn:
		return *value, nil
	case *RuntimeRequest:
		return *value, nil
	case *ConversationMessage:
		return *value, nil
	case *TurnItem:
		return *value, nil
	case *PlanArtifact:
		return *value, nil
	case *CheckpointScope:
		return *value, nil
	case *Checkpoint:
		return *value, nil
	case *CheckpointRollbackRequest:
		return *value, nil
	case *ContextHandoff:
		return *value, nil
	case *ContextTransfer:
		return *value, nil
	default:
		panic("unreachable event payload")
	}
}
