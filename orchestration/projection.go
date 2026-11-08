package orchestration

import (
	"fmt"
	"sort"
	"time"
)

func emptyProjection(event Event, thread Thread) Projection {
	return Projection{
		Thread:           thread,
		Runs:             []Run{},
		Attempts:         []RunAttempt{},
		Nodes:            []ExecutionNode{},
		Subagents:        []Subagent{},
		ProviderSessions: []ProviderSession{},
		ProviderThreads:  []ProviderThread{},
		ProviderTurns:    []ProviderTurn{},
		RuntimeRequests:  []RuntimeRequest{},
		Messages:         []ConversationMessage{},
		Plans:            []PlanArtifact{},
		TurnItems:        []TurnItem{},
		CheckpointScopes: []CheckpointScope{},
		Checkpoints:      []Checkpoint{},
		ContextHandoffs:  []ContextHandoff{},
		ContextTransfers: []ContextTransfer{},
		UpdatedAt:        event.OccurredAt,
	}
}

func upsertByID[T any](items []T, next T, id func(T) string) []T {
	for index := range items {
		if id(items[index]) == id(next) {
			updated := append([]T(nil), items...)
			updated[index] = next
			return updated
		}
	}
	return append(items, next)
}

func isQueuedProviderThreadPlaceholder(thread ProviderThread) bool {
	return thread.Status == ProviderThreadNotLoaded && thread.FirstRunOrdinal == nil &&
		thread.NativeThreadRef == nil && thread.ProviderSessionID == nil
}

// Apply folds one normalized event into a projection. Entity rows are upserted
// in insertion order, making the result stable for replay and tests.
func Apply(projection Projection, event Event) (Projection, error) {
	if event.ThreadID == "" || event.OccurredAt.IsZero() {
		return projection, fmt.Errorf("event thread and occurredAt are required: %w", ErrInvalidProviderEvent)
	}

	if event.Type == EventThreadCreated {
		thread, ok := event.Payload.(Thread)
		if !ok {
			return projection, fmt.Errorf("thread.created payload is %T: %w", event.Payload, ErrInvalidProviderEvent)
		}
		return emptyProjection(event, thread), nil
	}
	if projection.Thread.ID == "" {
		return projection, ErrThreadNotFound
	}

	base := projection
	base.UpdatedAt = event.OccurredAt
	base.Thread.UpdatedAt = event.OccurredAt

	switch event.Type {
	case EventThreadArchived, EventThreadUnarchived, EventThreadDeleted,
		EventThreadSettled, EventThreadUnsettled, EventThreadSnoozed,
		EventThreadUnsnoozed, EventThreadPinned, EventThreadUnpinned,
		EventThreadMetadataUpdated, EventThreadRuntimeModeUpdated,
		EventThreadInteractionModeUpdated, EventThreadModelSelectionUpdated,
		EventThreadProviderSwitched:
		thread, ok := event.Payload.(Thread)
		if !ok {
			return projection, payloadTypeError(event, "Thread")
		}
		base.Thread = thread
	case EventThreadVisited, EventThreadMarkedUnread:
		thread, ok := event.Payload.(Thread)
		if !ok {
			return projection, payloadTypeError(event, "Thread")
		}
		// Read watermarks do not constitute thread activity.
		base = projection
		base.Thread = thread
	case EventRunCreated, EventRunUpdated:
		run, ok := event.Payload.(Run)
		if !ok {
			return projection, payloadTypeError(event, "Run")
		}
		base.Runs = upsertByID(base.Runs, run, func(row Run) string { return string(row.ID) })
	case EventRunBackgroundWorkCancelled:
		_, ok := event.Payload.(RunBackgroundWorkCancelled)
		if !ok {
			return projection, payloadTypeError(event, "RunBackgroundWorkCancelled")
		}
	case EventRunAttemptCreated, EventRunAttemptUpdated:
		attempt, ok := event.Payload.(RunAttempt)
		if !ok {
			return projection, payloadTypeError(event, "RunAttempt")
		}
		base.Attempts = upsertByID(base.Attempts, attempt, func(row RunAttempt) string { return string(row.ID) })
	case EventNodeUpdated:
		node, ok := event.Payload.(ExecutionNode)
		if !ok {
			return projection, payloadTypeError(event, "ExecutionNode")
		}
		base.Nodes = upsertByID(base.Nodes, node, func(row ExecutionNode) string { return string(row.ID) })
	case EventSubagentUpdated:
		subagent, ok := event.Payload.(Subagent)
		if !ok {
			return projection, payloadTypeError(event, "Subagent")
		}
		base.Subagents = upsertByID(base.Subagents, subagent, func(row Subagent) string { return string(row.ID) })
	case EventProviderSessionAttached, EventProviderSessionUpdated:
		session, ok := event.Payload.(ProviderSession)
		if !ok {
			return projection, payloadTypeError(event, "ProviderSession")
		}
		base.ProviderSessions = upsertByID(base.ProviderSessions, session, func(row ProviderSession) string { return string(row.ID) })
	case EventProviderSessionDetached:
		detached, ok := event.Payload.(ProviderSessionDetached)
		if !ok {
			return projection, payloadTypeError(event, "ProviderSessionDetached")
		}
		filtered := base.ProviderSessions[:0]
		for _, session := range base.ProviderSessions {
			if session.ID != detached.ProviderSessionID {
				filtered = append(filtered, session)
			}
		}
		base.ProviderSessions = filtered
	case EventProviderThreadUpdated:
		thread, ok := event.Payload.(ProviderThread)
		if !ok {
			return projection, payloadTypeError(event, "ProviderThread")
		}
		if thread.AppThreadID != nil && *thread.AppThreadID == base.Thread.ID && !isQueuedProviderThreadPlaceholder(thread) {
			id := thread.ID
			base.Thread.ActiveProviderThreadID = &id
		}
		base.ProviderThreads = upsertByID(base.ProviderThreads, thread, func(row ProviderThread) string { return string(row.ID) })
	case EventProviderTurnUpdated:
		turn, ok := event.Payload.(ProviderTurn)
		if !ok {
			return projection, payloadTypeError(event, "ProviderTurn")
		}
		for _, current := range base.ProviderTurns {
			if current.ID == turn.ID && turn.TokenUsage == nil {
				turn.TokenUsage = current.TokenUsage
				break
			}
		}
		base.ProviderTurns = upsertByID(base.ProviderTurns, turn, func(row ProviderTurn) string { return string(row.ID) })
	case EventRuntimeRequestUpdated:
		request, ok := event.Payload.(RuntimeRequest)
		if !ok {
			return projection, payloadTypeError(event, "RuntimeRequest")
		}
		base.RuntimeRequests = upsertByID(base.RuntimeRequests, request, func(row RuntimeRequest) string { return string(row.ID) })
	case EventMessageUpdated:
		message, ok := event.Payload.(ConversationMessage)
		if !ok {
			return projection, payloadTypeError(event, "ConversationMessage")
		}
		base.Messages = upsertByID(base.Messages, message, func(row ConversationMessage) string { return string(row.ID) })
		runOrdinals := make(map[RunID]int, len(base.Runs))
		for _, run := range base.Runs {
			runOrdinals[run.ID] = run.Ordinal
		}
		// Conversation history is grouped by app run even when a queued user
		// message was durably accepted before the previous assistant finished.
		sort.SliceStable(base.Messages, func(i, j int) bool {
			left, right := base.Messages[i], base.Messages[j]
			leftOrdinal, rightOrdinal := -1, -1
			if left.RunID != nil {
				leftOrdinal = runOrdinals[*left.RunID]
			}
			if right.RunID != nil {
				rightOrdinal = runOrdinals[*right.RunID]
			}
			if leftOrdinal != rightOrdinal {
				return leftOrdinal < rightOrdinal
			}
			return left.Role == MessageUser && right.Role != MessageUser
		})
	case EventTurnItemUpdated:
		item, ok := event.Payload.(TurnItem)
		if !ok {
			return projection, payloadTypeError(event, "TurnItem")
		}
		base.TurnItems = upsertByID(base.TurnItems, item, func(row TurnItem) string { return string(row.ID) })
	case EventPlanUpdated:
		plan, ok := event.Payload.(PlanArtifact)
		if !ok {
			return projection, payloadTypeError(event, "PlanArtifact")
		}
		base.Plans = upsertByID(base.Plans, plan, func(row PlanArtifact) string { return string(row.ID) })
	case EventCheckpointScopeCreated:
		scope, ok := event.Payload.(CheckpointScope)
		if !ok {
			return projection, payloadTypeError(event, "CheckpointScope")
		}
		base.CheckpointScopes = upsertByID(base.CheckpointScopes, scope, func(row CheckpointScope) string { return string(row.ID) })
	case EventCheckpointCaptured:
		checkpoint, ok := event.Payload.(Checkpoint)
		if !ok {
			return projection, payloadTypeError(event, "Checkpoint")
		}
		base.Checkpoints = upsertByID(base.Checkpoints, checkpoint, func(row Checkpoint) string { return string(row.ID) })
	case EventCheckpointRollbackRequested:
		if _, ok := event.Payload.(CheckpointRollbackRequest); !ok {
			return projection, payloadTypeError(event, "CheckpointRollbackRequest")
		}
	case EventContextHandoffUpdated:
		handoff, ok := event.Payload.(ContextHandoff)
		if !ok {
			return projection, payloadTypeError(event, "ContextHandoff")
		}
		base.ContextHandoffs = upsertByID(base.ContextHandoffs, handoff, func(row ContextHandoff) string { return string(row.ID) })
	case EventContextTransferCreated, EventContextTransferUpdated:
		transfer, ok := event.Payload.(ContextTransfer)
		if !ok {
			return projection, payloadTypeError(event, "ContextTransfer")
		}
		base.ContextTransfers = upsertByID(base.ContextTransfers, transfer, func(row ContextTransfer) string { return string(row.ID) })
	default:
		// Unknown newer events still advance the cursor, but cannot mutate rows
		// whose semantics this version does not understand.
	}
	return base, nil
}

func payloadTypeError(event Event, expected string) error {
	return fmt.Errorf("%s payload must be %s, got %T: %w", event.Type, expected, event.Payload, ErrInvalidProviderEvent)
}

// Replay rebuilds projections from the durable log. Sequence order, rather
// than provider timestamps or IDs, is authoritative.
func Replay(events []StoredEvent) (map[ThreadID]Projection, error) {
	ordered := append([]StoredEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Sequence < ordered[j].Sequence })
	projections := make(map[ThreadID]Projection)
	for _, stored := range ordered {
		event := stored.Event
		projection := projections[event.ThreadID]
		next, err := Apply(projection, event)
		if err != nil {
			return nil, fmt.Errorf("replay event %d (%s): %w", stored.Sequence, event.Type, err)
		}
		projections[event.ThreadID] = next
	}
	return projections, nil
}

func latestRun(runs []Run) (Run, bool) {
	if len(runs) == 0 {
		return Run{}, false
	}
	latest := runs[0]
	for _, run := range runs[1:] {
		if run.Ordinal > latest.Ordinal {
			latest = run
		}
	}
	return latest, true
}

func updateTime(value *time.Time, now time.Time) *time.Time {
	if value != nil {
		return value
	}
	return &now
}
