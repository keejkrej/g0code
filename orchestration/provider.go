package orchestration

import (
	"context"
	"fmt"
	"time"
)

type ProviderEventType string

const (
	ProviderEventAppThreadCreated      ProviderEventType = "app_thread.created"
	ProviderEventSessionUpdated        ProviderEventType = "provider_session.updated"
	ProviderEventThreadUpdated         ProviderEventType = "provider_thread.updated"
	ProviderEventTurnUpdated           ProviderEventType = "provider_turn.updated"
	ProviderEventNodeUpdated           ProviderEventType = "node.updated"
	ProviderEventSubagentUpdated       ProviderEventType = "subagent.updated"
	ProviderEventMessageUpdated        ProviderEventType = "message.updated"
	ProviderEventTurnItemUpdated       ProviderEventType = "turn_item.updated"
	ProviderEventRuntimeRequestUpdated ProviderEventType = "runtime_request.updated"
	ProviderEventPlanUpdated           ProviderEventType = "plan.updated"
	ProviderEventBackgroundWorkUpdated ProviderEventType = "background_work.updated"
	ProviderEventCommandOutputDelta    ProviderEventType = "command_output.delta"
	ProviderEventTurnTerminal          ProviderEventType = "turn.terminal"
)

type ProviderFailure struct {
	Class   string `json:"class"`
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
	ResetAt string `json:"resetAt,omitempty"`
}

type ProviderRetry struct {
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"maxAttempts"`
	DelayMS     int64  `json:"delayMs,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type TerminalEvent struct {
	ProviderThreadID   ProviderThreadID   `json:"providerThreadId"`
	ProviderTurnID     ProviderTurnID     `json:"providerTurnId"`
	RunOrdinal         int                `json:"runOrdinal"`
	Status             ProviderTurnStatus `json:"status"`
	Failure            *ProviderFailure   `json:"failure,omitempty"`
	Retry              *ProviderRetry     `json:"retry,omitempty"`
	FailureItemOrdinal int                `json:"failureItemOrdinal,omitempty"`
	ThreadDisposition  string             `json:"threadDisposition"`
}

type ProviderEvent struct {
	Type            ProviderEventType    `json:"type"`
	Driver          ProviderDriverKind   `json:"driver"`
	AppThread       *Thread              `json:"appThread,omitempty"`
	CreateOnly      bool                 `json:"createOnly,omitempty"`
	ProviderSession *ProviderSession     `json:"providerSession,omitempty"`
	ProviderThread  *ProviderThread      `json:"providerThread,omitempty"`
	ProviderTurn    *ProviderTurn        `json:"providerTurn,omitempty"`
	Node            *ExecutionNode       `json:"node,omitempty"`
	Subagent        *Subagent            `json:"subagent,omitempty"`
	Message         *ConversationMessage `json:"message,omitempty"`
	TurnItem        *TurnItem            `json:"turnItem,omitempty"`
	RuntimeRequest  *RuntimeRequest      `json:"runtimeRequest,omitempty"`
	Plan            *PlanArtifact        `json:"plan,omitempty"`
	Terminal        *TerminalEvent       `json:"terminal,omitempty"`
}

type RunGuard struct {
	RunID           RunID
	ActiveAttemptID RunAttemptID
	ExpectedStatus  RunStatus
}

type IngestInput struct {
	ProviderSessionID  ProviderSessionID
	ProviderInstanceID ProviderInstanceID
	CommandID          *CommandID
	ThreadID           ThreadID
	RunID              *RunID
	NodeID             *NodeID
	RawEventID         *RawEventID
	Event              ProviderEvent
	Guard              *RunGuard
}

type IngestResult struct {
	Sequence     uint64        `json:"sequence"`
	StoredEvents []StoredEvent `json:"storedEvents"`
	Effects      []Effect      `json:"effects,omitempty"`
	Replayed     bool          `json:"replayed,omitempty"`
}

func (o *Orchestrator) Ingest(ctx context.Context, input IngestInput) (IngestResult, error) {
	if input.ThreadID == "" {
		return IngestResult{}, fmt.Errorf("threadId is required: %w", ErrInvalidProviderEvent)
	}
	lock := o.lock(input.ThreadID)
	lock.Lock()
	defer lock.Unlock()
	if input.RawEventID != nil {
		existing, err := o.store.Read(ctx, EventFilter{RawEventID: input.RawEventID, Limit: 1})
		if err != nil {
			return IngestResult{}, err
		}
		if len(existing) > 0 {
			snapshot, err := o.store.Snapshot(ctx, input.ThreadID)
			return IngestResult{Sequence: snapshot.SnapshotSequence, Replayed: true}, err
		}
	}
	snapshot, err := o.store.Snapshot(ctx, input.ThreadID)
	if isNotFound(err) && input.Event.Type == ProviderEventAppThreadCreated && input.Event.AppThread != nil &&
		input.Event.AppThread.ID == input.ThreadID {
		snapshot, err = ThreadSnapshot{}, nil
	}
	if err != nil {
		return IngestResult{}, err
	}
	if input.Guard != nil {
		run, ok := snapshot.Projection.Run(input.Guard.RunID)
		if !ok || (input.Guard.ExpectedStatus != "" && run.Status != input.Guard.ExpectedStatus) || run.ActiveAttemptID == nil ||
			*run.ActiveAttemptID != input.Guard.ActiveAttemptID {
			return IngestResult{}, fmt.Errorf("provider event is stale for run %s: %w", input.Guard.RunID, ErrSequenceConflict)
		}
	}
	plan := &commandPlan{orchestrator: o, now: o.now().UTC(), projection: snapshot.Projection, ctx: ctx}
	plan.command.ThreadID = input.ThreadID
	if input.CommandID != nil {
		plan.command.CommandID = *input.CommandID
		plan.command.ThreadID = input.ThreadID
	}
	if err := o.normalizeProviderEvent(plan, input); err != nil {
		return IngestResult{}, fmt.Errorf("normalize %s for thread %s: %w", input.Event.Type, input.ThreadID, err)
	}
	if len(plan.events) == 0 {
		return IngestResult{Sequence: snapshot.SnapshotSequence, StoredEvents: []StoredEvent{}}, nil
	}
	committed, err := o.store.Commit(ctx, CommitInput{ThreadID: input.ThreadID,
		ExpectedSequence: snapshot.SnapshotSequence, Events: plan.events, Effects: plan.effects, CommandID: input.CommandID,
		ExpectedSequences: plan.expectedSequences})
	if err != nil {
		return IngestResult{}, fmt.Errorf("commit provider event: %w", err)
	}
	return IngestResult{Sequence: committed.Sequence, StoredEvents: committed.StoredEvents, Effects: plan.effects}, nil
}

func (o *Orchestrator) normalizeProviderEvent(plan *commandPlan, input IngestInput) error {
	event := input.Event
	base := eventSpec{threadID: input.ThreadID, runID: input.RunID, nodeID: input.NodeID,
		driver: event.Driver, instanceID: input.ProviderInstanceID, rawEventID: input.RawEventID}
	switch event.Type {
	case ProviderEventAppThreadCreated:
		if event.AppThread == nil {
			return fmt.Errorf("appThread is required: %w", ErrInvalidProviderEvent)
		}
		base.eventType = EventThreadCreated
		if plan.projection.Thread.ID != "" {
			if event.CreateOnly {
				return nil
			}
			base.eventType = EventThreadMetadataUpdated
		}
		base.payload = *event.AppThread
		_, err := plan.emitAndApply(base)
		return err
	case ProviderEventSessionUpdated:
		if event.ProviderSession == nil {
			return fmt.Errorf("providerSession is required: %w", ErrInvalidProviderEvent)
		}
		base.eventType, base.payload = EventProviderSessionUpdated, *event.ProviderSession
		_, err := plan.emitAndApply(base)
		return err
	case ProviderEventThreadUpdated:
		if event.ProviderThread == nil {
			return fmt.Errorf("providerThread is required: %w", ErrInvalidProviderEvent)
		}
		base.eventType, base.payload = EventProviderThreadUpdated, *event.ProviderThread
		_, err := plan.emitAndApply(base)
		return err
	case ProviderEventTurnUpdated:
		if event.ProviderTurn == nil {
			return fmt.Errorf("providerTurn is required: %w", ErrInvalidProviderEvent)
		}
		return o.planProviderTurn(plan, base, *event.ProviderTurn, nil)
	case ProviderEventNodeUpdated:
		if event.Node == nil {
			return fmt.Errorf("node is required: %w", ErrInvalidProviderEvent)
		}
		base.eventType, base.payload = EventNodeUpdated, *event.Node
		base.threadID, base.runID, base.nodeID = event.Node.ThreadID, event.Node.RunID, &event.Node.ID
		if current, ok := plan.projection.Node(event.Node.ID); ok && nodesEqual(current, *event.Node) {
			return nil
		}
		_, err := plan.emitAndApply(base)
		return err
	case ProviderEventSubagentUpdated:
		if event.Subagent == nil {
			return fmt.Errorf("subagent is required: %w", ErrInvalidProviderEvent)
		}
		base.eventType, base.payload = EventSubagentUpdated, *event.Subagent
		base.threadID, base.runID, base.nodeID = event.Subagent.ThreadID, event.Subagent.RunID, &event.Subagent.ID
		_, err := plan.emitAndApply(base)
		return err
	case ProviderEventMessageUpdated:
		if event.Message == nil {
			return fmt.Errorf("message is required: %w", ErrInvalidProviderEvent)
		}
		base.eventType, base.payload = EventMessageUpdated, *event.Message
		base.threadID, base.runID, base.nodeID = event.Message.ThreadID, event.Message.RunID, event.Message.NodeID
		_, err := plan.emitAndApply(base)
		return err
	case ProviderEventTurnItemUpdated:
		if event.TurnItem == nil {
			return fmt.Errorf("turnItem is required: %w", ErrInvalidProviderEvent)
		}
		base.eventType, base.payload = EventTurnItemUpdated, *event.TurnItem
		base.threadID, base.runID, base.nodeID = event.TurnItem.ThreadID, event.TurnItem.RunID, event.TurnItem.NodeID
		_, err := plan.emitAndApply(base)
		return err
	case ProviderEventRuntimeRequestUpdated:
		if event.RuntimeRequest == nil {
			return fmt.Errorf("runtimeRequest is required: %w", ErrInvalidProviderEvent)
		}
		request := *event.RuntimeRequest
		base.eventType, base.payload, base.nodeID = EventRuntimeRequestUpdated, request, &request.NodeID
		if _, err := plan.emitAndApply(base); err != nil {
			return err
		}
		if request.Status == RequestPending {
			if node, ok := plan.projection.Node(request.NodeID); ok && node.RunID != nil {
				node.Status = NodeWaiting
				if _, err := plan.emitAndApply(eventSpec{eventType: EventNodeUpdated, threadID: node.ThreadID,
					runID: node.RunID, nodeID: &node.ID, driver: event.Driver, instanceID: input.ProviderInstanceID,
					rawEventID: input.RawEventID, payload: node}); err != nil {
					return err
				}
				if run, ok := plan.projection.Run(*node.RunID); ok && run.Status == RunRunning {
					run.Status = RunWaiting
					if _, err := plan.emitAndApply(eventSpec{eventType: EventRunUpdated, threadID: run.ThreadID,
						runID: &run.ID, nodeID: run.RootNodeID, driver: event.Driver, instanceID: run.ProviderInstanceID,
						rawEventID: input.RawEventID, payload: run}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	case ProviderEventPlanUpdated:
		if event.Plan == nil {
			return fmt.Errorf("plan is required: %w", ErrInvalidProviderEvent)
		}
		planArtifact, err := cloneJSON(*event.Plan)
		if err != nil {
			return fmt.Errorf("clone plan: %w", err)
		}
		planArtifact = withPlanStepDurations(planArtifact, plan.projection.Plans, plan.now)
		base.eventType, base.payload = EventPlanUpdated, planArtifact
		base.threadID, base.runID, base.nodeID = planArtifact.ThreadID, planArtifact.RunID, &planArtifact.NodeID
		_, err = plan.emitAndApply(base)
		return err
	case ProviderEventTurnTerminal:
		if event.Terminal == nil {
			return fmt.Errorf("terminal is required: %w", ErrInvalidProviderEvent)
		}
		terminal := *event.Terminal
		turn, ok := plan.projection.ProviderTurn(terminal.ProviderTurnID)
		if !ok {
			return fmt.Errorf("provider turn %s not found: %w", terminal.ProviderTurnID, ErrInvalidProviderEvent)
		}
		if terminal.Status == ProviderTurnFailed && terminal.Failure != nil {
			item := providerFailureTurnItem(terminal, input, plan.now)
			base.eventType, base.payload = EventTurnItemUpdated, item
			base.runID, base.nodeID = item.RunID, item.NodeID
			if _, err := plan.emitAndApply(base); err != nil {
				return err
			}
		}
		turn.Status = terminal.Status
		return o.planProviderTurn(plan, base, turn, &terminal)
	case ProviderEventBackgroundWorkUpdated, ProviderEventCommandOutputDelta:
		// Live diagnostics and output deltas are deliberately not durable graph state.
		return nil
	default:
		return fmt.Errorf("unknown provider event %q: %w", event.Type, ErrInvalidProviderEvent)
	}
}

func nodesEqual(left, right ExecutionNode) bool {
	leftClone, leftErr := cloneJSON(left)
	rightClone, rightErr := cloneJSON(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return jsonValuesEqual(leftClone, rightClone)
}

func (o *Orchestrator) planProviderTurn(plan *commandPlan, base eventSpec, turn ProviderTurn, terminal *TerminalEvent) error {
	if turn.ProviderThreadID == "" || turn.NodeID == "" || turn.Ordinal < 1 {
		return fmt.Errorf("provider turn identity and ordinal are required: %w", ErrInvalidProviderEvent)
	}
	now := plan.now
	if turn.Status == ProviderTurnRunning {
		turn.StartedAt = updateTime(turn.StartedAt, now)
	}
	if turn.Status.Terminal() {
		turn.CompletedAt = updateTime(turn.CompletedAt, now)
	}
	base.eventType, base.payload = EventProviderTurnUpdated, turn
	base.nodeID = &turn.NodeID
	if _, err := plan.emitAndApply(base); err != nil {
		return err
	}

	// Pending native user inputs cannot outlive their provider callback turn.
	if turn.Status.Terminal() {
		for _, request := range plan.projection.RuntimeRequests {
			if request.ProviderTurnID == nil || *request.ProviderTurnID != turn.ID || request.Status != RequestPending {
				continue
			}
			request.Status = RequestCancelled
			request.ResolvedAt = &now
			if _, err := plan.emitAndApply(eventSpec{eventType: EventRuntimeRequestUpdated, threadID: plan.command.ThreadID,
				nodeID: &request.NodeID, driver: base.driver, instanceID: base.instanceID, rawEventID: base.rawEventID, payload: request}); err != nil {
				return err
			}
		}
	}

	node, ok := plan.projection.Node(turn.NodeID)
	if !ok {
		return fmt.Errorf("provider turn %s references unknown node %s: %w", turn.ID, turn.NodeID, ErrInvalidProviderEvent)
	}
	node.ProviderTurnID = &turn.ID
	if node.ProviderThreadID == nil {
		node.ProviderThreadID = &turn.ProviderThreadID
	}
	if turn.Status == ProviderTurnRunning {
		node.Status = NodeRunning
		node.StartedAt = updateTime(node.StartedAt, now)
	} else if turn.Status.Terminal() {
		node.Status = nodeStatusForTurn(turn.Status)
		node.CompletedAt = updateTime(node.CompletedAt, now)
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventNodeUpdated, threadID: node.ThreadID,
		runID: node.RunID, nodeID: &node.ID, driver: base.driver, instanceID: base.instanceID,
		rawEventID: base.rawEventID, payload: node}); err != nil {
		return err
	}

	if node.RunID == nil {
		return nil
	}
	run, ok := plan.projection.Run(*node.RunID)
	if !ok {
		return fmt.Errorf("node %s references unknown run %s: %w", node.ID, *node.RunID, ErrInvalidProviderEvent)
	}
	// A child provider turn has its own node and may settle independently. It
	// must never complete the parent run or its attempt.
	isActiveRoot := node.Kind == NodeRootTurn && node.CountsForRun && run.RootNodeID != nil &&
		*run.RootNodeID == node.ID && run.ActiveAttemptID != nil && turn.RunAttemptID != nil &&
		*run.ActiveAttemptID == *turn.RunAttemptID
	if !isActiveRoot {
		return nil
	}
	var attempt RunAttempt
	for _, candidate := range plan.projection.Attempts {
		if candidate.ID == *run.ActiveAttemptID {
			attempt = candidate
			break
		}
	}
	if attempt.ID == "" {
		return fmt.Errorf("active attempt %s not found: %w", *run.ActiveAttemptID, ErrInvalidProviderEvent)
	}
	attempt.ProviderTurnID = &turn.ID
	if turn.Status == ProviderTurnRunning {
		attempt.Status = AttemptRunning
		attempt.StartedAt = updateTime(attempt.StartedAt, now)
		run.Status = RunRunning
		run.StartedAt = updateTime(run.StartedAt, now)
	} else if turn.Status.Terminal() && !run.Status.Terminal() {
		attempt.Status = attemptStatusForTurn(turn.Status)
		attempt.CompletedAt = updateTime(attempt.CompletedAt, now)
		run.Status = runStatusForTurn(turn.Status)
		run.StartedAt = updateTime(run.StartedAt, now)
		run.CompletedAt = updateTime(run.CompletedAt, now)
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventRunAttemptUpdated, threadID: run.ThreadID,
		runID: &run.ID, nodeID: &node.ID, driver: base.driver, instanceID: base.instanceID,
		rawEventID: base.rawEventID, payload: attempt}); err != nil {
		return err
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventRunUpdated, threadID: run.ThreadID,
		runID: &run.ID, nodeID: &node.ID, driver: base.driver, instanceID: base.instanceID,
		rawEventID: base.rawEventID, payload: run}); err != nil {
		return err
	}
	if run.Status.Terminal() {
		if err := o.finalizeDelegatedTask(plan, run); err != nil {
			return err
		}
		if run.Status == RunInterrupted {
			requestItemID := DeriveRunSignalTurnItemID(run.ID, "interrupt-request")
			for _, item := range plan.projection.TurnItems {
				if item.ID != requestItemID {
					continue
				}
				resultItem := TurnItem{
					ID: DeriveRunSignalTurnItemID(run.ID, "interrupt-result"), ThreadID: run.ThreadID,
					RunID: &run.ID, NodeID: run.RootNodeID, ProviderThreadID: run.ProviderThreadID,
					ProviderTurnID: &turn.ID, ParentItemID: &requestItemID, Ordinal: nextTurnItemOrdinal(plan.projection),
					Status: TurnItemInterrupted, Title: "Run interrupted", StartedAt: &now,
					CompletedAt: &now, UpdatedAt: now, Type: "run_interrupt_result",
				}
				if _, err := plan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: run.ThreadID,
					runID: &run.ID, nodeID: run.RootNodeID, driver: base.driver, instanceID: base.instanceID,
					rawEventID: base.rawEventID, payload: resultItem}); err != nil {
					return err
				}
				break
			}
		}
		if providerThread, ok := plan.projection.ProviderThread(turn.ProviderThreadID); ok {
			providerThread.Status = ProviderThreadIdle
			if terminal != nil && terminal.ThreadDisposition == "broken" {
				providerThread.Status = ProviderThreadError
			}
			providerThread.UpdatedAt = now
			if _, err := plan.emitAndApply(eventSpec{eventType: EventProviderThreadUpdated, threadID: run.ThreadID,
				runID: &run.ID, driver: providerThread.Driver, instanceID: providerThread.ProviderInstanceID,
				rawEventID: base.rawEventID, payload: providerThread}); err != nil {
				return err
			}
		}
		return o.startNextQueuedRun(plan)
	}
	return nil
}

func nodeStatusForTurn(status ProviderTurnStatus) NodeStatus {
	switch status {
	case ProviderTurnCompleted:
		return NodeCompleted
	case ProviderTurnInterrupted:
		return NodeInterrupted
	case ProviderTurnFailed:
		return NodeFailed
	case ProviderTurnCancelled:
		return NodeCancelled
	default:
		return NodePending
	}
}

func attemptStatusForTurn(status ProviderTurnStatus) AttemptStatus {
	switch status {
	case ProviderTurnCompleted:
		return AttemptCompleted
	case ProviderTurnInterrupted:
		return AttemptInterrupted
	case ProviderTurnFailed:
		return AttemptFailed
	case ProviderTurnCancelled:
		return AttemptCancelled
	default:
		return AttemptPending
	}
}

func runStatusForTurn(status ProviderTurnStatus) RunStatus {
	switch status {
	case ProviderTurnCompleted:
		return RunCompleted
	case ProviderTurnInterrupted:
		return RunInterrupted
	case ProviderTurnFailed:
		return RunFailed
	case ProviderTurnCancelled:
		return RunCancelled
	default:
		return RunRunning
	}
}

func providerFailureTurnItem(terminal TerminalEvent, input IngestInput, now time.Time) TurnItem {
	nodeID := input.NodeID
	return TurnItem{
		ID:       TurnItemID(joinID("turn-item", "provider-turn", terminal.ProviderTurnID, "failure", terminal.FailureItemOrdinal)),
		ThreadID: input.ThreadID, RunID: input.RunID, NodeID: nodeID,
		ProviderThreadID: &terminal.ProviderThreadID, ProviderTurnID: &terminal.ProviderTurnID,
		Ordinal: terminal.FailureItemOrdinal, Status: TurnItemFailed, Title: terminal.Failure.Message,
		StartedAt: &now, CompletedAt: &now, UpdatedAt: now, Type: "provider_failure",
		Data: map[string]any{"failure": terminal.Failure, "retry": terminal.Retry},
	}
}

func withPlanStepDurations(next PlanArtifact, existing []PlanArtifact, now time.Time) PlanArtifact {
	if next.Kind != "todo_list" {
		return next
	}
	var previous *PlanArtifact
	for index := range existing {
		if existing[index].ID == next.ID && existing[index].Kind == next.Kind {
			previous = &existing[index]
			break
		}
	}
	for index := range next.Steps {
		step := &next.Steps[index]
		var prior *PlanStep
		if previous != nil {
			for priorIndex := range previous.Steps {
				candidate := &previous.Steps[priorIndex]
				if candidate.ID == step.ID && candidate.Text == step.Text {
					prior = candidate
					break
				}
			}
		}
		switch step.Status {
		case "running":
			if prior != nil && prior.Status == "running" && prior.DurationAnchorAt != nil {
				step.DurationAnchorAt = prior.DurationAnchorAt
			} else {
				step.DurationAnchorAt = &now
			}
		case "completed":
			if prior != nil && prior.Status == "completed" {
				step.DurationMS = prior.DurationMS
				continue
			}
			if prior != nil && prior.DurationAnchorAt != nil {
				duration := max(0, now.Sub(*prior.DurationAnchorAt).Milliseconds())
				step.DurationMS = &duration
			}
		}
	}
	return next
}
