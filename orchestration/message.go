package orchestration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

func (o *Orchestrator) planMessageDispatch(ctx context.Context, plan *commandPlan) error {
	command, _ := commandPayload[DispatchMessage](plan.command)
	if plan.projection.Thread.ArchivedAt != nil || plan.projection.Thread.DeletedAt != nil {
		return fmt.Errorf("archived or deleted threads cannot receive messages: %w", ErrCommandRejected)
	}
	for _, message := range plan.projection.Messages {
		if message.ID == command.MessageID {
			return fmt.Errorf("message %s already exists: %w", command.MessageID, ErrCommandRejected)
		}
	}
	actor, source := creationDefaults(command.CreatedBy, command.CreationSource)

	if plan.projection.Thread.LastVisitedAt == nil && actor == ActorUser {
		thread := plan.projection.Thread
		visitedAt := plan.now.Add(-1_000_000)
		thread.LastVisitedAt = &visitedAt
		thread.UpdatedAt = plan.now
		if _, err := plan.emitAndApply(eventSpec{eventType: EventThreadMetadataUpdated, threadID: thread.ID,
			instanceID: thread.ProviderInstanceID, payload: thread}); err != nil {
			return err
		}
	}
	if command.TitleSeed != "" && len(plan.projection.Messages) == 0 {
		thread := plan.projection.Thread
		thread.Title = command.TitleSeed
		thread.UpdatedAt = plan.now
		if _, err := plan.emitAndApply(eventSpec{eventType: EventThreadMetadataUpdated, threadID: thread.ID,
			instanceID: thread.ProviderInstanceID, payload: thread}); err != nil {
			return err
		}
	}

	model := plan.projection.Thread.ModelSelection
	if command.ModelSelection != nil {
		model = *command.ModelSelection
	}
	provider, err := o.resolveProvider(ctx, model.InstanceID)
	if err != nil {
		return fmt.Errorf("resolve provider %s: %w", model.InstanceID, err)
	}
	if !plan.projection.Thread.ModelSelection.Equal(model) {
		thread := plan.projection.Thread
		eventType := EventThreadModelSelectionUpdated
		if thread.ProviderInstanceID != model.InstanceID {
			eventType = EventThreadProviderSwitched
		}
		thread.ModelSelection = model
		thread.ProviderInstanceID = model.InstanceID
		thread.UpdatedAt = plan.now
		if _, err := plan.emitAndApply(eventSpec{eventType: eventType, threadID: thread.ID,
			instanceID: model.InstanceID, payload: thread}); err != nil {
			return err
		}
	}

	mode := command.DispatchMode
	if mode.Type == "" {
		mode.Type = DispatchStartImmediately
	}
	if mode.Type == DispatchSteerActive || mode.Type == DispatchRestartActive {
		// Steering needs a live provider capability decision and belongs in the
		// provider-control adapter, not in an implicit prompt rewrite.
		return fmt.Errorf("%s requires the provider steering adapter: %w", mode.Type, ErrUnsupportedCapability)
	}
	activeRun, hasActiveRun := plan.projection.ActiveRun()
	_ = activeRun
	queueHeld := queueIsHeld(plan.projection)
	userReleasesQueue := actor == ActorUser && (source == CreationWeb || source == CreationMobile)
	if queueHeld && userReleasesQueue {
		if _, err := releaseQueueHolds(plan); err != nil {
			return err
		}
		queueHeld = false
	}
	queued := hasActiveRun || mode.Type == DispatchQueueAfterActive || (queueHeld && actor != ActorUser)
	if queued && !provider.Capabilities.Turns.SupportsQueuedMessages {
		return fmt.Errorf("provider %s does not support queued messages: %w", model.InstanceID, ErrUnsupportedCapability)
	}

	ordinal := plan.projection.NextRunOrdinal()
	runID := DeriveRunID(plan.command.ThreadID, ordinal)
	providerThread, providerThreadNew, err := selectProviderThread(plan.projection, provider.Driver, model.InstanceID, runID, plan.now)
	if err != nil {
		return err
	}

	var handoff *ContextHandoff
	if err := o.resolvePendingFork(ctx, plan, provider, &providerThread, runID, &handoff); err != nil {
		return err
	}
	if handoff == nil && plan.projection.Thread.ActiveProviderThreadID != nil {
		activeProviderID := *plan.projection.Thread.ActiveProviderThreadID
		if activeThread, ok := plan.projection.ProviderThread(activeProviderID); ok && activeThread.ProviderInstanceID != model.InstanceID {
			handoff, err = o.planProviderHandoff(plan, activeThread, providerThread, runID)
			if err != nil {
				return err
			}
		}
	}
	if handoff != nil {
		providerThread.HandoffIDs = appendUniqueHandoff(providerThread.HandoffIDs, handoff.ID)
		providerThread.UpdatedAt = plan.now
		providerThreadNew = true
	}

	status := RunStarting
	if mode.Type == DispatchDeferStart {
		status = RunPreparing
	}
	if queued {
		status = RunQueued
	}
	if status == RunStarting || status == RunPreparing {
		if providerThread.FirstRunOrdinal == nil {
			providerThread.FirstRunOrdinal = ptr(ordinal)
		}
		providerThread.LastRunOrdinal = ptr(ordinal)
		providerThread.UpdatedAt = plan.now
		providerThreadNew = true
	}
	if providerThreadNew {
		if _, err := plan.emitAndApply(eventSpec{eventType: EventProviderThreadUpdated, threadID: plan.command.ThreadID,
			driver: provider.Driver, instanceID: model.InstanceID, payload: providerThread}); err != nil {
			return err
		}
	}
	if handoff != nil {
		if _, err := plan.emitAndApply(eventSpec{eventType: EventContextHandoffUpdated, threadID: plan.command.ThreadID,
			runID: &runID, driver: provider.Driver, instanceID: model.InstanceID, payload: *handoff}); err != nil {
			return err
		}
	}

	attemptID := DeriveRunAttemptID(runID, 1)
	rootNodeID := DeriveRootNodeID(runID)
	providerThreadID := providerThread.ID
	run := Run{
		ID: runID, ThreadID: plan.command.ThreadID, Ordinal: ordinal,
		ProviderInstanceID: model.InstanceID, ModelSelection: model,
		ProviderThreadID: &providerThreadID, UserMessageID: command.MessageID,
		RootNodeID: &rootNodeID, ActiveAttemptID: &attemptID, Status: status,
		RequestedAt: plan.now,
	}
	if handoff != nil {
		run.ContextHandoffID = &handoff.ID
	}
	if queued {
		position := 1
		for _, candidate := range plan.projection.Runs {
			if candidate.Status != RunQueued {
				continue
			}
			if candidate.QueueHeld {
				queueHeld = true
			}
			candidatePosition := candidate.Ordinal
			if candidate.QueuePosition != nil {
				candidatePosition = *candidate.QueuePosition
			}
			if candidatePosition >= position {
				position = candidatePosition + 1
			}
		}
		run.QueuePosition = &position
		run.QueueHeld = queueHeld
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventRunCreated, threadID: run.ThreadID,
		runID: &runID, nodeID: &rootNodeID, driver: provider.Driver, instanceID: model.InstanceID, payload: run}); err != nil {
		return err
	}
	attempt := RunAttempt{
		ID: attemptID, RunID: runID, AttemptOrdinal: 1, RootNodeID: rootNodeID,
		ProviderInstanceID: model.InstanceID, ProviderThreadID: providerThread.ID,
		Reason: "initial", Status: AttemptPending,
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventRunAttemptCreated, threadID: run.ThreadID,
		runID: &runID, nodeID: &rootNodeID, driver: provider.Driver, instanceID: model.InstanceID, payload: attempt}); err != nil {
		return err
	}
	root := ExecutionNode{
		ID: rootNodeID, ThreadID: run.ThreadID, RunID: &runID, RootNodeID: rootNodeID,
		Kind: NodeRootTurn, Status: NodePending, CountsForRun: true, ProviderThreadID: &providerThreadID,
	}
	if status != RunPreparing && plan.projection.Thread.WorktreePath != nil {
		scope := CheckpointScope{
			ID: DeriveCheckpointScopeID(run.ThreadID, "root"), ThreadID: run.ThreadID,
			RunID: &runID, NodeID: rootNodeID, ProviderThreadID: &providerThreadID,
			Kind: "root_run", AdvancesAppRunCount: true, CWD: *plan.projection.Thread.WorktreePath, CreatedAt: plan.now,
		}
		root.CheckpointScopeID = &scope.ID
		if _, err := plan.emitAndApply(eventSpec{eventType: EventCheckpointScopeCreated, threadID: run.ThreadID,
			runID: &runID, nodeID: &rootNodeID, driver: provider.Driver, instanceID: model.InstanceID, payload: scope}); err != nil {
			return err
		}
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventNodeUpdated, threadID: run.ThreadID,
		runID: &runID, nodeID: &rootNodeID, driver: provider.Driver, instanceID: model.InstanceID, payload: root}); err != nil {
		return err
	}
	message := ConversationMessage{
		CreatedBy: actor, CreationSource: source, ID: command.MessageID, ThreadID: run.ThreadID,
		RunID: &runID, NodeID: &rootNodeID, Role: MessageUser, Text: command.Text,
		Attachments: append([]Attachment(nil), command.Attachments...), CreatedAt: plan.now, UpdatedAt: plan.now,
	}
	if message.Attachments == nil {
		message.Attachments = []Attachment{}
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventMessageUpdated, threadID: run.ThreadID,
		runID: &runID, nodeID: &rootNodeID, driver: provider.Driver, instanceID: model.InstanceID, payload: message}); err != nil {
		return err
	}
	itemStatus := TurnItemCompleted
	inputIntent := "turn_start"
	if queued {
		itemStatus = TurnItemPending
		inputIntent = "queued_turn"
	}
	item := TurnItem{
		CreatedBy: actor, CreationSource: source,
		ID: DeriveUserTurnItemID(message.ID), ThreadID: run.ThreadID, RunID: &runID,
		NodeID: &rootNodeID, ProviderThreadID: &providerThreadID, Ordinal: nextTurnItemOrdinal(plan.projection),
		Status: itemStatus, StartedAt: &plan.now, UpdatedAt: plan.now, Type: "user_message",
		MessageID: message.ID, Text: message.Text,
		Data: map[string]any{"inputIntent": inputIntent, "attachments": message.Attachments},
	}
	if itemStatus == TurnItemCompleted {
		item.CompletedAt = &plan.now
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: run.ThreadID,
		runID: &runID, nodeID: &rootNodeID, driver: provider.Driver, instanceID: model.InstanceID, payload: item}); err != nil {
		return err
	}
	if status == RunStarting {
		return o.appendTurnStartEffect(plan, run)
	}
	return nil
}

func selectProviderThread(projection Projection, driver ProviderDriverKind, instanceID ProviderInstanceID, runID RunID, nowTime time.Time) (ProviderThread, bool, error) {
	if projection.Thread.ActiveProviderThreadID != nil {
		active, ok := projection.ProviderThread(*projection.Thread.ActiveProviderThreadID)
		if ok && active.ProviderInstanceID == instanceID {
			return active, false, nil
		}
	}
	candidates := make([]ProviderThread, 0)
	for _, candidate := range projection.ProviderThreads {
		if candidate.AppThreadID != nil && *candidate.AppThreadID == projection.Thread.ID &&
			candidate.OwnerNodeID == nil && candidate.ProviderInstanceID == instanceID &&
			candidate.Status != ProviderThreadClosed && candidate.Status != ProviderThreadError {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) > 0 {
		sort.SliceStable(candidates, func(i, j int) bool {
			return runOrdinalValue(candidates[i].LastRunOrdinal) > runOrdinalValue(candidates[j].LastRunOrdinal)
		})
		return candidates[0], false, nil
	}
	threadID := DeriveProviderThreadID(driver, instanceID, "pending:"+string(runID))
	return ProviderThread{
		ID: threadID, Driver: driver, ProviderInstanceID: instanceID, AppThreadID: &projection.Thread.ID,
		Status: ProviderThreadNotLoaded, HandoffIDs: []ContextHandoffID{}, CreatedAt: nowTime, UpdatedAt: nowTime,
	}, true, nil
}

func runOrdinalValue(ordinal *int) int {
	if ordinal == nil {
		return 0
	}
	return *ordinal
}

func appendUniqueHandoff(ids []ContextHandoffID, id ContextHandoffID) []ContextHandoffID {
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}

func (o *Orchestrator) planProviderHandoff(plan *commandPlan, from, to ProviderThread, runID RunID) (*ContextHandoff, error) {
	latest, ok := latestRun(plan.projection.Runs)
	if !ok {
		return nil, nil
	}
	fromOrdinal := 1
	strategy := "full_thread_summary"
	if to.LastRunOrdinal != nil && *to.LastRunOrdinal < latest.Ordinal {
		fromOrdinal = *to.LastRunOrdinal + 1
		strategy = "delta_since_target_last_seen"
	}
	transferID, err := o.randomID("context-transfer", "type", "provider_handoff", "source-thread", plan.command.ThreadID, "target-thread", plan.command.ThreadID)
	if err != nil {
		return nil, err
	}
	now := plan.now
	transfer := ContextTransfer{
		ID: ContextTransferID(transferID), Type: "provider_handoff", SourceThreadID: plan.command.ThreadID,
		TargetThreadID: plan.command.ThreadID, SourcePoint: ContextSourcePoint{ThreadID: plan.command.ThreadID, RunID: &latest.ID},
		SourceProviderInstanceID: &from.ProviderInstanceID, TargetProviderInstanceID: &to.ProviderInstanceID,
		TargetRunID: &runID, Status: "consumed", CreatedBy: ActorSystem, CreatedAt: now, UpdatedAt: now, ConsumedAt: &now,
	}
	handoffID, err := o.randomID("context-handoff", "thread", plan.command.ThreadID,
		"from-provider-instance", from.ProviderInstanceID, "to-provider-instance", to.ProviderInstanceID)
	if err != nil {
		return nil, err
	}
	handoff := &ContextHandoff{
		ID: ContextHandoffID(handoffID), TransferID: &transfer.ID, ThreadID: plan.command.ThreadID,
		TargetRunID: runID, FromProviderThreadIDs: []ProviderThreadID{from.ID}, ToProviderThreadID: to.ID,
		Strategy: strategy, Status: "ready", SummaryText: handoffSummary(plan.projection, fromOrdinal, latest.Ordinal),
		CreatedAt: now, UpdatedAt: now,
	}
	handoff.CoveredRunOrdinals.From = fromOrdinal
	handoff.CoveredRunOrdinals.To = latest.Ordinal
	transfer.Resolution = &ContextTransferResolution{Strategy: map[string]string{
		"delta_since_target_last_seen": "delta_context", "full_thread_summary": "portable_context",
	}[strategy], ContextHandoffID: &handoff.ID}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventContextTransferCreated, threadID: plan.command.ThreadID,
		runID: &runID, instanceID: to.ProviderInstanceID, payload: transfer}); err != nil {
		return nil, err
	}
	return handoff, nil
}

func handoffSummary(projection Projection, fromOrdinal, toOrdinal int) string {
	runOrdinals := make(map[RunID]int)
	for _, run := range projection.Runs {
		runOrdinals[run.ID] = run.Ordinal
	}
	var text strings.Builder
	fmt.Fprintf(&text, "Portable orchestration context for runs %d-%d.\n", fromOrdinal, toOrdinal)
	for _, message := range projection.Messages {
		if message.RunID == nil {
			continue
		}
		ordinal := runOrdinals[*message.RunID]
		if ordinal < fromOrdinal || ordinal > toOrdinal || message.Streaming {
			continue
		}
		fmt.Fprintf(&text, "\n[%s, run %d]\n%s\n", message.Role, ordinal, message.Text)
	}
	return text.String()
}

func (o *Orchestrator) resolvePendingFork(ctx context.Context, plan *commandPlan, provider ProviderInfo, providerThread *ProviderThread, runID RunID, handoff **ContextHandoff) error {
	var pending *ContextTransfer
	for index := range plan.projection.ContextTransfers {
		transfer := &plan.projection.ContextTransfers[index]
		if transfer.Type == "fork" && transfer.Status == "pending" {
			copy := *transfer
			pending = &copy
			break
		}
	}
	if pending == nil {
		return nil
	}
	sourceSnapshot, err := o.store.Snapshot(ctx, pending.SourceThreadID)
	if err != nil {
		return fmt.Errorf("load fork source: %w", err)
	}
	var sourceRun Run
	if pending.SourcePoint.RunID != nil {
		var ok bool
		sourceRun, ok = sourceSnapshot.Projection.Run(*pending.SourcePoint.RunID)
		if !ok {
			return fmt.Errorf("fork source run disappeared: %w", ErrStableSourceNotFound)
		}
	}
	var sourceProvider ProviderThread
	if sourceRun.ProviderThreadID != nil {
		sourceProvider, _ = sourceSnapshot.Projection.ProviderThread(*sourceRun.ProviderThreadID)
	}
	native := pending.SourceProviderInstanceID != nil && *pending.SourceProviderInstanceID == providerThread.ProviderInstanceID &&
		sourceProvider.NativeThreadRef != nil && sourceProvider.NativeThreadRef.Strength == NativeRefStrong && provider.Capabilities.Threads.CanForkThread
	if native {
		providerThread.ForkedFrom = &ProviderThreadForkSource{ProviderThreadID: sourceProvider.ID}
		if sourceRun.ActiveAttemptID != nil {
			if turn, ok := LatestProviderTurnForAttempt(sourceSnapshot.Projection.ProviderTurns, *sourceRun.ActiveAttemptID); ok {
				providerThread.ForkedFrom.ProviderTurnID = &turn.ID
			}
		}
		pending.Resolution = &ContextTransferResolution{Strategy: "native_fork", ProviderThreadRef: sourceProvider.NativeThreadRef}
	} else {
		handoffID, err := o.randomID("context-handoff", "thread", plan.command.ThreadID,
			"from-provider-instance", providerThread.ProviderInstanceID, "to-provider-instance", providerThread.ProviderInstanceID)
		if err != nil {
			return err
		}
		toOrdinal := 0
		if sourceRun.ID != "" {
			toOrdinal = sourceRun.Ordinal
		}
		created := &ContextHandoff{
			ID: ContextHandoffID(handoffID), TransferID: &pending.ID, ThreadID: plan.command.ThreadID,
			TargetRunID: runID, ToProviderThreadID: providerThread.ID, Strategy: "fork_delta_summary",
			Status: "ready", SummaryText: handoffSummary(sourceSnapshot.Projection, 1, toOrdinal), CreatedAt: plan.now, UpdatedAt: plan.now,
		}
		if sourceProvider.ID != "" {
			created.FromProviderThreadIDs = []ProviderThreadID{sourceProvider.ID}
		}
		created.CoveredRunOrdinals.From = 1
		created.CoveredRunOrdinals.To = max(1, toOrdinal)
		createdByProvider := sourceRun.ProviderInstanceID
		if createdByProvider != "" {
			created.CreatedByProviderInstanceID = &createdByProvider
		}
		*handoff = created
		pending.Resolution = &ContextTransferResolution{Strategy: "portable_context", ContextHandoffID: &created.ID}
	}
	pending.TargetProviderInstanceID = &providerThread.ProviderInstanceID
	pending.TargetRunID = &runID
	pending.Status = "consumed"
	pending.UpdatedAt = plan.now
	pending.ConsumedAt = &plan.now
	_, err = plan.emitAndApply(eventSpec{eventType: EventContextTransferUpdated, threadID: plan.command.ThreadID,
		runID: &runID, instanceID: providerThread.ProviderInstanceID, payload: *pending})
	return err
}
