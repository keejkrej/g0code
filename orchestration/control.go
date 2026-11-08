package orchestration

import "fmt"

func (o *Orchestrator) planRunInterrupt(plan *commandPlan) error {
	command, _ := commandPayload[InterruptRun](plan.command)
	run, ok := plan.projection.Run(command.RunID)
	if !ok || !run.Status.Active() || run.RootNodeID == nil || run.ProviderThreadID == nil {
		return fmt.Errorf("run %s: %w", command.RunID, ErrRunNotInterruptible)
	}
	providerThread, ok := plan.projection.ProviderThread(*run.ProviderThreadID)
	if !ok {
		return fmt.Errorf("run %s provider thread is missing: %w", command.RunID, ErrRunNotInterruptible)
	}
	var providerTurn ProviderTurn
	if run.ActiveAttemptID != nil {
		providerTurn, _ = LatestProviderTurnForAttempt(plan.projection.ProviderTurns, *run.ActiveAttemptID)
	}
	item := TurnItem{CreatedBy: ActorSystem, CreationSource: CreationServer,
		ID: DeriveRunSignalTurnItemID(run.ID, "interrupt-request"), ThreadID: run.ThreadID,
		RunID: &run.ID, NodeID: run.RootNodeID, ProviderThreadID: &providerThread.ID,
		Ordinal: nextTurnItemOrdinal(plan.projection), Status: TurnItemCompleted,
		Title: "Interrupt requested", StartedAt: &plan.now, CompletedAt: &plan.now, UpdatedAt: plan.now,
		Type: "run_interrupt_request", Text: command.Reason,
		Data: map[string]any{"holdQueue": command.HoldQueue},
	}
	if providerTurn.ID != "" {
		item.ProviderTurnID = &providerTurn.ID
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: run.ThreadID,
		runID: &run.ID, nodeID: run.RootNodeID, instanceID: run.ProviderInstanceID, payload: item}); err != nil {
		return err
	}
	// Acknowledgement is not termination. The run changes only when the root
	// provider turn reports an interrupted terminal state.
	if providerTurn.ID != "" && providerThread.ProviderSessionID != nil {
		plan.effect(EffectProviderTurnInterrupt,
			fmt.Sprintf("effect:%s:provider-turn.interrupt:%s", plan.command.CommandID, providerTurn.ID),
			TurnInterruptEffect{ProviderSessionID: *providerThread.ProviderSessionID,
				ProviderThreadID: providerThread.ID, ProviderTurnID: providerTurn.ID, Reason: command.Reason})
	}
	if command.HoldQueue {
		for _, queued := range plan.projection.Runs {
			if queued.Status != RunQueued {
				continue
			}
			queued.QueueHeld = true
			if _, err := plan.emitAndApply(eventSpec{eventType: EventRunUpdated, threadID: queued.ThreadID,
				runID: &queued.ID, nodeID: queued.RootNodeID, instanceID: queued.ProviderInstanceID, payload: queued}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Orchestrator) planRuntimeRequestRespond(plan *commandPlan) error {
	command, _ := commandPayload[RuntimeRequestResponse](plan.command)
	request, ok := plan.projection.RuntimeRequest(command.RequestID)
	if !ok || request.Status != RequestPending || request.ResponseCapability.Type != "live" ||
		request.ResponseCapability.ProviderSessionID == nil {
		return fmt.Errorf("request %s: %w", command.RequestID, ErrRequestNotRespondable)
	}
	request.Status = RequestResolved
	request.ResolvedAt = &plan.now
	request.Decision = command.Decision
	request.Feedback = command.Feedback
	request.Answers = command.Answers
	if _, err := plan.emitAndApply(eventSpec{eventType: EventRuntimeRequestUpdated, threadID: plan.command.ThreadID,
		nodeID: &request.NodeID, payload: request}); err != nil {
		return err
	}
	if node, ok := plan.projection.Node(request.NodeID); ok {
		node.Status = NodeCompleted
		node.CompletedAt = &plan.now
		if _, err := plan.emitAndApply(eventSpec{eventType: EventNodeUpdated, threadID: node.ThreadID,
			runID: node.RunID, nodeID: &node.ID, payload: node}); err != nil {
			return err
		}
	}
	plan.effect(EffectProviderRuntimeRequestRespond,
		fmt.Sprintf("effect:%s:provider-runtime-request.respond:%s", plan.command.CommandID, request.ID),
		RuntimeRequestRespondEffect{Request: request, Response: command})
	return nil
}
