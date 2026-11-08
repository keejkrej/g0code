package orchestration

import (
	"fmt"
	"sort"
)

func queuedRunsInDeliveryOrder(projection Projection) []Run {
	runs := make([]Run, 0)
	for _, run := range projection.Runs {
		if run.Status == RunQueued {
			runs = append(runs, run)
		}
	}
	sort.SliceStable(runs, func(i, j int) bool {
		left, right := runs[i], runs[j]
		leftPosition, rightPosition := left.Ordinal, right.Ordinal
		if left.QueuePosition != nil {
			leftPosition = *left.QueuePosition
		}
		if right.QueuePosition != nil {
			rightPosition = *right.QueuePosition
		}
		if leftPosition != rightPosition {
			return leftPosition < rightPosition
		}
		return left.Ordinal < right.Ordinal
	})
	return runs
}

func latestQueueAnchor(projection Projection) (Run, bool) {
	var latest Run
	found := false
	for _, run := range projection.Runs {
		if run.Status != RunQueued && (!found || run.Ordinal > latest.Ordinal) {
			latest, found = run, true
		}
	}
	return latest, found
}

func queueIsHeld(projection Projection) bool {
	for _, run := range projection.Runs {
		if run.Status == RunQueued && run.QueueHeld {
			return true
		}
	}
	latest, ok := latestQueueAnchor(projection)
	if !ok {
		return false
	}
	for _, item := range projection.TurnItems {
		if item.Type == "run_interrupt_request" && item.RunID != nil && *item.RunID == latest.ID {
			held, _ := item.Data["holdQueue"].(bool)
			if held {
				return true
			}
		}
	}
	return false
}

func releaseQueueHolds(plan *commandPlan) (bool, error) {
	changed := false
	for _, run := range plan.projection.Runs {
		if run.Status == RunQueued && run.QueueHeld {
			run.QueueHeld = false
			if _, err := plan.emitAndApply(eventSpec{eventType: EventRunUpdated, threadID: run.ThreadID,
				runID: &run.ID, nodeID: run.RootNodeID, instanceID: run.ProviderInstanceID, payload: run}); err != nil {
				return false, err
			}
			changed = true
		}
	}
	latest, ok := latestQueueAnchor(plan.projection)
	if !ok {
		return changed, nil
	}
	for _, item := range plan.projection.TurnItems {
		if item.Type != "run_interrupt_request" || item.RunID == nil || *item.RunID != latest.ID {
			continue
		}
		held, _ := item.Data["holdQueue"].(bool)
		if !held {
			continue
		}
		item.Data = map[string]any{"holdQueue": false}
		item.UpdatedAt = plan.now
		if _, err := plan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: item.ThreadID,
			runID: item.RunID, nodeID: item.NodeID, instanceID: latest.ProviderInstanceID, payload: item}); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}

func (o *Orchestrator) planQueueResume(plan *commandPlan) error {
	changed, err := releaseQueueHolds(plan)
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("queue is not held: %w", ErrCommandRejected)
	}
	return o.startNextQueuedRun(plan)
}

func (o *Orchestrator) planQueuedRunCancel(plan *commandPlan) error {
	command, _ := commandPayload[CancelQueuedRun](plan.command)
	run, ok := plan.projection.Run(command.RunID)
	if !ok || run.Status != RunQueued {
		return fmt.Errorf("run %s is not queued: %w", command.RunID, ErrCommandRejected)
	}
	run.Status = RunCancelled
	run.CompletedAt = &plan.now
	run.QueueHeld = false
	if _, err := plan.emitAndApply(eventSpec{eventType: EventRunUpdated, threadID: run.ThreadID,
		runID: &run.ID, nodeID: run.RootNodeID, instanceID: run.ProviderInstanceID, payload: run}); err != nil {
		return err
	}
	for _, item := range plan.projection.TurnItems {
		if item.RunID != nil && *item.RunID == run.ID && item.Type == "user_message" {
			item.Status = TurnItemCancelled
			item.CompletedAt = &plan.now
			item.UpdatedAt = plan.now
			if _, err := plan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: run.ThreadID,
				runID: &run.ID, nodeID: item.NodeID, instanceID: run.ProviderInstanceID, payload: item}); err != nil {
				return err
			}
		}
	}
	return o.startNextQueuedRun(plan)
}

func (o *Orchestrator) startNextQueuedRun(plan *commandPlan) error {
	if _, active := plan.projection.ActiveRun(); active {
		return nil
	}
	for _, queued := range queuedRunsInDeliveryOrder(plan.projection) {
		if queued.QueueHeld {
			continue
		}
		if queued.ProviderThreadID != nil {
			providerThread, ok := plan.projection.ProviderThread(*queued.ProviderThreadID)
			if !ok {
				return fmt.Errorf("queued run %s provider thread is missing: %w", queued.ID, ErrCommandRejected)
			}
			if providerThread.FirstRunOrdinal == nil {
				providerThread.FirstRunOrdinal = &queued.Ordinal
			}
			providerThread.LastRunOrdinal = &queued.Ordinal
			providerThread.UpdatedAt = plan.now
			if _, err := plan.emitAndApply(eventSpec{eventType: EventProviderThreadUpdated, threadID: queued.ThreadID,
				runID: &queued.ID, driver: providerThread.Driver, instanceID: providerThread.ProviderInstanceID, payload: providerThread}); err != nil {
				return err
			}
		}
		queued.Status = RunStarting
		queued.QueuePosition = nil
		if _, err := plan.emitAndApply(eventSpec{eventType: EventRunUpdated, threadID: queued.ThreadID,
			runID: &queued.ID, nodeID: queued.RootNodeID, instanceID: queued.ProviderInstanceID, payload: queued}); err != nil {
			return err
		}
		for _, item := range plan.projection.TurnItems {
			if item.Type != "user_message" || item.RunID == nil || *item.RunID != queued.ID {
				continue
			}
			item.Status = TurnItemCompleted
			item.StartedAt = &plan.now
			item.CompletedAt = &plan.now
			item.UpdatedAt = plan.now
			if item.Data == nil {
				item.Data = make(map[string]any)
			}
			if _, err := plan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: item.ThreadID,
				runID: item.RunID, nodeID: item.NodeID, instanceID: queued.ProviderInstanceID, payload: item}); err != nil {
				return err
			}
		}
		return o.appendTurnStartEffect(plan, queued)
	}
	return nil
}

func turnStartEffectID(commandID CommandID, runID RunID) string {
	if commandID == "" {
		return fmt.Sprintf("effect:provider-turn.start:%s", runID)
	}
	return fmt.Sprintf("effect:%s:provider-turn.start:%s", commandID, runID)
}

func (o *Orchestrator) appendTurnStartEffect(plan *commandPlan, run Run) error {
	if run.ActiveAttemptID == nil || run.ProviderThreadID == nil {
		return fmt.Errorf("run %s lacks attempt or provider thread: %w", run.ID, ErrCommandRejected)
	}
	var attempt RunAttempt
	found := false
	for _, candidate := range plan.projection.Attempts {
		if candidate.ID == *run.ActiveAttemptID {
			attempt, found = candidate, true
			break
		}
	}
	providerThread, threadFound := plan.projection.ProviderThread(*run.ProviderThreadID)
	var message ConversationMessage
	messageFound := false
	for _, candidate := range plan.projection.Messages {
		if candidate.ID == run.UserMessageID {
			message, messageFound = candidate, true
			break
		}
	}
	if !found || !threadFound || !messageFound {
		return fmt.Errorf("run %s has incomplete start state: %w", run.ID, ErrCommandRejected)
	}
	var handoff *ContextHandoff
	if run.ContextHandoffID != nil {
		for _, candidate := range plan.projection.ContextHandoffs {
			if candidate.ID == *run.ContextHandoffID {
				copy := candidate
				handoff = &copy
				break
			}
		}
	}
	plan.effect(EffectProviderTurnStart,
		turnStartEffectID(plan.command.CommandID, run.ID),
		TurnStartEffect{Thread: plan.projection.Thread, Run: run, Attempt: attempt,
			ProviderThread: providerThread, Message: message, ContextHandoff: handoff})
	return nil
}
