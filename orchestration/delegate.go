package orchestration

import (
	"context"
	"fmt"
	"strings"
)

func (o *Orchestrator) planDelegatedTask(ctx context.Context, plan *commandPlan) error {
	command, _ := commandPayload[DelegateTask](plan.command)
	parentRun, ok := plan.projection.Run(command.ParentRunID)
	if !ok || !parentRun.Status.Active() {
		return fmt.Errorf("parent run %s is not active: %w", command.ParentRunID, ErrCommandRejected)
	}
	parentNode, ok := plan.projection.Node(command.ParentNodeID)
	if !ok || parentNode.RunID == nil || *parentNode.RunID != parentRun.ID || parentRun.RootNodeID == nil {
		return fmt.Errorf("parent node %s is not part of active run %s: %w", command.ParentNodeID, parentRun.ID, ErrCommandRejected)
	}
	provider, err := o.resolveProvider(ctx, command.ModelSelection.InstanceID)
	if err != nil {
		return err
	}
	actor, source := creationDefaults(command.CreatedBy, command.CreationSource)
	now := plan.now
	taskNodeID := DeriveDelegatedTaskNodeID(plan.command.CommandID)
	childThreadID := DeriveDelegatedTaskThreadID(plan.command.CommandID)
	childMessageID := DeriveDelegatedTaskMessageID(plan.command.CommandID)
	taskTurnItemID := DeriveDelegatedTaskTurnItemID(plan.command.CommandID)
	title := command.Title
	if title == "" {
		title = "Task: " + strings.TrimSpace(command.Task)
		if len(title) > 80 {
			title = title[:80]
		}
	}
	relationship := "subagent"
	childThread := Thread{
		CreatedBy: actor, CreationSource: source, ID: childThreadID, ProjectID: plan.projection.Thread.ProjectID,
		Title: title, ProviderInstanceID: command.ModelSelection.InstanceID, ModelSelection: command.ModelSelection,
		RuntimeMode: command.RuntimeMode, InteractionMode: command.InteractionMode,
		Branch: plan.projection.Thread.Branch, WorktreePath: plan.projection.Thread.WorktreePath,
		Lineage: ThreadLineage{ParentThreadID: &command.ParentThreadID, RelationshipToParent: &relationship,
			RootThreadID: plan.projection.Thread.Lineage.RootThreadID},
		ForkedFrom: &ForkedFrom{Type: "node", NodeID: taskNodeID}, CreatedAt: now, UpdatedAt: now,
	}
	threadDefaults(&childThread)
	childCreated, err := plan.emit(eventSpec{eventType: EventThreadCreated, threadID: childThreadID,
		driver: provider.Driver, instanceID: command.ModelSelection.InstanceID, payload: childThread})
	if err != nil {
		return err
	}
	taskNode := ExecutionNode{ID: taskNodeID, ThreadID: command.ParentThreadID, RunID: &parentRun.ID,
		ParentNodeID: &parentNode.ID, RootNodeID: *parentRun.RootNodeID, Kind: NodeSubagent,
		Status: NodeRunning, CountsForRun: false, StartedAt: &now}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventNodeUpdated, threadID: command.ParentThreadID,
		runID: &parentRun.ID, nodeID: &taskNodeID, driver: provider.Driver,
		instanceID: command.ModelSelection.InstanceID, payload: taskNode}); err != nil {
		return err
	}
	task := Subagent{ID: taskNodeID, ThreadID: command.ParentThreadID, RunID: &parentRun.ID,
		ParentNodeID: parentNode.ID, Origin: "app_owned", CreatedBy: actor, Driver: provider.Driver,
		ProviderInstanceID: command.ModelSelection.InstanceID, ChildThreadID: &childThreadID,
		Prompt: command.Task, Model: &command.ModelSelection.Model,
		Status: NodeRunning, StartedAt: &now, UpdatedAt: now}
	if command.Title != "" {
		task.Title = &command.Title
	}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventSubagentUpdated, threadID: command.ParentThreadID,
		runID: &parentRun.ID, nodeID: &taskNodeID, driver: provider.Driver,
		instanceID: command.ModelSelection.InstanceID, payload: task}); err != nil {
		return err
	}
	item := TurnItem{CreatedBy: actor, CreationSource: source,
		ID: taskTurnItemID, ThreadID: command.ParentThreadID, RunID: &parentRun.ID,
		NodeID: &taskNodeID, ProviderThreadID: parentRun.ProviderThreadID, Ordinal: nextTurnItemOrdinal(plan.projection),
		Status: TurnItemRunning, Title: title, StartedAt: &now, UpdatedAt: now, Type: "subagent",
		Data: map[string]any{"subagentId": taskNodeID, "origin": "app_owned", "driver": provider.Driver,
			"providerInstanceId": command.ModelSelection.InstanceID, "childThreadId": childThreadID, "prompt": command.Task}}
	if _, err := plan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: command.ParentThreadID,
		runID: &parentRun.ID, nodeID: &taskNodeID, driver: provider.Driver,
		instanceID: command.ModelSelection.InstanceID, payload: item}); err != nil {
		return err
	}

	childPlan := &commandPlan{orchestrator: o, now: now, projection: emptyProjection(childCreated, childThread), ctx: ctx,
		command: Command{Type: CommandMessageDispatch, CommandID: plan.command.CommandID, ThreadID: childThreadID,
			Payload: DispatchMessage{MessageID: childMessageID, Text: command.Task, Attachments: []Attachment{},
				ModelSelection: &command.ModelSelection, DispatchMode: DispatchMode{Type: DispatchStartImmediately},
				CreatedBy: actor, CreationSource: source}}}
	if err := o.planMessageDispatch(ctx, childPlan); err != nil {
		return err
	}
	plan.events = append(plan.events, childPlan.events...)
	plan.effects = append(plan.effects, childPlan.effects...)
	if plan.expectedSequences == nil {
		plan.expectedSequences = make(map[ThreadID]uint64)
	}
	plan.expectedSequences[childThreadID] = 0
	if len(childPlan.projection.Runs) != 1 {
		return fmt.Errorf("delegated child did not create a run: %w", ErrCommandRejected)
	}
	childRun := childPlan.projection.Runs[0]
	transferID, err := o.randomID("context-transfer", "type", "subagent_spawn",
		"source-thread", command.ParentThreadID, "target-thread", childThreadID)
	if err != nil {
		return err
	}
	transfer := ContextTransfer{ID: ContextTransferID(transferID), Type: "subagent_spawn",
		SourceThreadID: command.ParentThreadID, TargetThreadID: childThreadID,
		SourcePoint:              ContextSourcePoint{ThreadID: command.ParentThreadID, RunID: &parentRun.ID, TurnItemID: &taskTurnItemID},
		SourceProviderInstanceID: &parentRun.ProviderInstanceID, TargetProviderInstanceID: &command.ModelSelection.InstanceID,
		TargetRunID: &childRun.ID, Status: "consumed", CreatedBy: actor, CreatedAt: now, UpdatedAt: now, ConsumedAt: &now}
	_, err = plan.emit(eventSpec{eventType: EventContextTransferCreated, threadID: childThreadID,
		runID: &childRun.ID, driver: provider.Driver, instanceID: command.ModelSelection.InstanceID, payload: transfer})
	return err
}

func (o *Orchestrator) finalizeDelegatedTask(plan *commandPlan, childRun Run) error {
	thread := plan.projection.Thread
	if thread.Lineage.RelationshipToParent == nil || *thread.Lineage.RelationshipToParent != "subagent" ||
		thread.Lineage.ParentThreadID == nil || thread.ForkedFrom == nil || thread.ForkedFrom.Type != "node" {
		return nil
	}
	parentThreadID := *thread.Lineage.ParentThreadID
	parentSnapshot, err := o.store.Snapshot(plan.ctx, parentThreadID)
	if err != nil {
		return err
	}
	taskID := thread.ForkedFrom.NodeID
	task, found := findSubagent(parentSnapshot.Projection.Subagents, taskID)
	if !found || task.CompletedAt != nil {
		return nil
	}
	parentPlan := &commandPlan{orchestrator: o, command: plan.command, now: plan.now, ctx: plan.ctx,
		projection: parentSnapshot.Projection}
	status := NodeStatus(childRun.Status)
	summary := latestAssistantText(plan.projection.Messages, childRun.ID)
	task.Status = status
	task.Result = summary
	task.CompletedAt = &plan.now
	task.UpdatedAt = plan.now
	task.ProviderThreadID = childRun.ProviderThreadID
	var rawEventID *RawEventID
	if len(plan.events) > 0 {
		rawEventID = plan.events[len(plan.events)-1].RawEventID
	}
	if _, err := parentPlan.emitAndApply(eventSpec{eventType: EventSubagentUpdated, threadID: parentThreadID,
		runID: task.RunID, nodeID: &task.ID, driver: task.Driver, instanceID: task.ProviderInstanceID,
		rawEventID: rawEventID, payload: task}); err != nil {
		return err
	}
	if node, ok := parentPlan.projection.Node(taskID); ok {
		node.Status = status
		node.CompletedAt = &plan.now
		if _, err := parentPlan.emitAndApply(eventSpec{eventType: EventNodeUpdated, threadID: parentThreadID,
			runID: node.RunID, nodeID: &node.ID, driver: task.Driver, instanceID: task.ProviderInstanceID,
			rawEventID: rawEventID, payload: node}); err != nil {
			return err
		}
	}
	for _, item := range parentPlan.projection.TurnItems {
		if item.Type != "subagent" || item.NodeID == nil || *item.NodeID != taskID {
			continue
		}
		item.Status = TurnItemStatus(status)
		item.CompletedAt = &plan.now
		item.UpdatedAt = plan.now
		if item.Data == nil {
			item.Data = make(map[string]any)
		}
		item.Data["result"] = summary
		if _, err := parentPlan.emitAndApply(eventSpec{eventType: EventTurnItemUpdated, threadID: parentThreadID,
			runID: item.RunID, nodeID: item.NodeID, driver: task.Driver, instanceID: task.ProviderInstanceID,
			rawEventID: rawEventID, payload: item}); err != nil {
			return err
		}
	}
	transferID, err := o.randomID("context-transfer", "type", "subagent_result",
		"source-thread", thread.ID, "target-thread", parentThreadID)
	if err != nil {
		return err
	}
	targetProvider := parentSnapshot.Projection.Thread.ProviderInstanceID
	if parentRun, ok := parentSnapshot.Projection.Run(*task.RunID); ok {
		targetProvider = parentRun.ProviderInstanceID
	}
	transfer := ContextTransfer{ID: ContextTransferID(transferID), Type: "subagent_result",
		SourceThreadID: thread.ID, TargetThreadID: parentThreadID,
		SourcePoint:              ContextSourcePoint{ThreadID: thread.ID, RunID: &childRun.ID},
		SourceProviderInstanceID: &childRun.ProviderInstanceID, TargetProviderInstanceID: &targetProvider,
		TargetRunID: task.RunID, Status: "consumed", CreatedBy: ActorSystem,
		CreatedAt: plan.now, UpdatedAt: plan.now, ConsumedAt: &plan.now}
	if _, err := parentPlan.emitAndApply(eventSpec{eventType: EventContextTransferCreated, threadID: parentThreadID,
		runID: task.RunID, nodeID: &task.ID, driver: task.Driver, instanceID: task.ProviderInstanceID,
		rawEventID: rawEventID, payload: transfer}); err != nil {
		return err
	}
	plan.events = append(plan.events, parentPlan.events...)
	if plan.expectedSequences == nil {
		plan.expectedSequences = make(map[ThreadID]uint64)
	}
	plan.expectedSequences[parentThreadID] = parentSnapshot.SnapshotSequence
	return nil
}

func findSubagent(tasks []Subagent, id NodeID) (Subagent, bool) {
	for _, task := range tasks {
		if task.ID == id {
			return task, true
		}
	}
	return Subagent{}, false
}

func latestAssistantText(messages []ConversationMessage, runID RunID) *string {
	var result *string
	for _, message := range messages {
		if message.RunID != nil && *message.RunID == runID && message.Role == MessageAssistant {
			text := message.Text
			result = &text
		}
	}
	return result
}
