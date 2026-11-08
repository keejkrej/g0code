package orchestration

import (
	"context"
	"fmt"
)

func forkableStatus(status RunStatus) bool {
	switch status {
	case RunCompleted, RunWaiting, RunFailed, RunInterrupted, RunCancelled:
		return true
	default:
		return false
	}
}

func (o *Orchestrator) planThreadFork(ctx context.Context, plan *commandPlan) error {
	command, _ := commandPayload[ForkThread](plan.command)
	if plan.projection.Thread.ID != "" {
		return &Error{Op: "fork thread", CommandID: plan.command.CommandID,
			ThreadID: plan.command.ThreadID, Err: ErrThreadAlreadyExists}
	}
	sourceSnapshot, err := o.store.Snapshot(ctx, command.SourceThreadID)
	if err != nil {
		return fmt.Errorf("load fork source %s: %w", command.SourceThreadID, err)
	}
	source := sourceSnapshot.Projection
	if source.Thread.DeletedAt != nil {
		return fmt.Errorf("source thread is deleted: %w", ErrCommandRejected)
	}
	sourceRun, sourcePoint, err := resolveForkSource(source, command.SourcePoint)
	if err != nil {
		return err
	}
	if sourceRun.ID != "" && !forkableStatus(sourceRun.Status) {
		return fmt.Errorf("fork source run %s is %s; active and rolled-back runs cannot be forked: %w",
			sourceRun.ID, sourceRun.Status, ErrCommandRejected)
	}
	actor, creationSource := creationDefaults(command.CreatedBy, command.CreationSource)
	target := source.Thread
	target.CreatedBy = actor
	target.CreationSource = creationSource
	target.ID = plan.command.ThreadID
	target.Title = command.Title
	if target.Title == "" {
		target.Title = source.Thread.Title + " fork"
	}
	if command.SideChat {
		target.RuntimeMode = RuntimeApprovalRequired
	}
	target.ActiveProviderThreadID = nil
	relationship := "fork"
	target.Lineage = ThreadLineage{ParentThreadID: &source.Thread.ID, RelationshipToParent: &relationship,
		RootThreadID: source.Thread.Lineage.RootThreadID, SideChat: command.SideChat}
	target.ForkedFrom = nil
	if sourceRun.ID != "" {
		target.ForkedFrom = &ForkedFrom{Type: "run", ThreadID: source.Thread.ID, RunID: sourceRun.ID}
	}
	target.CreatedAt = plan.now
	target.UpdatedAt = plan.now
	target.ArchivedAt = nil
	target.SettledOverride = nil
	target.SettledAt = nil
	target.LastVisitedAt = nil
	target.DeletedAt = nil

	var sourceProvider ProviderThread
	if sourceRun.ProviderThreadID != nil {
		sourceProvider, _ = source.ProviderThread(*sourceRun.ProviderThreadID)
	}
	if sourceProvider.NativeThreadRef != nil {
		sourcePoint.ProviderThreadRef = sourceProvider.NativeThreadRef
		if sourceRun.ActiveAttemptID != nil {
			if turn, ok := LatestProviderTurnForAttempt(source.ProviderTurns, *sourceRun.ActiveAttemptID); ok {
				sourcePoint.ProviderTurnRef = turn.NativeTurnRef
			}
		}
	}
	transferID, err := o.randomID("context-transfer", "type", "fork", "source-thread", source.Thread.ID, "target-thread", target.ID)
	if err != nil {
		return err
	}
	providerInstance := source.Thread.ProviderInstanceID
	if sourceRun.ProviderInstanceID != "" {
		providerInstance = sourceRun.ProviderInstanceID
	}
	transfer := ContextTransfer{
		ID: ContextTransferID(transferID), Type: "fork", SourceThreadID: source.Thread.ID,
		TargetThreadID: target.ID, SourcePoint: sourcePoint, SourceProviderInstanceID: &providerInstance,
		Status: "pending", CreatedBy: actor, CreatedAt: plan.now, UpdatedAt: plan.now,
	}
	if sourceProvider.ID != "" && (sourceProvider.NativeThreadRef == nil || sourceProvider.NativeThreadRef.Strength != NativeRefStrong) {
		detail := "Source provider thread does not expose a strong native thread ref."
		transfer.Error = &detail
	}
	if _, err := plan.emit(eventSpec{eventType: EventThreadCreated, threadID: target.ID,
		instanceID: target.ProviderInstanceID, payload: target}); err != nil {
		return err
	}
	_, err = plan.emit(eventSpec{eventType: EventContextTransferCreated, threadID: target.ID,
		instanceID: providerInstance, payload: transfer})
	return err
}

func resolveForkSource(source Projection, point ForkSourcePoint) (Run, ContextSourcePoint, error) {
	var selected Run
	switch point.Type {
	case "latest_stable":
		for _, run := range source.Runs {
			if forkableStatus(run.Status) && run.Ordinal > selected.Ordinal {
				selected = run
			}
		}
	case "run":
		var ok bool
		selected, ok = source.Run(point.RunID)
		if !ok {
			return Run{}, ContextSourcePoint{}, fmt.Errorf("run %s: %w", point.RunID, ErrStableSourceNotFound)
		}
	case "checkpoint":
		found := false
		for _, checkpoint := range source.Checkpoints {
			if checkpoint.ID == point.CheckpointID && checkpoint.RunID != nil && checkpoint.Status == "ready" {
				selected, found = source.Run(*checkpoint.RunID)
				break
			}
		}
		if !found {
			return Run{}, ContextSourcePoint{}, fmt.Errorf("checkpoint %s: %w", point.CheckpointID, ErrStableSourceNotFound)
		}
	}
	if selected.ID == "" {
		return Run{}, ContextSourcePoint{}, ErrStableSourceNotFound
	}
	sourcePoint := ContextSourcePoint{ThreadID: source.Thread.ID, RunID: &selected.ID}
	if selected.CheckpointID != nil {
		sourcePoint.CheckpointID = selected.CheckpointID
	}
	if point.CheckpointID != "" {
		sourcePoint.CheckpointID = &point.CheckpointID
	}
	return selected, sourcePoint, nil
}
