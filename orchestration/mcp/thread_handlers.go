package mcp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

func (s *Server) capabilities(input CapabilitiesInput) (any, *Failure) {
	providers := make([]map[string]any, 0, len(s.config.Providers))
	search := strings.ToLower(strings.TrimSpace(input.Search))
	for _, provider := range s.config.Providers {
		models := provider.Models
		if search != "" {
			models = nil
			for _, model := range provider.Models {
				if strings.Contains(strings.ToLower(model), search) {
					models = append(models, model)
				}
			}
		} else if len(models) > 8 {
			models = models[:8]
		}
		providers = append(providers, map[string]any{
			"instanceId": provider.InstanceID, "driverKind": provider.Driver,
			"models": models, "available": provider.Available,
			"canDelegate": provider.Available, "runtimeModes": provider.RuntimeModes,
			"omittedModels": max(0, len(provider.Models)-len(models)),
		})
	}
	return map[string]any{"providers": providers, "features": map[string]any{
		"polling": true, "cancellation": true, "batchThreadCreation": true,
	}}, nil
}

func (s *Server) createThreads(ctx context.Context, input CreateThreadsInput) (any, *Failure) {
	if len(input.Threads) < 1 || len(input.Threads) > 20 {
		return nil, invalidRequest("threads must contain between 1 and 20 entries")
	}
	requestKey := requestKeyOrRandom(input.ClientRequestID)
	results := make([]map[string]any, 0, len(input.Threads))
	for index, entry := range input.Threads {
		target, runtimeMode, interactionMode, failure := s.resolveTarget(entry.Target, entry.RuntimeMode, entry.InteractionMode)
		if failure != nil {
			return nil, failure
		}
		projectID, worktree, failure := s.defaultWorkspace()
		if failure != nil {
			return nil, failure
		}
		threadID := orchestration.ThreadID(scopedID("thread:mcp", requestKey, "thread", index))
		commandID := orchestration.CommandID(scopedID("command:mcp", requestKey, "create", index))
		title := entry.Title
		if title == "" {
			title = "MCP thread"
			if entry.Prompt != "" {
				title = truncate(entry.Prompt, 80)
			}
		}
		_, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandThreadCreate,
			CommandID: commandID, ThreadID: threadID, Payload: orchestration.CreateThread{
				ProjectID: projectID, Title: title, ModelSelection: target, RuntimeMode: runtimeMode,
				InteractionMode: interactionMode, WorktreePath: worktree, CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationMCP,
			}})
		if err != nil {
			return nil, failureFromError(err)
		}
		if entry.Prompt != "" {
			messageID := orchestration.MessageID(scopedID("message:mcp", requestKey, "message", index))
			result, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandMessageDispatch,
				CommandID: orchestration.CommandID(scopedID("command:mcp", requestKey, "send", index)), ThreadID: threadID,
				Payload: orchestration.DispatchMessage{MessageID: messageID, Text: entry.Prompt, Attachments: []orchestration.Attachment{},
					DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately},
					CreatedBy:    orchestration.ActorAgent, CreationSource: orchestration.CreationMCP}})
			if err != nil {
				return nil, failureFromError(err)
			}
			s.runEffects(result.Effects)
		}
		snapshot, err := s.orchestrator.Snapshot(ctx, threadID)
		if err != nil {
			return nil, failureFromError(err)
		}
		var runID *orchestration.RunID
		status := "idle"
		if run, ok := latestRun(snapshot.Projection.Runs); ok {
			runID, status = &run.ID, string(run.Status)
		}
		results = append(results, map[string]any{"threadId": threadID, "runId": runID, "status": status,
			"title": snapshot.Projection.Thread.Title, "createdBy": orchestration.ActorAgent, "creationSource": orchestration.CreationMCP,
			"providerInstanceId": target.InstanceID, "model": target.Model})
	}
	return map[string]any{"threads": results}, nil
}

func (s *Server) threadList(input ThreadListInput) (any, *Failure) {
	snapshots, err := s.orchestrator.List(context.Background())
	if err != nil {
		return nil, failureFromError(err)
	}
	projectID := orchestration.ProjectID(input.ProjectID)
	if projectID == "" && !input.AllProjects {
		projectID = s.config.DefaultProjectID
		if s.config.ParentThreadID != "" {
			if snapshot, err := s.orchestrator.Snapshot(context.Background(), s.config.ParentThreadID); err == nil {
				projectID = snapshot.Projection.Thread.ProjectID
			}
		}
	}
	threads := make([]map[string]any, 0)
	for _, snapshot := range snapshots {
		thread := snapshot.Projection.Thread
		if thread.DeletedAt != nil || (!input.IncludeSubagents && thread.Lineage.RelationshipToParent != nil && *thread.Lineage.RelationshipToParent == "subagent") ||
			(!input.AllProjects && projectID != "" && thread.ProjectID != projectID) ||
			(input.TitleContains != "" && !strings.Contains(strings.ToLower(thread.Title), strings.ToLower(input.TitleContains))) {
			continue
		}
		latest, ok := latestRun(snapshot.Projection.Runs)
		status := "idle"
		var latestRunID *orchestration.RunID
		if ok {
			status, latestRunID = string(latest.Status), &latest.ID
		}
		if len(input.Statuses) > 0 && !containsString(input.Statuses, status) {
			continue
		}
		threads = append(threads, map[string]any{
			"threadId": thread.ID, "projectId": thread.ProjectID, "title": thread.Title,
			"createdBy": thread.CreatedBy, "creationSource": thread.CreationSource, "status": status,
			"latestRunId": latestRunID, "providerInstanceId": thread.ProviderInstanceID, "model": thread.ModelSelection.Model,
			"runtimeMode": thread.RuntimeMode, "interactionMode": thread.InteractionMode,
			"parentThreadId": thread.Lineage.ParentThreadID, "relationshipToParent": thread.Lineage.RelationshipToParent,
			"itemCount": len(snapshot.Projection.TurnItems), "createdAt": thread.CreatedAt, "updatedAt": thread.UpdatedAt,
		})
	}
	sort.Slice(threads, func(i, j int) bool {
		return threads[i]["updatedAt"].(time.Time).After(threads[j]["updatedAt"].(time.Time))
	})
	total := len(threads)
	cursor := max(0, input.Cursor)
	if cursor > total {
		cursor = total
	}
	limit := input.Limit
	if limit == 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	end := min(total, cursor+limit)
	var nextCursor *int
	if end < total {
		nextCursor = &end
	}
	var projectResult *orchestration.ProjectID
	if !input.AllProjects {
		projectResult = &projectID
	}
	return map[string]any{"projectId": projectResult, "currentThreadId": s.config.ParentThreadID,
		"threads": threads[cursor:end], "nextCursor": nextCursor, "total": total}, nil
}

func (s *Server) threadRead(input ThreadReadInput) (any, *Failure) {
	if input.ThreadID == "" {
		return nil, invalidRequest("threadId is required")
	}
	snapshot, err := s.orchestrator.Snapshot(context.Background(), orchestration.ThreadID(input.ThreadID))
	if err != nil {
		return nil, failureFromError(err)
	}
	projection := snapshot.Projection
	thread := projection.Thread
	latest, hasLatest := latestRun(projection.Runs)
	status := "idle"
	var latestRunID *orchestration.RunID
	if hasLatest {
		status, latestRunID = string(latest.Status), &latest.ID
	}
	var activeRunID *orchestration.RunID
	if active, ok := projection.ActiveRun(); ok {
		activeRunID = &active.ID
	}
	pending := 0
	for _, request := range projection.RuntimeRequests {
		if request.Status == orchestration.RequestPending {
			pending++
		}
	}
	runLimit := input.RunLimit
	if runLimit == 0 {
		runLimit = 10
	}
	runLimit = min(runLimit, 50)
	runs := projection.Runs
	if len(runs) > runLimit {
		runs = runs[len(runs)-runLimit:]
	}
	recentRuns := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		recentRuns = append(recentRuns, map[string]any{"runId": run.ID, "ordinal": run.Ordinal, "status": run.Status,
			"providerInstanceId": run.ProviderInstanceID, "model": run.ModelSelection.Model,
			"requestedAt": run.RequestedAt, "startedAt": run.StartedAt, "completedAt": run.CompletedAt})
	}
	items := projection.TurnItems
	if input.ItemID != "" {
		items = nil
		for _, item := range projection.TurnItems {
			if item.ID == orchestration.TurnItemID(input.ItemID) {
				items = append(items, item)
			}
		}
	}
	filtered := make([]orchestration.TurnItem, 0)
	for _, item := range items {
		if input.View != "activity" && item.Type != "user_message" && item.Type != "assistant_message" && item.Type != "proposed_plan" {
			continue
		}
		if input.ItemID == "" && item.Ordinal <= input.AfterPosition {
			continue
		}
		filtered = append(filtered, item)
	}
	limit := input.Limit
	if limit == 0 {
		limit = 50
	}
	limit = min(limit, 100)
	hasMore := len(filtered) > limit
	if hasMore {
		filtered = filtered[:limit]
	}
	maxChars := input.MaxCharsPerItem
	if maxChars == 0 {
		maxChars = 50_000
	}
	maxChars = min(maxChars, 50_000)
	resultItems := make([]map[string]any, 0, len(filtered))
	for _, item := range filtered {
		text := item.Text
		offset := 0
		if input.ItemID != "" {
			offset = input.TextOffset
		}
		runes := []rune(text)
		if offset > len(runes) {
			offset = len(runes)
		}
		runes = runes[offset:]
		truncated := len(runes) > maxChars
		if truncated {
			runes = runes[:maxChars]
		}
		var nextOffset *int
		if truncated {
			next := offset + maxChars
			nextOffset = &next
		}
		var title *string
		if item.Title != "" {
			title = &item.Title
		}
		var textResult *string
		if text != "" {
			value := string(runes)
			textResult = &value
		}
		resultItems = append(resultItems, map[string]any{"position": item.Ordinal, "visibility": "local",
			"sourceThreadId": thread.ID, "itemId": item.ID, "runId": item.RunID, "messageId": nullableMessageID(item.MessageID),
			"createdBy": nullableActor(item.CreatedBy), "creationSource": nullableCreationSource(item.CreationSource),
			"type": item.Type, "status": item.Status, "title": title, "text": textResult,
			"textTruncated": truncated, "nextTextOffset": nextOffset, "updatedAt": item.UpdatedAt})
	}
	var nextPosition *int
	if len(filtered) > 0 {
		next := filtered[len(filtered)-1].Ordinal
		nextPosition = &next
	}
	return map[string]any{
		"thread": map[string]any{"threadId": thread.ID, "projectId": thread.ProjectID, "title": thread.Title,
			"createdBy": thread.CreatedBy, "creationSource": thread.CreationSource, "status": status,
			"latestRunId": latestRunID, "activeRunId": activeRunID, "providerInstanceId": thread.ProviderInstanceID,
			"model": thread.ModelSelection.Model, "runtimeMode": thread.RuntimeMode, "interactionMode": thread.InteractionMode,
			"branch": thread.Branch, "worktreePath": thread.WorktreePath, "parentThreadId": thread.Lineage.ParentThreadID,
			"relationshipToParent": thread.Lineage.RelationshipToParent, "runCount": len(projection.Runs),
			"itemCount": len(projection.TurnItems), "pendingRequestCount": pending, "archived": thread.ArchivedAt != nil,
			"createdAt": thread.CreatedAt, "updatedAt": thread.UpdatedAt},
		"recentRuns": recentRuns, "items": resultItems, "nextPosition": nextPosition, "hasMore": hasMore,
	}, nil
}

func (s *Server) threadSend(ctx context.Context, input ThreadSendInput) (any, *Failure) {
	if input.ThreadID == "" || strings.TrimSpace(input.Message) == "" {
		return nil, invalidRequest("threadId and message are required")
	}
	key := requestKeyOrRandom(input.ClientRequestID)
	threadID := orchestration.ThreadID(input.ThreadID)
	snapshot, err := s.orchestrator.Snapshot(ctx, threadID)
	if err != nil {
		return nil, failureFromError(err)
	}
	mode := orchestration.DispatchStartImmediately
	delivery := "started"
	_, active := snapshot.Projection.ActiveRun()
	switch input.Mode {
	case "", "auto":
		if active {
			mode, delivery = orchestration.DispatchQueueAfterActive, "queued"
		}
	case "queue":
		mode, delivery = orchestration.DispatchQueueAfterActive, "queued"
	case "steer":
		mode, delivery = orchestration.DispatchSteerActive, "steered"
	case "restart":
		mode, delivery = orchestration.DispatchRestartActive, "restarted"
	default:
		return nil, invalidRequest("unknown send mode")
	}
	if active && (mode == orchestration.DispatchSteerActive || mode == orchestration.DispatchRestartActive) {
		run, _ := snapshot.Projection.ActiveRun()
		result, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandMessageDispatch,
			CommandID: orchestration.CommandID(scopedID("command:mcp", key, "send")), ThreadID: threadID,
			Payload: orchestration.DispatchMessage{MessageID: orchestration.MessageID(scopedID("message:mcp", key, "message")),
				Text: input.Message, Attachments: []orchestration.Attachment{}, DispatchMode: orchestration.DispatchMode{Type: mode, TargetRunID: run.ID},
				CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationMCP}})
		if err != nil {
			return nil, failureFromError(err)
		}
		s.runEffects(result.Effects)
		return map[string]any{"threadId": threadID, "messageId": scopedID("message:mcp", key, "message"), "runId": run.ID,
			"status": run.Status, "delivery": delivery}, nil
	}
	result, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandMessageDispatch,
		CommandID: orchestration.CommandID(scopedID("command:mcp", key, "send")), ThreadID: threadID,
		Payload: orchestration.DispatchMessage{MessageID: orchestration.MessageID(scopedID("message:mcp", key, "message")),
			Text: input.Message, Attachments: []orchestration.Attachment{}, DispatchMode: orchestration.DispatchMode{Type: mode},
			CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationMCP}})
	if err != nil {
		return nil, failureFromError(err)
	}
	s.runEffects(result.Effects)
	updated, _ := s.orchestrator.Snapshot(ctx, threadID)
	run, _ := latestRun(updated.Projection.Runs)
	return map[string]any{"threadId": threadID, "messageId": scopedID("message:mcp", key, "message"),
		"runId": run.ID, "status": run.Status, "delivery": delivery}, nil
}

func (s *Server) threadWait(ctx context.Context, input ThreadWaitInput) (any, *Failure) {
	snapshot, err := s.orchestrator.Snapshot(ctx, orchestration.ThreadID(input.ThreadID))
	if err != nil {
		return nil, failureFromError(err)
	}
	var runID *orchestration.RunID
	if input.RunID != "" {
		id := orchestration.RunID(input.RunID)
		runID = &id
	} else if run, ok := latestRun(snapshot.Projection.Runs); ok {
		runID = &run.ID
	}
	if runID == nil {
		return map[string]any{"threadId": input.ThreadID, "runId": nil, "status": "idle", "timedOut": false}, nil
	}
	timeout := time.Duration(input.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := s.orchestrator.Snapshot(ctx, orchestration.ThreadID(input.ThreadID))
		if err != nil {
			return nil, failureFromError(err)
		}
		run, ok := snapshot.Projection.Run(*runID)
		if !ok {
			return nil, &Failure{Tag: "OrchestratorMcpFailure", Code: "run_not_found", Message: "run not found: " + string(*runID)}
		}
		if run.Status.Terminal() {
			return map[string]any{"threadId": input.ThreadID, "runId": run.ID, "status": run.Status, "timedOut": false}, nil
		}
		select {
		case <-ctx.Done():
			return nil, failureFromError(ctx.Err())
		case <-deadline.C:
			return map[string]any{"threadId": input.ThreadID, "runId": run.ID, "status": run.Status, "timedOut": true}, nil
		case <-ticker.C:
		}
	}
}

func (s *Server) threadInterrupt(ctx context.Context, input ThreadInterruptInput) (any, *Failure) {
	threadID := orchestration.ThreadID(input.ThreadID)
	snapshot, err := s.orchestrator.Snapshot(ctx, threadID)
	if err != nil {
		return nil, failureFromError(err)
	}
	var run orchestration.Run
	if input.RunID != "" && !input.Stop {
		run, _ = snapshot.Projection.Run(orchestration.RunID(input.RunID))
	} else if active, ok := snapshot.Projection.ActiveRun(); ok {
		run = active
	}
	if run.ID == "" {
		return map[string]any{"threadId": threadID, "runId": nil, "status": "no_active_run"}, nil
	}
	if run.Status.Terminal() {
		return map[string]any{"threadId": threadID, "runId": run.ID, "status": run.Status}, nil
	}
	key := requestKeyOrRandom(input.ClientRequestID)
	result, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandRunInterrupt,
		CommandID: orchestration.CommandID(scopedID("command:mcp", key, "interrupt")), ThreadID: threadID,
		Payload: orchestration.InterruptRun{RunID: run.ID, Reason: input.Reason, HoldQueue: input.Stop}})
	if err != nil {
		return nil, failureFromError(err)
	}
	s.runEffects(result.Effects)
	return map[string]any{"threadId": threadID, "runId": run.ID, "status": "interrupt_requested"}, nil
}

func (s *Server) resolveTarget(target *Target, runtimeValue, interactionValue string, parentOverrides ...orchestration.ThreadID) (orchestration.ModelSelection, orchestration.RuntimeMode, orchestration.InteractionMode, *Failure) {
	parentID := s.config.ParentThreadID
	if len(parentOverrides) > 0 {
		parentID = parentOverrides[0]
	}
	model := s.config.DefaultTarget
	runtimeMode := orchestration.RuntimeFullAccess
	interactionMode := orchestration.InteractionDefault
	if parentID != "" {
		if snapshot, err := s.orchestrator.Snapshot(context.Background(), parentID); err == nil {
			model = snapshot.Projection.Thread.ModelSelection
			runtimeMode = snapshot.Projection.Thread.RuntimeMode
			interactionMode = snapshot.Projection.Thread.InteractionMode
		}
	}
	if target != nil {
		if target.ProviderInstanceID != "" {
			model.InstanceID = orchestration.ProviderInstanceID(target.ProviderInstanceID)
		} else if target.DriverKind != "" {
			for _, provider := range s.config.Providers {
				if provider.Driver == orchestration.ProviderDriverKind(target.DriverKind) && provider.Available {
					model.InstanceID = provider.InstanceID
					break
				}
			}
		}
		if target.Model != "" {
			model.Model = target.Model
		}
		if target.Options != nil {
			model.Options = target.Options
		}
	}
	if model.InstanceID == "" || model.Model == "" {
		return model, runtimeMode, interactionMode, &Failure{Tag: "OrchestratorMcpFailure", Code: "provider_unavailable", Message: "no default provider/model configured"}
	}
	if len(s.config.Providers) > 0 {
		var provider *Provider
		for index := range s.config.Providers {
			if s.config.Providers[index].InstanceID == model.InstanceID {
				provider = &s.config.Providers[index]
			}
		}
		if provider == nil || !provider.Available {
			return model, runtimeMode, interactionMode, &Failure{Tag: "OrchestratorMcpFailure", Code: "provider_unavailable", Message: "provider is unavailable: " + string(model.InstanceID)}
		}
	}
	if runtimeValue != "" && runtimeValue != "inherit" {
		runtimeMode = orchestration.RuntimeMode(runtimeValue)
	}
	if interactionValue != "" && interactionValue != "inherit" {
		interactionMode = orchestration.InteractionMode(interactionValue)
	}
	if parentID != "" {
		if snapshot, err := s.orchestrator.Snapshot(context.Background(), parentID); err == nil {
			if orchestration.RuntimeModeAbove(runtimeMode, snapshot.Projection.Thread.RuntimeMode) {
				return model, runtimeMode, interactionMode, &Failure{Tag: "OrchestratorMcpFailure", Code: "runtime_mode_escalation_denied", Message: "target runtime mode exceeds the caller"}
			}
			if snapshot.Projection.Thread.InteractionMode == orchestration.InteractionPlan && interactionMode == orchestration.InteractionDefault {
				return model, runtimeMode, interactionMode, &Failure{Tag: "OrchestratorMcpFailure", Code: "interaction_mode_escalation_denied", Message: "plan caller cannot launch default-mode work"}
			}
		}
	}
	return model, runtimeMode, interactionMode, nil
}

func (s *Server) defaultWorkspace() (orchestration.ProjectID, *string, *Failure) {
	if s.config.ParentThreadID != "" {
		if snapshot, err := s.orchestrator.Snapshot(context.Background(), s.config.ParentThreadID); err == nil {
			return snapshot.Projection.Thread.ProjectID, snapshot.Projection.Thread.WorktreePath, nil
		}
	}
	if s.config.DefaultProjectID == "" {
		return "", nil, invalidRequest("projectId is required outside a parent thread")
	}
	return s.config.DefaultProjectID, s.config.DefaultWorktreePath, nil
}

func latestRun(runs []orchestration.Run) (orchestration.Run, bool) {
	if len(runs) == 0 {
		return orchestration.Run{}, false
	}
	return runs[len(runs)-1], true
}

func scopedID(prefix, key string, parts ...any) string {
	hash := sha256.Sum256([]byte(strings.Join(append([]string{key}, toStrings(parts)...), ":")))
	return prefix + ":" + hex.EncodeToString(hash[:16])
}

func toStrings(values []any) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = fmt.Sprint(value)
	}
	return result
}

func requestKeyOrRandom(value string) string {
	if value != "" {
		return value
	}
	var bytes [16]byte
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}

func truncate(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit])
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func nullableMessageID(id orchestration.MessageID) *orchestration.MessageID {
	if id == "" {
		return nil
	}
	return &id
}

func nullableActor(actor orchestration.Actor) *orchestration.Actor {
	if actor == "" {
		return nil
	}
	return &actor
}

func nullableCreationSource(source orchestration.CreationSource) *orchestration.CreationSource {
	if source == "" {
		return nil
	}
	return &source
}
