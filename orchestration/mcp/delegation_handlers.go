package mcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

const runningTaskNext = "Completion wakes this thread, so end your turn. Do not wait on it or poll it."

func (s *Server) delegateTask(ctx context.Context, input DelegateTaskInput) (any, *Failure) {
	if strings.TrimSpace(input.Task) == "" {
		return nil, invalidRequest("task is required")
	}
	if input.Isolation == "worktree" {
		return nil, &Failure{Tag: "OrchestratorMcpFailure", Code: "invalid_request", Message: "worktree isolation requires the launch adapter"}
	}
	parentID := s.config.ParentThreadID
	if parentID == "" {
		parentID = orchestration.ThreadID(input.ParentThreadID)
	}
	if parentID == "" {
		return nil, &Failure{Tag: "OrchestratorMcpFailure", Code: "parent_not_active", Message: "delegate_task requires parentThreadId in standalone mode"}
	}
	parentSnapshot, err := s.orchestrator.Snapshot(ctx, parentID)
	if err != nil {
		return nil, failureFromError(err)
	}
	parentRun, ok := parentSnapshot.Projection.ActiveRun()
	if !ok || parentRun.RootNodeID == nil {
		return nil, &Failure{Tag: "OrchestratorMcpFailure", Code: "parent_not_active", Message: "the parent run is not active"}
	}
	target, runtimeMode, interactionMode, failure := s.resolveTarget(input.Target, input.RuntimeMode, input.InteractionMode, parentID)
	if failure != nil {
		return nil, failure
	}
	key := requestKeyOrRandom(input.ClientRequestID)
	commandID := orchestration.CommandID(scopedID("command:mcp", key, "delegate"))
	task := input.Task
	if role := roleInstruction(input.Role); role != "" {
		task = role + "\n\n" + task
	}
	result, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandDelegatedTaskRequest,
		CommandID: commandID, ThreadID: parentID, Payload: orchestration.DelegateTask{
			ParentThreadID: parentID, ParentRunID: parentRun.ID, ParentNodeID: *parentRun.RootNodeID,
			Task: task, Title: input.Title, ModelSelection: target, RuntimeMode: runtimeMode,
			InteractionMode: interactionMode, CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationMCP,
		}})
	if err != nil {
		return nil, failureFromError(err)
	}
	s.runEffects(result.Effects)
	taskID := orchestration.DeriveDelegatedTaskNodeID(commandID)
	var waitTimedOut bool
	if input.Mode == "wait" {
		waitTimedOut, failure = s.waitForDelegatedTask(ctx, parentID, taskID, input.TimeoutMS)
		if failure != nil {
			return nil, failure
		}
	}
	status, failure := s.taskStatus(TaskInput{TaskID: string(taskID), ParentThreadID: string(parentID)}, input.Mode == "wait")
	if status != nil {
		status.(map[string]any)["waitTimedOut"] = waitTimedOut
	}
	return status, failure
}

func roleInstruction(role string) string {
	switch role {
	case "":
		return ""
	case "implementation":
		return "Role: implement the requested change and verify it."
	case "research":
		return "Role: research the question and report evidence and uncertainty."
	case "review":
		return "Role: review the work critically and prioritize concrete findings."
	case "design":
		return "Role: produce a coherent design with tradeoffs."
	case "test":
		return "Role: build and run focused verification."
	case "general":
		return "Role: complete the task using the available tools."
	default:
		return "Role: " + role
	}
}

func (s *Server) waitForDelegatedTask(ctx context.Context, parentID orchestration.ThreadID, taskID orchestration.NodeID, timeoutMS int) (bool, *Failure) {
	timeout := time.Duration(timeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, failure := s.taskStatus(TaskInput{TaskID: string(taskID), ParentThreadID: string(parentID)}, true)
		if failure != nil {
			return false, failure
		}
		if values, ok := result.(map[string]any); ok {
			if values["workState"] == "result_available" || values["hasPendingQuestion"] == true || values["pendingApproval"] != nil {
				return false, nil
			}
		}
		select {
		case <-ctx.Done():
			return false, failureFromError(ctx.Err())
		case <-deadline.C:
			return true, nil
		case <-ticker.C:
		}
	}
}

func (s *Server) taskStatus(input TaskInput, waiting bool) (any, *Failure) {
	parentID := s.config.ParentThreadID
	if parentID == "" {
		parentID = orchestration.ThreadID(input.ParentThreadID)
	}
	parentSnapshot, err := s.orchestrator.Snapshot(context.Background(), parentID)
	if err != nil {
		return nil, failureFromError(err)
	}
	var task *orchestration.Subagent
	for index := range parentSnapshot.Projection.Subagents {
		if parentSnapshot.Projection.Subagents[index].ID == orchestration.NodeID(input.TaskID) {
			task = &parentSnapshot.Projection.Subagents[index]
			break
		}
	}
	if task == nil {
		return nil, &Failure{Tag: "OrchestratorMcpFailure", Code: "task_not_found", Message: "task not found: " + input.TaskID}
	}
	if task.ChildThreadID == nil {
		return nil, &Failure{Tag: "OrchestratorMcpFailure", Code: "task_not_found", Message: "task has no local child thread"}
	}
	childSnapshot, err := s.orchestrator.Snapshot(context.Background(), *task.ChildThreadID)
	if err != nil {
		return nil, failureFromError(err)
	}
	var childRunID *orchestration.RunID
	status := string(task.Status)
	if len(childSnapshot.Projection.Runs) > 0 {
		childRunID = &childSnapshot.Projection.Runs[0].ID
		status = string(childSnapshot.Projection.Runs[0].Status)
	}
	workState := "working"
	if task.CompletedAt != nil {
		workState = "result_available"
	}
	hasPending := false
	var latestTerminalRunID *orchestration.RunID
	var latestTerminalStatus *string
	var latestTerminalSummary *string
	for _, run := range childSnapshot.Projection.Runs {
		if run.Status.Active() || run.Status == orchestration.RunQueued {
			hasPending = true
		}
		if run.Status.Terminal() {
			latestTerminalRunID = &run.ID
			value := string(run.Status)
			latestTerminalStatus = &value
			latestTerminalSummary = latestAssistant(childSnapshot.Projection.Messages, run.ID)
		}
	}
	var resultTransferID *orchestration.ContextTransferID
	for _, transfer := range parentSnapshot.Projection.ContextTransfers {
		if transfer.Type == "subagent_result" && transfer.SourceThreadID == *task.ChildThreadID {
			resultTransferID = &transfer.ID
		}
	}
	hasQuestion := false
	var pendingApproval any
	for _, request := range childSnapshot.Projection.RuntimeRequests {
		if request.Status != orchestration.RequestPending {
			continue
		}
		if request.Kind == orchestration.RequestUserInput {
			hasQuestion = true
		} else {
			pendingApproval = map[string]any{"requestId": request.ID, "kind": request.Kind, "runId": childRunID}
		}
	}
	result := map[string]any{
		"taskId": task.ID, "childThreadId": task.ChildThreadID, "childRunId": childRunID,
		"childNodeId": task.ID, "status": status, "workState": workState,
		"hasPendingChildRuns": hasPending, "providerInstanceId": task.ProviderInstanceID, "model": task.Model,
		"summary": task.Result, "resultContextTransferId": resultTransferID,
		"latestTerminalRunId": latestTerminalRunID, "latestTerminalStatus": latestTerminalStatus,
		"latestTerminalSummary": latestTerminalSummary, "hasPendingQuestion": hasQuestion,
		"pendingApproval": pendingApproval, "waitTimedOut": false,
	}
	if workState == "working" && !waiting {
		result["next"] = runningTaskNext
	}
	return result, nil
}

func (s *Server) taskCancel(ctx context.Context, input TaskInput) (any, *Failure) {
	status, failure := s.taskStatus(input, false)
	if failure != nil {
		return nil, failure
	}
	values := status.(map[string]any)
	childThreadID := orchestration.ThreadID(fmt.Sprint(values["childThreadId"]))
	snapshot, err := s.orchestrator.Snapshot(ctx, childThreadID)
	if err != nil {
		return nil, failureFromError(err)
	}
	key := requestKeyOrRandom("")
	changed := false
	for _, run := range snapshot.Projection.Runs {
		if run.Status.Active() {
			result, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandRunInterrupt,
				CommandID: orchestration.CommandID(scopedID("command:mcp", key, "cancel", run.ID)), ThreadID: childThreadID,
				Payload: orchestration.InterruptRun{RunID: run.ID, Reason: input.Reason, HoldQueue: true}})
			if err != nil {
				return nil, failureFromError(err)
			}
			s.runEffects(result.Effects)
			changed = true
		} else if run.Status == orchestration.RunQueued {
			_, err := s.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandQueuedRunCancel,
				CommandID: orchestration.CommandID(scopedID("command:mcp", key, "cancel-queued", run.ID)), ThreadID: childThreadID,
				Payload: orchestration.CancelQueuedRun{RunID: run.ID}})
			if err != nil {
				return nil, failureFromError(err)
			}
			changed = true
		}
	}
	if !changed {
		return map[string]any{"taskId": input.TaskID, "childThreadId": childThreadID, "status": values["status"]}, nil
	}
	return map[string]any{"taskId": input.TaskID, "childThreadId": childThreadID, "status": "interrupted"}, nil
}

func latestAssistant(messages []orchestration.ConversationMessage, runID orchestration.RunID) *string {
	var result *string
	for _, message := range messages {
		if message.RunID != nil && *message.RunID == runID && message.Role == orchestration.MessageAssistant {
			text := message.Text
			result = &text
		}
	}
	return result
}
