package mcp

import (
	"bytes"
	"context"
	"encoding/json"
)

type Failure struct {
	Tag     string `json:"_tag"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations,omitempty"`
}

func (s *Server) tools() []tool {
	return []tool{
		makeTool("orchestrator_capabilities", "List providers, models, options, and runtime modes for delegate_task.", false, "search"),
		makeTool("delegate_task", "Start one child agent with only task; no parent history. Keep taskId for task_status and task_cancel.", false, "task", "target", "title", "role", "mode", "timeoutMs", "clientRequestId", "runtimeMode", "interactionMode", "parentThreadId"),
		makeTool("task_status", "Read one delegated task and its durable result.", true, "taskId", "parentThreadId"),
		makeTool("task_cancel", "Stop one delegated task and its child thread.", false, "taskId", "reason", "parentThreadId"),
		makeTool("create_threads", "Create one to twenty ordinary top-level threads in this checkout.", false, "threads", "clientRequestId"),
		makeTool("t3_thread_list", "List durable threads newest first with bounded offset pagination.", true, "projectId", "allProjects", "statuses", "titleContains", "includeSubagents", "cursor", "limit"),
		makeTool("t3_thread_read", "Read durable thread state, recent runs, and timeline items.", true, "threadId", "itemId", "textOffset", "view", "afterPosition", "limit", "runLimit", "maxCharsPerItem"),
		makeTool("t3_thread_send", "Send or queue a follow-up message on an app-owned thread.", false, "threadId", "message", "mode", "clientRequestId"),
		makeTool("t3_thread_wait", "Wait for a selected run to settle. Timeout never cancels work.", true, "threadId", "runId", "timeoutMs"),
		makeTool("t3_thread_interrupt", "Interrupt an active run through the normal run.interrupt command.", false, "threadId", "runId", "reason", "stop", "clientRequestId"),
		makeTool("t3_pending_request", "List, read, or respond to pending provider requests on a thread.", false, "threadId", "action", "requestId", "decision", "feedback", "answers", "clientRequestId"),
	}
}

func makeTool(name, description string, readOnly bool, properties ...string) tool {
	propertyMap := make(map[string]any, len(properties))
	for _, property := range properties {
		propertyMap[property] = map[string]any{}
	}
	return tool{Name: name, Description: description,
		InputSchema: map[string]any{"type": "object", "properties": propertyMap, "additionalProperties": true},
		Annotations: map[string]any{"readOnlyHint": readOnly, "destructiveHint": !readOnly, "idempotentHint": readOnly}}
}

func decodeInput(raw json.RawMessage, target any) *Failure {
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(target); err != nil {
		return invalidRequest(err.Error())
	}
	return nil
}

type Target struct {
	ProviderInstanceID string         `json:"providerInstanceId,omitempty"`
	DriverKind         string         `json:"driverKind,omitempty"`
	Model              string         `json:"model,omitempty"`
	Options            map[string]any `json:"options,omitempty"`
}

type CapabilitiesInput struct {
	Search string `json:"search,omitempty"`
}

type DelegateTaskInput struct {
	Task            string  `json:"task"`
	Target          *Target `json:"target,omitempty"`
	Title           string  `json:"title,omitempty"`
	Role            string  `json:"role,omitempty"`
	Isolation       string  `json:"isolation,omitempty"`
	Mode            string  `json:"mode,omitempty"`
	TimeoutMS       int     `json:"timeoutMs,omitempty"`
	ClientRequestID string  `json:"clientRequestId,omitempty"`
	RuntimeMode     string  `json:"runtimeMode,omitempty"`
	InteractionMode string  `json:"interactionMode,omitempty"`
	ParentThreadID  string  `json:"parentThreadId,omitempty"`
}

type CreateThreadsInput struct {
	Threads []struct {
		Prompt          string  `json:"prompt,omitempty"`
		Title           string  `json:"title,omitempty"`
		Target          *Target `json:"target,omitempty"`
		RuntimeMode     string  `json:"runtimeMode,omitempty"`
		InteractionMode string  `json:"interactionMode,omitempty"`
	} `json:"threads"`
	ClientRequestID string `json:"clientRequestId,omitempty"`
}

type TaskInput struct {
	TaskID         string `json:"taskId"`
	ParentThreadID string `json:"parentThreadId,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

type ThreadListInput struct {
	ProjectID        string   `json:"projectId,omitempty"`
	AllProjects      bool     `json:"allProjects,omitempty"`
	Statuses         []string `json:"statuses,omitempty"`
	TitleContains    string   `json:"titleContains,omitempty"`
	IncludeSubagents bool     `json:"includeSubagents,omitempty"`
	Cursor           int      `json:"cursor,omitempty"`
	Limit            int      `json:"limit,omitempty"`
}

type ThreadReadInput struct {
	ThreadID        string `json:"threadId"`
	ItemID          string `json:"itemId,omitempty"`
	TextOffset      int    `json:"textOffset,omitempty"`
	View            string `json:"view,omitempty"`
	AfterPosition   int    `json:"afterPosition,omitempty"`
	Limit           int    `json:"limit,omitempty"`
	RunLimit        int    `json:"runLimit,omitempty"`
	MaxCharsPerItem int    `json:"maxCharsPerItem,omitempty"`
}

type ThreadSendInput struct {
	ThreadID        string `json:"threadId"`
	Message         string `json:"message"`
	Mode            string `json:"mode,omitempty"`
	ClientRequestID string `json:"clientRequestId,omitempty"`
}

type ThreadWaitInput struct {
	ThreadID  string `json:"threadId"`
	RunID     string `json:"runId,omitempty"`
	TimeoutMS int    `json:"timeoutMs,omitempty"`
}

type ThreadInterruptInput struct {
	ThreadID        string `json:"threadId"`
	RunID           string `json:"runId,omitempty"`
	Reason          string `json:"reason,omitempty"`
	Stop            bool   `json:"stop,omitempty"`
	ClientRequestID string `json:"clientRequestId,omitempty"`
}

func (s *Server) callTool(ctx context.Context, name string, raw json.RawMessage) (any, *Failure) {
	switch name {
	case "orchestrator_capabilities":
		var input CapabilitiesInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.capabilities(input)
	case "create_threads":
		var input CreateThreadsInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.createThreads(ctx, input)
	case "t3_thread_list":
		var input ThreadListInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.threadList(input)
	case "t3_thread_read":
		var input ThreadReadInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.threadRead(input)
	case "t3_thread_send":
		var input ThreadSendInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.threadSend(ctx, input)
	case "t3_thread_wait":
		var input ThreadWaitInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.threadWait(ctx, input)
	case "t3_thread_interrupt":
		var input ThreadInterruptInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.threadInterrupt(ctx, input)
	case "t3_pending_request":
		var input PendingRequestInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.pendingRequest(ctx, input)
	case "delegate_task":
		var input DelegateTaskInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.delegateTask(ctx, input)
	case "task_status":
		var input TaskInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.taskStatus(input, false)
	case "task_cancel":
		var input TaskInput
		if failure := decodeInput(raw, &input); failure != nil {
			return nil, failure
		}
		return s.taskCancel(ctx, input)
	default:
		return nil, invalidRequest("unknown tool: " + name)
	}
}
