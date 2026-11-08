package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keejkrej/g0code/orchestration"
	"github.com/keejkrej/g0code/orchestration/cli"
)

type testClient struct {
	t      *testing.T
	writer io.WriteCloser
	reader *bufio.Reader
	nextID int
}

func (c *testClient) call(method string, params any) map[string]any {
	c.t.Helper()
	c.nextID++
	id := c.nextID
	if err := json.NewEncoder(c.writer).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		c.t.Fatal(err)
	}
	for {
		line, err := c.reader.ReadBytes('\n')
		if err != nil {
			c.t.Fatal(err)
		}
		var response struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  any             `json:"error"`
		}
		if err := json.Unmarshal(line, &response); err != nil {
			c.t.Fatal(err)
		}
		if response.ID != id {
			continue
		}
		if response.Error != nil {
			c.t.Fatalf("RPC %s failed: %v", method, response.Error)
		}
		var result map[string]any
		if err := json.Unmarshal(response.Result, &result); err != nil {
			c.t.Fatal(err)
		}
		return result
	}
}

func (c *testClient) tool(name string, arguments any) map[string]any {
	c.t.Helper()
	result := c.call("tools/call", map[string]any{"name": name, "arguments": arguments})
	if result["isError"] == true {
		c.t.Fatalf("tool %s failed: %+v", name, result)
	}
	structured, ok := result["structuredContent"].(map[string]any)
	if !ok {
		c.t.Fatalf("tool %s has no structured content: %+v", name, result)
	}
	return structured
}

type blockingParentExecutor struct {
	executor *cli.Executor
	block    atomic.Bool
	release  chan struct{}
}

func (e *blockingParentExecutor) Execute(ctx context.Context, effect orchestration.Effect) error {
	if e.block.CompareAndSwap(true, false) {
		<-e.release
	}
	return e.executor.Execute(ctx, effect)
}

func TestMCPOrchestratorWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := orchestration.NewMemoryStore()
	o := orchestration.New(store)
	cliExecutor := cli.NewExecutor(o)
	defer cliExecutor.Close()
	replay, err := cli.NewReplayAdapter(&cli.CodexAdapter{}, readSimpleFrames(t))
	if err != nil {
		t.Fatal(err)
	}
	cliExecutor.Register("codex", replay)
	executor := &blockingParentExecutor{executor: cliExecutor, release: make(chan struct{})}
	executor.block.Store(true)
	workspace := t.TempDir()
	server := NewServer(o, executor, Config{
		Providers:        []Provider{{InstanceID: "codex", Driver: "codex", Models: []string{"default"}, Available: true}},
		DefaultProjectID: "project-test", DefaultWorktreePath: &workspace,
		DefaultTarget: orchestration.ModelSelection{InstanceID: "codex", Model: "default"},
	})
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	defer inputWriter.Close()
	defer outputReader.Close()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, inputReader, outputWriter) }()
	client := &testClient{t: t, writer: inputWriter, reader: bufio.NewReader(outputReader)}

	initialized := client.call("initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{},
		"clientInfo": map[string]any{"name": "mcp-test", "version": "1"}})
	if initialized["protocolVersion"] != "2025-06-18" {
		t.Fatalf("initialize = %+v", initialized)
	}
	tools := client.call("tools/list", map[string]any{})
	toolList := tools["tools"].([]any)
	if len(toolList) != 11 {
		t.Fatalf("tool count = %d, want 11", len(toolList))
	}

	created := client.tool("create_threads", map[string]any{"threads": []any{map[string]any{
		"prompt": "Respond with the following text: fixture simple ok", "title": "MCP parent",
	}}, "clientRequestId": "parent-create"})
	parentID := created["threads"].([]any)[0].(map[string]any)["threadId"].(string)
	parentRunID := created["threads"].([]any)[0].(map[string]any)["runId"].(string)
	if created["threads"].([]any)[0].(map[string]any)["status"] != "starting" {
		t.Fatalf("parent did not start: %+v", created)
	}

	delegated := client.tool("delegate_task", map[string]any{"task": "Return the child result only.",
		"mode": "wait", "timeoutMs": 2000, "clientRequestId": "delegate-1", "parentThreadId": parentID})
	taskID := delegated["taskId"].(string)
	childID := delegated["childThreadId"].(string)
	if delegated["workState"] != "result_available" || delegated["summary"] != "fixture simple ok" ||
		delegated["resultContextTransferId"] == nil || delegated["status"] != "completed" {
		t.Fatalf("delegated result mismatch: %+v", delegated)
	}
	childRead := client.tool("t3_thread_read", map[string]any{"threadId": childID})
	childItems := childRead["items"].([]any)
	for _, raw := range childItems {
		item := raw.(map[string]any)
		if item["type"] == "user_message" && item["text"] != "Return the child result only." {
			t.Fatalf("child inherited parent prompt: %+v", item)
		}
	}
	status := client.tool("task_status", map[string]any{"taskId": taskID, "parentThreadId": parentID})
	if status["summary"] != delegated["summary"] || status["childRunId"] != delegated["childRunId"] {
		t.Fatalf("task result changed: %+v", status)
	}
	retried := client.tool("delegate_task", map[string]any{"task": "Return the child result only.",
		"clientRequestId": "delegate-1", "parentThreadId": parentID})
	if retried["taskId"] != taskID || retried["childThreadId"] != childID {
		t.Fatalf("retry created new work: %+v", retried)
	}

	close(executor.release)
	wait := client.tool("t3_thread_wait", map[string]any{"threadId": parentID, "runId": parentRunID, "timeoutMs": 2000})
	if wait["status"] != "completed" || wait["timedOut"] != false {
		t.Fatalf("parent wait = %+v", wait)
	}
	read := client.tool("t3_thread_read", map[string]any{"threadId": parentID})
	foundAssistant := false
	for _, raw := range read["items"].([]any) {
		item := raw.(map[string]any)
		if item["type"] == "assistant_message" && item["text"] == "fixture simple ok" {
			foundAssistant = true
		}
		if item["type"] == "user_message" && (item["createdBy"] != "agent" || item["creationSource"] != "mcp") {
			t.Fatalf("MCP provenance missing: %+v", item)
		}
	}
	if !foundAssistant {
		t.Fatalf("parent result missing: %+v", read)
	}
	listed := client.tool("t3_thread_list", map[string]any{"includeSubagents": false})
	if listed["total"].(float64) != 1 {
		t.Fatalf("root list included child: %+v", listed)
	}
	listed = client.tool("t3_thread_list", map[string]any{"includeSubagents": true})
	if listed["total"].(float64) != 2 {
		t.Fatalf("subagent list missing child: %+v", listed)
	}

	sent := client.tool("t3_thread_send", map[string]any{"threadId": parentID,
		"message": "Respond with the following text: fixture simple ok", "clientRequestId": "followup-1"})
	if sent["delivery"] != "started" || sent["status"] != "starting" {
		t.Fatalf("followup = %+v", sent)
	}
	wait = client.tool("t3_thread_wait", map[string]any{"threadId": parentID, "runId": sent["runId"], "timeoutMs": 2000})
	if wait["status"] != "completed" {
		t.Fatalf("followup wait = %+v", wait)
	}
	idle := client.tool("create_threads", map[string]any{"threads": []any{map[string]any{"title": "Idle"}}, "clientRequestId": "idle-1"})
	idleAgain := client.tool("create_threads", map[string]any{"threads": []any{map[string]any{"title": "Idle"}}, "clientRequestId": "idle-1"})
	if idle["threads"].([]any)[0].(map[string]any)["threadId"] != idleAgain["threads"].([]any)[0].(map[string]any)["threadId"] {
		t.Fatal("idempotent create returned a different thread")
	}
	interrupt := client.tool("t3_thread_interrupt", map[string]any{"threadId": parentID})
	if interrupt["status"] != "no_active_run" {
		t.Fatalf("terminal interrupt = %+v", interrupt)
	}

	parentSnapshot, err := o.Snapshot(ctx, orchestration.ThreadID(parentID))
	if err != nil {
		t.Fatal(err)
	}
	providerThread, _ := parentSnapshot.Projection.ProviderThread(*parentSnapshot.Projection.Thread.ActiveProviderThreadID)
	request := orchestration.RuntimeRequest{ID: "mcp-pending-1", NodeID: *parentSnapshot.Projection.Runs[0].RootNodeID,
		Kind: orchestration.RequestUserInput, Status: orchestration.RequestPending,
		ResponseCapability: orchestration.ResponseCapability{Type: "live", ProviderSessionID: providerThread.ProviderSessionID},
		CreatedAt:          time.Now().UTC()}
	rawID := orchestration.RawEventID("mcp-pending-raw")
	if _, err := o.Ingest(ctx, orchestration.IngestInput{ThreadID: orchestration.ThreadID(parentID),
		ProviderInstanceID: "codex", RawEventID: &rawID, Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventRuntimeRequestUpdated, RuntimeRequest: &request}}); err != nil {
		t.Fatal(err)
	}
	pending := client.tool("t3_pending_request", map[string]any{"threadId": parentID, "action": "list"})
	if len(pending["requests"].([]any)) != 1 {
		t.Fatalf("pending list = %+v", pending)
	}
	client.tool("t3_pending_request", map[string]any{"threadId": parentID, "action": "respond",
		"requestId": "mcp-pending-1", "answers": map[string]any{"answer": "yes"}, "clientRequestId": "answer-1"})
	readRequest := client.tool("t3_pending_request", map[string]any{"threadId": parentID, "action": "read", "requestId": "mcp-pending-1"})
	if readRequest["request"].(map[string]any)["status"] != "resolved" {
		t.Fatalf("request did not resolve: %+v", readRequest)
	}

	inputWriter.Close()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MCP server did not stop")
	}
}

func readSimpleFrames(t *testing.T) []json.RawMessage {
	t.Helper()
	path := filepath.Join("..", "cli", "testdata", "t3code-v0.1.7", "simple", "codex_transcript.ndjson")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var frames []json.RawMessage
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var entry struct {
			Type  string          `json:"type"`
			Frame json.RawMessage `json:"frame"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Type == "emit_inbound" {
			frames = append(frames, entry.Frame)
		}
	}
	return frames
}
