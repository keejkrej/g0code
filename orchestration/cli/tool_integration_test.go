package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

func TestCodexNativeCommandApprovalRoundTrip(t *testing.T) {
	runNativeApprovalFixture(t, "tool_call_read_only_on_request", "codex", "accept", false)
}

func TestFxNativeDeniedWriteRoundTrip(t *testing.T) {
	runNativeApprovalFixture(t, "tool_call_denied_write", "fx", "decline", true)
}

func runNativeApprovalFixture(t *testing.T, scenario, provider, decision string, expectDenied bool) {
	t.Helper()
	ctx := context.Background()
	store := orchestration.NewMemoryStore()
	o := orchestration.New(store)
	executor := NewExecutor(o)
	fixture, err := filepath.Abs(filepath.Join("testdata", "t3code-v0.1.7", scenario, provider+"_transcript.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	binary := nativeHelperBinary(t, fixture, provider)
	instanceID := orchestration.ProviderInstanceID(provider)
	var adapter Adapter = &CodexAdapter{Binary: binary}
	if provider == "fx" {
		adapter = &ACPAdapter{Binary: binary, Args: []string{"acp"}, DriverKind: "fx", Mode: "ask"}
	}
	executor.Register(instanceID, adapter)
	defer executor.Close()
	threadID := orchestration.ThreadID("tool-" + provider)
	workspace := t.TempDir()
	var options map[string]any
	if provider == "fx" {
		options = map[string]any{"reasoningEffort": "medium"}
	}
	dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
		Payload: orchestration.CreateThread{ProjectID: "project", Title: "Tool fixture", WorktreePath: &workspace,
			RuntimeMode: orchestration.RuntimeAutoAcceptEdits, InteractionMode: orchestration.InteractionDefault,
			ModelSelection: orchestration.ModelSelection{InstanceID: instanceID, Model: "default", Options: options}}})
	started := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "start", ThreadID: threadID,
		Payload: orchestration.DispatchMessage{MessageID: "user", Text: "Run the recorded tool scenario",
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}}})
	turnDone := make(chan error, 1)
	go func() { turnDone <- executor.Execute(ctx, started.Effects[0]) }()
	request := waitForPendingRequest(t, o, threadID, turnDone)
	response := dispatch(t, o, orchestration.Command{Type: orchestration.CommandRuntimeRequestRespond,
		CommandID: "respond", ThreadID: threadID, Payload: orchestration.RuntimeRequestResponse{
			RequestID: request.ID, Decision: &decision}})
	if len(response.Effects) != 1 {
		t.Fatalf("response effects = %+v", response.Effects)
	}
	if err := executor.Execute(ctx, response.Effects[0]); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-turnDone:
		if err != nil {
			t.Fatalf("turn failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("native turn did not finish after approval")
	}
	snapshot, _ := o.Snapshot(ctx, threadID)
	resolved, ok := snapshot.Projection.RuntimeRequest(request.ID)
	if !ok || resolved.Status != orchestration.RequestResolved {
		t.Fatalf("request did not resolve: %+v", resolved)
	}
	if snapshot.Projection.Runs[0].Status != orchestration.RunCompleted {
		t.Fatalf("run did not complete: %+v", snapshot.Projection.Runs[0])
	}
	foundTool := false
	for _, item := range snapshot.Projection.TurnItems {
		if item.Type == "command_execution" || item.Type == "file_change" || item.Type == "dynamic_tool" {
			foundTool = true
			if expectDenied && item.Status != orchestration.TurnItemFailed {
				t.Fatalf("denied tool did not fail: %+v", item)
			}
		}
		if item.RequestID == request.ID && expectDenied && item.Status != orchestration.TurnItemDeclined {
			t.Fatalf("approval item is not declined: %+v", item)
		}
	}
	if !foundTool {
		t.Fatalf("tool call missing: %+v", snapshot.Projection.TurnItems)
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitForPendingRequest(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID, turnDone <-chan error) orchestration.RuntimeRequest {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := o.Snapshot(context.Background(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		for _, request := range snapshot.Projection.RuntimeRequests {
			if request.Status == orchestration.RequestPending {
				return request
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-turnDone:
		t.Fatalf("turn ended before a pending request on %s: %v", threadID, err)
	default:
	}
	t.Fatal(fmt.Sprintf("no pending request on %s", threadID))
	return orchestration.RuntimeRequest{}
}
