//go:build live

package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

func TestLiveCLIOrchestration(t *testing.T) {
	if os.Getenv("G0CODE_LIVE") != "1" {
		t.Skip("set G0CODE_LIVE=1 to run real provider turns")
	}
	tests := []struct {
		name       string
		binary     string
		instanceID orchestration.ProviderInstanceID
	}{
		{name: "codex", binary: "codex", instanceID: "codex"},
		{name: "claude", binary: "claude", instanceID: "claude"},
		{name: "pi", binary: "pi", instanceID: "pi"},
		{name: "agy", binary: "agy", instanceID: "agy"},
		{name: "fx", binary: "fx", instanceID: "fx"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := exec.LookPath(test.binary); err != nil {
				t.Skipf("%s is not installed", test.binary)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			store := orchestration.NewMemoryStore()
			o := orchestration.New(store)
			executor := NewExecutor(o)
			defer executor.Close()
			executor.RegisterDefaults()
			if test.name == "pi" {
				executor.Register("pi", &PiAdapter{SessionDir: t.TempDir()})
			}
			threadID := orchestration.ThreadID("live-" + test.name)
			workspace := t.TempDir()
			dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
				Payload: orchestration.CreateThread{ProjectID: "live", Title: "Live " + test.name,
					ModelSelection: orchestration.ModelSelection{InstanceID: test.instanceID, Model: "default"},
					WorktreePath:   &workspace, RuntimeMode: orchestration.RuntimeFullAccess,
					InteractionMode: orchestration.InteractionDefault, CreatedBy: orchestration.ActorUser, CreationSource: orchestration.CreationWeb}})

			firstErr := runLiveMessage(t, ctx, executor, threadID, "first", "Do not use tools. Reply with exactly ORCHESTRATION_OK.")
			usageLimited := false
			if firstErr != nil {
				var turnErr *TurnError
				if test.name == "claude" && errors.As(firstErr, &turnErr) && turnErr.Class == "usage_limit" {
					usageLimited = true
					assertLiveUsageLimit(t, o, threadID)
					return
				}
				t.Fatalf("first live turn failed: %v", firstErr)
			}
			assertLiveTurn(t, o, threadID, 1, "ORCHESTRATION_OK")
			if usageLimited {
				return
			}

			secondErr := runLiveMessage(t, ctx, executor, threadID, "second", "Do not use tools. Reply with exactly SECOND_TURN_OK.")
			if secondErr != nil {
				t.Fatalf("resumed live turn failed: %v", secondErr)
			}
			assertLiveTurn(t, o, threadID, 2, "SECOND_TURN_OK")
			assertAssistantText(t, snapshotMessages(t, o, threadID), "ORCHESTRATION_OK")
			snapshot, err := o.Snapshot(ctx, threadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Projection.Runs) != 2 || len(snapshot.Projection.ProviderThreads) != 1 || len(snapshot.Projection.Messages) != 4 {
				t.Fatalf("resume created a divergent graph: runs=%d providerThreads=%d messages=%d",
					len(snapshot.Projection.Runs), len(snapshot.Projection.ProviderThreads), len(snapshot.Projection.Messages))
			}
			assertSemanticProjectionIntegrity(t, snapshot.Projection)
		})
	}
}

func TestLiveModelCatalogs(t *testing.T) {
	if os.Getenv("G0CODE_LIVE") != "1" {
		t.Skip("set G0CODE_LIVE=1 to query native model catalogs")
	}
	o := orchestration.New(nil)
	executor := NewExecutor(o)
	executor.RegisterDefaults()
	defer executor.Close()
	for _, instance := range []orchestration.ProviderInstanceID{"codex", "pi", "agy", "fx", "claude"} {
		t.Run(string(instance), func(t *testing.T) {
			models, err := executor.Models(context.Background(), instance, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if len(models) == 0 {
				t.Fatal("empty model catalog")
			}
			found := false
			for _, model := range models {
				for _, option := range model.Options {
					switch option.ID {
					case "reasoningEffort", "thinking", "effort", "serviceTier", "fastMode":
						found = true
					}
				}
			}
			if !found && instance != "agy" {
				t.Fatalf("no reasoning/service/fast options in catalog: %+v", models)
			}
		})
	}
}

func TestLiveCodexWorkspaceEdit(t *testing.T) {
	if os.Getenv("G0CODE_LIVE") != "1" {
		t.Skip("set G0CODE_LIVE=1 to run the real provider")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	o := orchestration.New(nil)
	executor := NewExecutor(o)
	executor.RegisterDefaults()
	defer executor.Close()
	threadID := orchestration.ThreadID("live-workspace-edit")
	workspace := t.TempDir()
	dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
		Payload: orchestration.CreateThread{ProjectID: "live", Title: "Workspace edit", WorktreePath: &workspace,
			RuntimeMode: orchestration.RuntimeAutoAcceptEdits, InteractionMode: orchestration.InteractionDefault,
			ModelSelection: orchestration.ModelSelection{InstanceID: "codex", Model: "default"}}})
	started := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "edit", ThreadID: threadID,
		Payload: orchestration.DispatchMessage{MessageID: "edit-message",
			Text:         "Create a local file named result.txt containing exactly G0CODE_TOOLS_OK, then read it with a local shell command to verify it. Do not use network or MCP.",
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}}})
	done := make(chan error, 1)
	go func() { done <- executor.Execute(ctx, started.Effects[0]) }()
	for {
		snapshot, err := o.Snapshot(ctx, threadID)
		if err != nil {
			t.Fatal(err)
		}
		responded := false
		for _, request := range snapshot.Projection.RuntimeRequests {
			if request.Status != orchestration.RequestPending {
				continue
			}
			decision := "accept"
			response := dispatch(t, o, orchestration.Command{Type: orchestration.CommandRuntimeRequestRespond,
				CommandID: orchestration.CommandID("approve-" + string(request.ID)), ThreadID: threadID,
				Payload: orchestration.RuntimeRequestResponse{RequestID: request.ID, Decision: &decision}})
			for _, effect := range response.Effects {
				if err := executor.Execute(ctx, effect); err != nil {
					t.Fatal(err)
				}
			}
			responded = true
		}
		if responded {
			continue
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			goto finished
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}

finished:
	content, err := os.ReadFile(filepath.Join(workspace, "result.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(content)) != "G0CODE_TOOLS_OK" {
		t.Fatalf("result.txt = %q", content)
	}
	snapshot, _ := o.Snapshot(ctx, threadID)
	if snapshot.Projection.Runs[0].Status != orchestration.RunCompleted {
		t.Fatalf("run did not complete: %+v", snapshot.Projection.Runs[0])
	}
	foundTool := false
	for _, item := range snapshot.Projection.TurnItems {
		if item.Type == "command_execution" || item.Type == "file_change" {
			foundTool = true
		}
	}
	if !foundTool {
		t.Fatalf("native tool activity missing: %+v", snapshot.Projection.TurnItems)
	}
}

func runLiveMessage(t *testing.T, ctx context.Context, executor *Executor, threadID orchestration.ThreadID, suffix, prompt string) error {
	t.Helper()
	result := dispatch(t, executor.orchestrator, orchestration.Command{
		Type: orchestration.CommandMessageDispatch, CommandID: orchestration.CommandID("message-" + suffix), ThreadID: threadID,
		Payload: orchestration.DispatchMessage{MessageID: orchestration.MessageID("user-" + suffix), Text: prompt,
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately},
			CreatedBy:    orchestration.ActorUser, CreationSource: orchestration.CreationWeb},
	})
	if len(result.Effects) != 1 {
		t.Fatalf("message %s produced %d effects", suffix, len(result.Effects))
	}
	return executor.Execute(ctx, result.Effects[0])
}

func snapshotMessages(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID) []orchestration.ConversationMessage {
	t.Helper()
	snapshot, err := o.Snapshot(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.Projection.Messages
}

func assertLiveTurn(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID, ordinal int, expected string) {
	t.Helper()
	snapshot, err := o.Snapshot(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	run := snapshot.Projection.Runs[ordinal-1]
	if run.Ordinal != ordinal || run.Status != orchestration.RunCompleted {
		t.Fatalf("run %d status = %s", ordinal, run.Status)
	}
	assertAssistantText(t, snapshot.Projection.Messages, expected)
	if snapshot.Projection.Thread.ActiveProviderThreadID == nil {
		t.Fatal("live turn did not bind a provider thread")
	}
	providerThread, ok := snapshot.Projection.ProviderThread(*snapshot.Projection.Thread.ActiveProviderThreadID)
	if !ok || providerThread.NativeThreadRef == nil || providerThread.NativeThreadRef.NativeID == nil {
		t.Fatalf("live turn did not retain a native resume ref: %+v", providerThread)
	}
	assertSemanticProjectionIntegrity(t, snapshot.Projection)
}

func assertLiveUsageLimit(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID) {
	t.Helper()
	snapshot, err := o.Snapshot(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Projection.Runs) != 1 || snapshot.Projection.Runs[0].Status != orchestration.RunFailed {
		t.Fatalf("usage limit did not fail the run: %+v", snapshot.Projection.Runs)
	}
	for _, item := range snapshot.Projection.TurnItems {
		if item.Type != "provider_failure" {
			continue
		}
		failure, ok := item.Data["failure"].(map[string]any)
		if ok && failure["class"] == "usage_limit" {
			return
		}
		// In-process effects retain the concrete failure value; JSON snapshots
		// decode Data into maps. Handle both representations.
		if failureValue, ok := item.Data["failure"].(orchestration.ProviderFailure); ok && failureValue.Class == "usage_limit" {
			return
		}
	}
	t.Fatalf("missing usage-limit failure item: %+v", snapshot.Projection.TurnItems)
}
