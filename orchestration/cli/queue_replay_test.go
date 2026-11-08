package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/keejkrej/g0code/orchestration"
)

func TestT3CodeQueuedTurnFixtureParity(t *testing.T) {
	tests := []struct {
		name       string
		instanceID orchestration.ProviderInstanceID
		adapter    frameHandler
	}{
		{name: "codex", instanceID: "codex", adapter: &CodexAdapter{}},
		{name: "claude", instanceID: "claudeAgent", adapter: &ClaudeAdapter{}},
		{name: "fx", instanceID: "fx", adapter: &ACPAdapter{DriverKind: "fx"}},
		{name: "registry", instanceID: "acpRegistry", adapter: &ACPAdapter{DriverKind: "acpRegistry"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := orchestration.NewMemoryStore()
			o := orchestration.New(store)
			executor := NewExecutor(o)
			executor.Register(test.instanceID, &SequenceAdapter{Adapter: test.adapter,
				Turns: splitQueuedTranscript(t, test.name)})
			threadID := orchestration.ThreadID("queued-" + test.name)
			workspace := t.TempDir()
			dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
				Payload: orchestration.CreateThread{ProjectID: "project", Title: "Queued " + test.name,
					ModelSelection: orchestration.ModelSelection{InstanceID: test.instanceID, Model: "default"}, WorktreePath: &workspace}})
			first := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "first", ThreadID: threadID,
				Payload: orchestration.DispatchMessage{MessageID: "first-message", Text: "Respond with exactly: first fixture turn complete",
					DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}}})
			dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "second", ThreadID: threadID,
				Payload: orchestration.DispatchMessage{MessageID: "second-message", Text: "Respond with exactly: second fixture turn complete",
					DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchQueueAfterActive}}})
			snapshot, _ := o.Snapshot(ctx, threadID)
			if len(snapshot.Projection.Runs) != 2 || snapshot.Projection.Runs[1].Status != orchestration.RunQueued {
				t.Fatalf("second run was not queued: %+v", snapshot.Projection.Runs)
			}
			if err := executor.Execute(ctx, first.Effects[0]); err != nil {
				t.Fatalf("queued replay failed: %v", err)
			}
			snapshot, _ = o.Snapshot(ctx, threadID)
			projection := snapshot.Projection
			if len(projection.Runs) != 2 {
				t.Fatalf("run count = %d", len(projection.Runs))
			}
			for index, run := range projection.Runs {
				if run.Ordinal != index+1 || run.Status != orchestration.RunCompleted {
					t.Errorf("run %d mismatch: %+v", index, run)
				}
			}
			roles := make([]orchestration.MessageRole, len(projection.Messages))
			for index, message := range projection.Messages {
				roles[index] = message.Role
			}
			expectedRoles := []orchestration.MessageRole{orchestration.MessageUser, orchestration.MessageAssistant,
				orchestration.MessageUser, orchestration.MessageAssistant}
			if !equalRoles(roles, expectedRoles) {
				t.Errorf("message roles = %v, want %v", roles, expectedRoles)
			}
			assertAssistantText(t, projection.Messages, "first fixture turn complete")
			assertAssistantText(t, projection.Messages, "second fixture turn complete")
			for _, item := range projection.TurnItems {
				if item.Type == "user_message" && item.RunID != nil && *item.RunID == projection.Runs[1].ID && item.Data["inputIntent"] != "queued_turn" {
					t.Errorf("second user item intent = %v", item.Data["inputIntent"])
				}
			}
			var run2Statuses []orchestration.RunStatus
			for _, event := range mustEvents(t, store, threadID) {
				if event.Event.RunID == nil || *event.Event.RunID != projection.Runs[1].ID {
					continue
				}
				if event.Event.Type == orchestration.EventRunCreated || event.Event.Type == orchestration.EventRunUpdated {
					run2Statuses = append(run2Statuses, event.Event.Payload.(orchestration.Run).Status)
				}
			}
			if len(run2Statuses) < 3 || run2Statuses[0] != orchestration.RunQueued || !hasRunStatus(run2Statuses, orchestration.RunRunning) ||
				run2Statuses[len(run2Statuses)-1] != orchestration.RunCompleted {
				t.Errorf("run 2 transitions = %v", run2Statuses)
			}
			assertSemanticProjectionIntegrity(t, projection)
		})
	}
}

func splitQueuedTranscript(t *testing.T, name string) [][]json.RawMessage {
	t.Helper()
	path := filepath.Join("testdata", "t3code-v0.1.7", "queued_turn", name+"_transcript.ndjson")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	turns := [][]json.RawMessage{{}}
	triggerCount := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		var entry transcriptEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Type != "emit_inbound" && entry.Type != "expect_outbound" {
			continue
		}
		trigger := false
		if entry.Type == "expect_outbound" {
			switch name {
			case "codex":
				trigger = strings.HasPrefix(entry.Label, "turn/start")
			case "claude":
				trigger = strings.HasPrefix(entry.Label, "prompt.offer:")
			default:
				var frame struct {
					Kind   string `json:"kind"`
					Method string `json:"method"`
				}
				_ = json.Unmarshal(entry.Frame, &frame)
				trigger = frame.Kind == "request" && frame.Method == "session/prompt"
			}
		}
		if trigger {
			triggerCount++
			if triggerCount == 2 {
				turns = append(turns, []json.RawMessage{})
			}
		}
		turns[len(turns)-1] = append(turns[len(turns)-1], entry.Frame)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 {
		t.Fatalf("%s split into %d turns", name, len(turns))
	}
	return turns
}

func equalRoles(left, right []orchestration.MessageRole) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func hasRunStatus(statuses []orchestration.RunStatus, expected orchestration.RunStatus) bool {
	for _, status := range statuses {
		if status == expected {
			return true
		}
	}
	return false
}

func mustEvents(t *testing.T, store orchestration.Store, threadID orchestration.ThreadID) []orchestration.StoredEvent {
	t.Helper()
	events, err := store.Read(context.Background(), orchestration.EventFilter{ThreadID: &threadID})
	if err != nil {
		t.Fatal(err)
	}
	return events
}
