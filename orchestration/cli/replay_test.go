package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/keejkrej/g0code/orchestration"
)

type transcriptEntry struct {
	Type  string          `json:"type"`
	Label string          `json:"label"`
	Frame json.RawMessage `json:"frame"`
}

func readTranscript(t *testing.T, name string, includeOutbound bool) []json.RawMessage {
	t.Helper()
	return readScenarioTranscript(t, "simple", name, includeOutbound)
}

func readScenarioTranscript(t *testing.T, scenario, name string, includeOutbound bool) []json.RawMessage {
	t.Helper()
	path := filepath.Join("testdata", "t3code-v0.1.7", scenario, name+"_transcript.ndjson")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var frames []json.RawMessage
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		var entry transcriptEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if entry.Type == "emit_inbound" || (includeOutbound && entry.Type == "expect_outbound") {
			frames = append(frames, entry.Frame)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return frames
}

func TestT3CodeSimpleFixtureParity(t *testing.T) {
	tests := []struct {
		name         string
		instanceID   orchestration.ProviderInstanceID
		adapter      frameHandler
		acpTransport bool
	}{
		{name: "codex", instanceID: "codex", adapter: &CodexAdapter{}},
		{name: "claude", instanceID: "claudeAgent", adapter: &ClaudeAdapter{}},
		{name: "pi", instanceID: "pi", adapter: &PiAdapter{}},
		{name: "fx", instanceID: "fx", adapter: &ACPAdapter{DriverKind: "fx"}, acpTransport: true},
		{name: "registry", instanceID: "acpRegistry", adapter: &ACPAdapter{DriverKind: "acpRegistry"}, acpTransport: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := orchestration.NewMemoryStore()
			o := orchestration.New(store)
			executor := NewExecutor(o)
			executor.Register(test.instanceID, ReplayAdapter{Adapter: test.adapter,
				Frames: readTranscript(t, test.name, test.acpTransport)})
			threadID := orchestration.ThreadID("thread-" + test.name)
			workspace := t.TempDir()
			dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
				Payload: orchestration.CreateThread{ProjectID: "project", Title: "Replay " + test.name,
					ModelSelection: orchestration.ModelSelection{InstanceID: test.instanceID, Model: "default"},
					WorktreePath:   &workspace, CreatedBy: orchestration.ActorUser, CreationSource: orchestration.CreationWeb}})
			result := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "message", ThreadID: threadID,
				Payload: orchestration.DispatchMessage{MessageID: "user-message", Text: "Respond with the following text: fixture simple ok",
					DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately},
					CreatedBy:    orchestration.ActorUser, CreationSource: orchestration.CreationWeb}})
			if len(result.Effects) != 1 {
				t.Fatalf("expected one start effect: %+v", result.Effects)
			}
			if err := executor.Execute(ctx, result.Effects[0]); err != nil {
				t.Fatalf("replay failed: %v", err)
			}
			assertSimpleProjectionParity(t, o, store, threadID)
			if test.name == "pi" {
				assertPiSimpleDetails(t, o, threadID)
			}
		})
	}
}

func TestT3CodeTurnInterruptFixtureParity(t *testing.T) {
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
			executor.Register(test.instanceID, ReplayAdapter{Adapter: test.adapter,
				Frames: readScenarioTranscript(t, "turn_interrupt", test.name, true)})
			threadID := orchestration.ThreadID("interrupt-" + test.name)
			workspace := t.TempDir()
			dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
				Payload: orchestration.CreateThread{ProjectID: "project", Title: "Interrupt " + test.name,
					ModelSelection: orchestration.ModelSelection{InstanceID: test.instanceID, Model: "default"}, WorktreePath: &workspace}})
			started := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "message", ThreadID: threadID,
				Payload: orchestration.DispatchMessage{MessageID: "user-message", Text: "Do not answer immediately. First run the local shell command `sleep 30`, then respond with exactly: interrupt fixture should not finish naturally.",
					DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}}})
			snapshot, _ := o.Snapshot(ctx, threadID)
			run := snapshot.Projection.Runs[0]
			dispatch(t, o, orchestration.Command{Type: orchestration.CommandRunInterrupt, CommandID: "interrupt", ThreadID: threadID,
				Payload: orchestration.InterruptRun{RunID: run.ID}})
			if err := executor.Execute(ctx, started.Effects[0]); err != nil {
				t.Fatalf("interrupt replay failed: %v", err)
			}
			snapshot, _ = o.Snapshot(ctx, threadID)
			projection := snapshot.Projection
			if projection.Runs[0].Status != orchestration.RunInterrupted || projection.Attempts[0].Status != orchestration.AttemptInterrupted ||
				projection.Nodes[0].Status != orchestration.NodeInterrupted || projection.ProviderThreads[0].Status != orchestration.ProviderThreadIdle {
				t.Fatalf("interrupt status mismatch: run=%s attempt=%s node=%s providerThread=%s",
					projection.Runs[0].Status, projection.Attempts[0].Status, projection.Nodes[0].Status, projection.ProviderThreads[0].Status)
			}
			var requestItem, resultItem *orchestration.TurnItem
			for index := range projection.TurnItems {
				item := &projection.TurnItems[index]
				switch item.Type {
				case "run_interrupt_request":
					requestItem = item
				case "run_interrupt_result":
					resultItem = item
				}
			}
			if requestItem == nil || resultItem == nil || requestItem.Status != orchestration.TurnItemCompleted ||
				resultItem.Status != orchestration.TurnItemInterrupted || resultItem.ParentItemID == nil || *resultItem.ParentItemID != requestItem.ID {
				t.Fatalf("interrupt items mismatch: request=%+v result=%+v", requestItem, resultItem)
			}
			assertSemanticProjectionIntegrity(t, projection)
		})
	}
}

func dispatch(t *testing.T, o *orchestration.Orchestrator, command orchestration.Command) orchestration.DispatchResult {
	t.Helper()
	result, err := o.Dispatch(context.Background(), command)
	if err != nil {
		t.Fatalf("dispatch %s: %v", command.Type, err)
	}
	return result
}

func assertSimpleProjectionParity(t *testing.T, o *orchestration.Orchestrator, store orchestration.Store, threadID orchestration.ThreadID) {
	t.Helper()
	snapshot, err := o.Snapshot(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	projection := snapshot.Projection
	if len(projection.Runs) != 1 || projection.Runs[0].Ordinal != 1 || projection.Runs[0].Status != orchestration.RunCompleted {
		t.Fatalf("run mismatch: %+v", projection.Runs)
	}
	if len(projection.ProviderThreads) < 1 || len(projection.ProviderTurns) < 1 {
		t.Fatalf("provider graph missing: threads=%d turns=%d", len(projection.ProviderThreads), len(projection.ProviderTurns))
	}
	assertNodeKindPresent(t, projection.Nodes, orchestration.NodeRootTurn)
	assertNodeKindPresent(t, projection.Nodes, orchestration.NodeAssistantMessage)
	assertMessageRolePresent(t, projection.Messages, orchestration.MessageUser)
	assertMessageRolePresent(t, projection.Messages, orchestration.MessageAssistant)
	assertTurnItemTypePresent(t, projection.TurnItems, "user_message")
	assertTurnItemTypePresent(t, projection.TurnItems, "assistant_message")
	assertAssistantText(t, projection.Messages, "fixture simple ok")
	assertSemanticProjectionIntegrity(t, projection)

	events, err := store.Read(context.Background(), orchestration.EventFilter{ThreadID: &threadID})
	if err != nil {
		t.Fatal(err)
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("sequence %d is %d", index, event.Sequence)
		}
	}
	replayed, err := orchestration.Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	replaySnapshot := replayed[threadID]
	if len(replaySnapshot.Runs) != 1 || replaySnapshot.Runs[0].Status != orchestration.RunCompleted {
		t.Fatalf("replay mismatch: %+v", replaySnapshot.Runs)
	}
}

func assertNodeKindPresent(t *testing.T, nodes []orchestration.ExecutionNode, kind orchestration.NodeKind) {
	t.Helper()
	for _, node := range nodes {
		if node.Kind == kind {
			return
		}
	}
	t.Errorf("missing execution node kind %s", kind)
}

func assertMessageRolePresent(t *testing.T, messages []orchestration.ConversationMessage, role orchestration.MessageRole) {
	t.Helper()
	for _, message := range messages {
		if message.Role == role {
			return
		}
	}
	t.Errorf("missing message role %s", role)
}

func assertTurnItemTypePresent(t *testing.T, items []orchestration.TurnItem, itemType string) {
	t.Helper()
	for _, item := range items {
		if item.Type == itemType {
			return
		}
	}
	t.Errorf("missing turn item type %s", itemType)
}

func assertAssistantText(t *testing.T, messages []orchestration.ConversationMessage, expected string) {
	t.Helper()
	for _, message := range messages {
		if message.Role == orchestration.MessageAssistant && message.Text == expected && !message.Streaming {
			return
		}
	}
	t.Errorf("missing settled assistant text %q", expected)
}

func assertPiSimpleDetails(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID) {
	t.Helper()
	snapshot, err := o.Snapshot(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	const reasoning = "The user wants me to respond with a specific text: \"fixture simple ok\". Let me do that."
	foundReasoning := false
	for _, item := range snapshot.Projection.TurnItems {
		if item.Type == "reasoning" && item.Text == reasoning {
			foundReasoning = true
		}
	}
	if !foundReasoning {
		t.Errorf("Pi reasoning did not match the recorded thinking_end content: %+v", snapshot.Projection.TurnItems)
	}
	turn := snapshot.Projection.ProviderTurns[0]
	if turn.NativeTurnRef == nil || turn.NativeTurnRef.NativeID == nil || *turn.NativeTurnRef.NativeID != "0b4c51f4" {
		t.Errorf("Pi native turn ref does not use the recorded user entry: %+v", turn.NativeTurnRef)
	}
	if turn.TokenUsage == nil || turn.TokenUsage.UsedTokens != 1814 || turn.TokenUsage.MaxTokens == nil ||
		*turn.TokenUsage.MaxTokens != 1048576 || turn.TokenUsage.OutputTokens != 28 {
		t.Errorf("Pi settled usage mismatch: %+v", turn.TokenUsage)
	}
}

func assertSemanticProjectionIntegrity(t *testing.T, projection orchestration.Projection) {
	t.Helper()
	rootCount := 0
	for _, run := range projection.Runs {
		if run.RootNodeID == nil {
			t.Fatalf("run %s has no root", run.ID)
		}
		root, ok := projection.Node(*run.RootNodeID)
		if !ok || root.Kind != orchestration.NodeRootTurn || !root.CountsForRun {
			t.Fatalf("run %s root mismatch: %+v", run.ID, root)
		}
	}
	for _, node := range projection.Nodes {
		if node.Kind == orchestration.NodeRootTurn {
			rootCount++
			if !node.CountsForRun {
				t.Fatalf("root %s does not count for run", node.ID)
			}
		}
	}
	if rootCount < len(projection.Runs) {
		t.Fatalf("missing roots: %d < %d", rootCount, len(projection.Runs))
	}
	for _, turn := range projection.ProviderTurns {
		if _, ok := projection.Node(turn.NodeID); !ok {
			t.Errorf("provider turn %s references missing node %s", turn.ID, turn.NodeID)
		}
	}
	previousOrdinal := 0
	for _, item := range projection.TurnItems {
		if item.Ordinal <= previousOrdinal {
			t.Errorf("turn items are not strictly ordered: %+v", projection.TurnItems)
			break
		}
		previousOrdinal = item.Ordinal
		if item.NodeID != nil {
			if _, ok := projection.Node(*item.NodeID); !ok {
				t.Errorf("item %s references missing node %s", item.ID, *item.NodeID)
			}
		}
		if item.ProviderTurnID != nil {
			if _, ok := projection.ProviderTurn(*item.ProviderTurnID); !ok {
				t.Errorf("item %s references missing provider turn %s", item.ID, *item.ProviderTurnID)
			}
		}
	}
	for _, message := range projection.Messages {
		if _, ok := projection.Run(*message.RunID); !ok {
			t.Errorf("message %s references missing run", message.ID)
		}
		if message.NodeID != nil {
			if _, ok := projection.Node(*message.NodeID); !ok {
				t.Errorf("message %s references missing node", message.ID)
			}
		}
	}
}
