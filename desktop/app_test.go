package desktop

import (
	"context"
	"fmt"
	"image/png"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
	"github.com/keejkrej/g0code/orchestration"
)

type fakeExecutor struct {
	orchestrator *orchestration.Orchestrator
	block        atomic.Bool
	release      chan struct{}
	raw          atomic.Int64
}

func (f *fakeExecutor) Execute(ctx context.Context, effect orchestration.Effect) error {
	if effect.Kind != orchestration.EffectProviderTurnStart {
		return nil
	}
	start := effect.Payload.(orchestration.TurnStartEffect)
	if f.block.CompareAndSwap(true, false) {
		<-f.release
	}
	turn := orchestration.ProviderTurn{ID: orchestration.ProviderTurnID(fmt.Sprintf("turn-%d", f.raw.Add(1))),
		ProviderThreadID: *start.Run.ProviderThreadID, NodeID: *start.Run.RootNodeID,
		RunAttemptID: start.Run.ActiveAttemptID, Ordinal: 1, Status: orchestration.ProviderTurnRunning}
	if _, err := f.orchestrator.Ingest(ctx, orchestration.IngestInput{ThreadID: start.Thread.ID, RunID: &start.Run.ID,
		ProviderInstanceID: start.Run.ProviderInstanceID, Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}}); err != nil {
		return err
	}
	messageID := orchestration.MessageID("assistant-" + string(turn.ID))
	message := orchestration.ConversationMessage{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
		ID: messageID, ThreadID: start.Thread.ID, RunID: &start.Run.ID, NodeID: start.Run.RootNodeID,
		Role: orchestration.MessageAssistant, Text: "GUI_OK", Attachments: []orchestration.Attachment{}}
	if _, err := f.orchestrator.Ingest(ctx, orchestration.IngestInput{ThreadID: start.Thread.ID, RunID: &start.Run.ID,
		ProviderInstanceID: start.Run.ProviderInstanceID, Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventMessageUpdated, Message: &message}}); err != nil {
		return err
	}
	item := orchestration.TurnItem{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
		ID: orchestration.TurnItemID("item-" + string(messageID)), ThreadID: start.Thread.ID, RunID: &start.Run.ID,
		NodeID: start.Run.RootNodeID, ProviderTurnID: &turn.ID, Status: orchestration.TurnItemCompleted,
		Type: "assistant_message", MessageID: messageID, Text: "GUI_OK", UpdatedAt: time.Now().UTC()}
	if _, err := f.orchestrator.Ingest(ctx, orchestration.IngestInput{ThreadID: start.Thread.ID, RunID: &start.Run.ID,
		ProviderInstanceID: start.Run.ProviderInstanceID, Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventTurnItemUpdated, TurnItem: &item}}); err != nil {
		return err
	}
	turn.Status = orchestration.ProviderTurnCompleted
	_, err := f.orchestrator.Ingest(ctx, orchestration.IngestInput{ThreadID: start.Thread.ID, RunID: &start.Run.ID,
		ProviderInstanceID: start.Run.ProviderInstanceID, Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}})
	return err
}

func TestDesktopThreadPromptAndApproval(t *testing.T) {
	ctx := context.Background()
	store := orchestration.NewMemoryStore()
	o := orchestration.New(store)
	executor := &fakeExecutor{orchestrator: o, release: make(chan struct{})}
	workspace := t.TempDir()
	app := New(o, executor, Config{ProjectID: "local", WorktreePath: &workspace, Provider: "codex"})
	view := ui.NewTester(app.View, 1200, 760)
	if !view.HasText("Work with your agents") {
		t.Fatalf("empty state missing: %q", view.Texts())
	}
	if err := view.Click("New thread"); err != nil {
		t.Fatal(err)
	}
	if app.SelectedID == "" {
		t.Fatal("new thread was not selected")
	}
	app.Draft = "Run the GUI test"
	if err := view.Click("↑  Send"); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, o, app.SelectedID, orchestration.RunCompleted)
	if err := app.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	view.Frame()
	if !view.HasText("GUI_OK") {
		t.Fatalf("assistant response missing: %q", view.Texts())
	}
	if path := os.Getenv("G0CODE_CAPTURE"); path != "" {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := png.Encode(file, view.Image()); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	snapshot, _ := o.Snapshot(ctx, app.SelectedID)
	request := orchestration.RuntimeRequest{ID: "desktop-request", NodeID: *snapshot.Projection.Runs[0].RootNodeID,
		Kind: orchestration.RequestCommand, Status: orchestration.RequestPending,
		ResponseCapability: orchestration.ResponseCapability{Type: "live", ProviderSessionID: refDesktop(orchestration.ProviderSessionID("session"))},
		CreatedAt:          time.Now().UTC()}
	if _, err := o.Ingest(ctx, orchestration.IngestInput{ThreadID: app.SelectedID, ProviderInstanceID: "codex",
		Event: orchestration.ProviderEvent{Type: orchestration.ProviderEventRuntimeRequestUpdated, RuntimeRequest: &request}}); err != nil {
		t.Fatal(err)
	}
	_ = app.Refresh(ctx)
	view.Frame()
	if !view.HasText("Approval needed") {
		t.Fatalf("pending request UI missing: %q", view.Texts())
	}
	if err := view.Click("Allow"); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = o.Snapshot(ctx, app.SelectedID)
	resolved, _ := snapshot.Projection.RuntimeRequest(request.ID)
	if resolved.Status != orchestration.RequestResolved {
		t.Fatalf("request not resolved: %+v", resolved)
	}
}

func TestDesktopThreePaneToolDetails(t *testing.T) {
	ctx := context.Background()
	o := orchestration.New(nil)
	executor := &fakeExecutor{orchestrator: o, release: make(chan struct{})}
	workspace := t.TempDir()
	app := New(o, executor, Config{ProjectID: "local", WorktreePath: &workspace, Provider: "codex"})
	view := ui.NewTester(app.View, 1200, 760)
	if _, err := app.NewThread(ctx); err != nil {
		t.Fatal(err)
	}
	app.Draft = "Edit a file"
	view.Frame()
	if err := view.Click("↑  Send"); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, o, app.SelectedID, orchestration.RunCompleted)
	snapshot, _ := o.Snapshot(ctx, app.SelectedID)
	item := orchestration.TurnItem{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
		ID: "tool-file-1", ThreadID: app.SelectedID, RunID: &snapshot.Projection.Runs[0].ID,
		NodeID: snapshot.Projection.Runs[0].RootNodeID, ProviderTurnID: &snapshot.Projection.ProviderTurns[0].ID,
		Status: orchestration.TurnItemCompleted, Type: "file_change", Title: "result.txt",
		Data: map[string]any{"fileName": "result.txt", "diffStr": "+GUI_OK\n"}, UpdatedAt: time.Now().UTC()}
	if _, err := o.Ingest(ctx, orchestration.IngestInput{ThreadID: app.SelectedID, ProviderInstanceID: "codex",
		Event: orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated, TurnItem: &item}}); err != nil {
		t.Fatal(err)
	}
	app.SelectItem(item.ID)
	if err := app.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	view.Frame()
	if !view.HasText("1 changed file") || !view.HasText("result.txt") || !view.HasText("+GUI_OK") {
		t.Fatalf("three-pane file details missing: %q", view.Texts())
	}
	if path := os.Getenv("G0CODE_CAPTURE_DETAILS"); path != "" {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := png.Encode(file, view.Image()); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
}

func TestDesktopDelegationPublishesChildResult(t *testing.T) {
	ctx := context.Background()
	o := orchestration.New(nil)
	executor := &fakeExecutor{orchestrator: o, release: make(chan struct{})}
	executor.block.Store(true)
	workspace := t.TempDir()
	app := New(o, executor, Config{ProjectID: "local", WorktreePath: &workspace, Provider: "codex"})
	view := ui.NewTester(app.View, 1200, 760)
	if _, err := app.NewThread(ctx); err != nil {
		t.Fatal(err)
	}
	app.Draft = "parent task"
	if err := app.Send(ctx); err != nil {
		t.Fatal(err)
	}
	app.Draft = "child task"
	if err := app.Delegate(ctx); err != nil {
		t.Fatal(err)
	}
	if len(app.Selected.Projection.Subagents) != 1 {
		t.Fatalf("subagent missing: %+v", app.Selected.Projection.Subagents)
	}
	waitForSubagentStatus(t, o, app.SelectedID, orchestration.NodeCompleted)
	close(executor.release)
	waitForRunStatus(t, o, app.SelectedID, orchestration.RunCompleted)
	if err := app.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	view.Frame()
	if !view.HasText("subagent") || !view.HasText("GUI_OK") {
		t.Fatalf("delegated result missing: %q", view.Texts())
	}
}

func waitForRunStatus(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID, status orchestration.RunStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := o.Snapshot(context.Background(), threadID)
		if err == nil && len(snapshot.Projection.Runs) > 0 && snapshot.Projection.Runs[len(snapshot.Projection.Runs)-1].Status == status {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run did not reach %s", status)
}

func waitForSubagentStatus(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID, status orchestration.NodeStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, _ := o.Snapshot(context.Background(), threadID)
		if len(snapshot.Projection.Subagents) > 0 && snapshot.Projection.Subagents[0].Status == status {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("subagent did not reach %s", status)
}

func refDesktop[T any](value T) *T { return &value }
