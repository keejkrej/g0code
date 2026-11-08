//go:build live

package desktop

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/egoist/mygo/ui"
	"github.com/keejkrej/g0code/orchestration"
	"github.com/keejkrej/g0code/orchestration/cli"
)

func TestLiveDesktopDelegatedWorkspaceEdit(t *testing.T) {
	if os.Getenv("G0CODE_LIVE") != "1" {
		t.Skip("set G0CODE_LIVE=1 for the real delegated desktop workflow")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	o := orchestration.New(nil)
	executor := cli.NewExecutor(o)
	executor.RegisterDefaults()
	defer executor.Close()
	workspace := t.TempDir()
	app := New(o, executor, Config{ProjectID: "live", WorktreePath: &workspace, Provider: "codex"})
	view := ui.NewTester(app.View, 1200, 760)
	if _, err := app.NewThread(ctx); err != nil {
		t.Fatal(err)
	}
	app.Draft = "Use a local shell command to run `sleep 60`, then report when it exits. Do not use network or MCP."
	view.Frame()
	if err := view.Click("Send"); err != nil {
		t.Fatal(err)
	}
	approvePending(t, ctx, app)
	app.Draft = "Create child-result.txt containing exactly G0CODE_CHILD_OK using a local file edit, then read it with a local shell command. Do not use network or MCP."
	view.Frame()
	if err := view.Click("Delegate"); err != nil {
		t.Fatal(err)
	}
	approvePending(t, ctx, app)
	childID := *app.Selected.Projection.Subagents[0].ChildThreadID
	waitForRunStatusWithin(t, o, childID, orchestration.RunCompleted, 2*time.Minute)
	content, err := os.ReadFile(filepath.Join(workspace, "child-result.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(content)) != "G0CODE_CHILD_OK" {
		t.Fatalf("child-result.txt = %q", content)
	}
	if err := app.Interrupt(ctx); err != nil {
		t.Fatal(err)
	}
	waitForRunStatusWithin(t, o, app.SelectedID, orchestration.RunInterrupted, 2*time.Minute)
	if app.Selected.Projection.Subagents[0].Result == nil {
		t.Fatal("child result was not published to the parent")
	}
}

func waitForRunStatusWithin(t *testing.T, o *orchestration.Orchestrator, threadID orchestration.ThreadID, status orchestration.RunStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, err := o.Snapshot(context.Background(), threadID)
		if err == nil && len(snapshot.Projection.Runs) > 0 && snapshot.Projection.Runs[len(snapshot.Projection.Runs)-1].Status == status {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, _ := o.Snapshot(context.Background(), threadID)
	t.Fatalf("run did not reach %s: %+v", status, snapshot.Projection.Runs)
}

func approvePending(t *testing.T, ctx context.Context, app *App) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		snapshots, err := app.orchestrator.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		needsApproval := false
		for _, snapshot := range snapshots {
			for _, request := range snapshot.Projection.RuntimeRequests {
				if request.Status == orchestration.RequestPending {
					needsApproval = true
					if err := app.RespondOnThread(ctx, snapshot.Projection.Thread.ID, request.ID, "accept"); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		if !needsApproval && len(app.Selected.Projection.Runs) > 0 && app.Selected.Projection.Runs[0].Status.Active() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	t.Fatal("provider approval did not arrive")
}
