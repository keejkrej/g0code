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

func TestLiveDesktopWorkspaceEdit(t *testing.T) {
	if os.Getenv("G0CODE_LIVE") != "1" {
		t.Skip("set G0CODE_LIVE=1 for the real desktop/provider workflow")
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
	app.Draft = "Create result.txt containing exactly G0CODE_DESKTOP_OK, then read it with a local shell command. Do not use network or MCP."
	view.Frame()
	if err := view.Click("Send"); err != nil {
		t.Fatal(err)
	}
	for {
		snapshot, err := o.Snapshot(ctx, app.SelectedID)
		if err != nil {
			t.Fatal(err)
		}
		for _, request := range snapshot.Projection.RuntimeRequests {
			if request.Status == orchestration.RequestPending {
				if err := app.Respond(ctx, request.ID, "accept"); err != nil {
					t.Fatal(err)
				}
			}
		}
		run := snapshot.Projection.Runs[0]
		if run.Status.Terminal() {
			if run.Status != orchestration.RunCompleted {
				t.Fatalf("desktop run failed: %+v", run)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	content, err := os.ReadFile(filepath.Join(workspace, "result.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(content)) != "G0CODE_DESKTOP_OK" {
		t.Fatalf("result.txt = %q", content)
	}
	if err := app.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	view.Frame()
	if !view.HasText("completed") {
		t.Fatalf("completed status missing: %q", view.Texts())
	}
}
