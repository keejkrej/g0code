package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
	"github.com/keejkrej/g0code/desktop"
	"github.com/keejkrej/g0code/orchestration"
	"github.com/keejkrej/g0code/orchestration/cli"
	"github.com/keejkrej/g0code/orchestration/sqlite"
)

func main() {
	cwd, _ := os.Getwd()
	workspace := flag.String("workspace", cwd, "workspace used by new agent threads")
	provider := flag.String("provider", "codex", "default provider: codex, claude, pi, agy, or fx")
	stateDefault := ""
	if home, err := os.UserHomeDir(); err == nil {
		stateDefault = filepath.Join(home, ".g0code", "orchestration.sqlite")
	}
	statePath := flag.String("state", stateDefault, "SQLite state path, or :memory:")
	flag.Parse()
	log.SetOutput(os.Stderr)

	store, err := openStore(*statePath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	o := orchestration.New(store)
	executor := cli.NewExecutor(o)
	executor.RegisterDefaults()
	defer executor.Close()
	application := desktop.New(o, executor, desktop.Config{ProjectID: "local", WorktreePath: workspace,
		Provider: orchestration.ProviderInstanceID(*provider), Outbox: store})
	if err := application.Refresh(context.Background()); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hostname, _ := os.Hostname()
	worker := &orchestration.EffectWorker{Store: store, Executor: executor,
		WorkerID: fmt.Sprintf("%s-%d", hostname, os.Getpid()), Lease: time.Minute, Poll: 50 * time.Millisecond}
	go func() {
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("effect worker failed: %v", err)
		}
	}()

	mygo.App.WhenReady(func() {
		window := mygo.NewWindow(mygo.WindowOptions{Title: "g0code", Width: 1200, Height: 760,
			Content: ui.View(application.View)})
		go func() {
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					window.Update(func() { _ = application.Refresh(ctx) })
				}
			}
		}()
	})
	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}

func openStore(path string) (*sqlite.Store, error) {
	if path == ":memory:" {
		return sqlite.Open(path)
	}
	if path == "" {
		return nil, fmt.Errorf("state path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	return sqlite.Open(path)
}
