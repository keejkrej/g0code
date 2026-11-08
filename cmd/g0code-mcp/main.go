package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/keejkrej/g0code/orchestration"
	"github.com/keejkrej/g0code/orchestration/cli"
	orchestratormcp "github.com/keejkrej/g0code/orchestration/mcp"
	"github.com/keejkrej/g0code/orchestration/sqlite"
)

func main() {
	workspaceDefault, _ := os.Getwd()
	workspace := flag.String("workspace", workspaceDefault, "workspace inherited by new threads")
	project := flag.String("project", "local", "project ID inherited by new threads")
	provider := flag.String("provider", "codex", "default provider: codex, claude, pi, agy, or fx")
	model := flag.String("model", "default", "default model; default uses the CLI's configured model")
	defaultState := ":memory:"
	if home, err := os.UserHomeDir(); err == nil {
		defaultState = filepath.Join(home, ".g0code", "orchestration.sqlite")
	}
	statePath := flag.String("state", defaultState, "SQLite state path, or :memory: for ephemeral state")
	parentThread := flag.String("parent-thread", "", "optional parent thread used to scope delegated tasks")
	flag.Parse()
	log.SetOutput(os.Stderr)

	store, err := durableStore(*statePath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	o := orchestration.New(store)
	executor := cli.NewExecutor(o)
	executor.RegisterDefaults()
	defer executor.Close()
	providers := []orchestratormcp.Provider{
		{InstanceID: "codex", Driver: "codex", Available: true},
		{InstanceID: "claude", Driver: "claudeAgent", Available: true},
		{InstanceID: "pi", Driver: "pi", Available: true},
		{InstanceID: "agy", Driver: "agy", Available: true},
		{InstanceID: "fx", Driver: "fx", Available: true},
	}
	server := orchestratormcp.NewServer(o, nil, orchestratormcp.Config{
		Providers: providers, DefaultProjectID: orchestration.ProjectID(*project), DefaultWorktreePath: workspace,
		DefaultTarget:  orchestration.ModelSelection{InstanceID: orchestration.ProviderInstanceID(*provider), Model: *model},
		ParentThreadID: orchestration.ThreadID(*parentThread),
		OnEffectError: func(effect orchestration.Effect, err error) {
			log.Printf("effect %s (%s) failed: %v", effect.ID, effect.Kind, err)
		},
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("g0code MCP listening on stdio; provider=%s workspace=%s state=%s", *provider, *workspace, *statePath)
	hostname, _ := os.Hostname()
	worker := &orchestration.EffectWorker{Store: store, Executor: executor,
		WorkerID: fmt.Sprintf("%s-%d", hostname, os.Getpid()), Lease: time.Minute, Poll: 50 * time.Millisecond}
	go func() {
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("effect worker failed: %v", err)
		}
	}()
	if err := server.Serve(ctx, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func durableStore(path string) (*sqlite.Store, error) {
	if path == ":memory:" {
		return sqlite.Open(path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	return sqlite.Open(path)
}
