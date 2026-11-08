package desktop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/keejkrej/g0code/orchestration"
	"github.com/keejkrej/g0code/orchestration/cli"
)

type EffectExecutor interface {
	Execute(context.Context, orchestration.Effect) error
}

type Config struct {
	ProjectID    orchestration.ProjectID
	WorktreePath *string
	Provider     orchestration.ProviderInstanceID
	Model        string
	Outbox       orchestration.OutboxStore
	Catalog      map[orchestration.ProviderInstanceID][]cli.Model
}

type ThreadSummary struct {
	ID        orchestration.ThreadID
	Title     string
	Status    string
	Provider  orchestration.ProviderInstanceID
	UpdatedAt time.Time
}

type selectedSnapshot struct {
	orchestration.ThreadSnapshot
	Thread orchestration.Thread
}

type App struct {
	orchestrator *orchestration.Orchestrator
	effects      EffectExecutor
	outbox       orchestration.OutboxStore
	config       Config

	mu         sync.RWMutex
	Threads    []ThreadSummary
	SelectedID orchestration.ThreadID
	Selected   selectedSnapshot
	Children   []orchestration.ThreadSnapshot
	Error      string

	Draft    string
	Provider orchestration.ProviderInstanceID
	Model    string
	Options  map[string]any
	Mode     orchestration.RuntimeMode
	Search   string

	SelectedItemID orchestration.TurnItemID
	RightOpen      bool
	RightSplit     float32
	Catalog        map[orchestration.ProviderInstanceID][]cli.Model
	catalogTried   bool
}

func New(o *orchestration.Orchestrator, effects EffectExecutor, config Config) *App {
	outbox := config.Outbox
	return &App{orchestrator: o, effects: effects, outbox: outbox, config: config,
		Provider: config.Provider, Model: config.Model, Options: make(map[string]any), Mode: orchestration.RuntimeAutoAcceptEdits,
		RightSplit: 60, Catalog: config.Catalog}
}

func (a *App) Refresh(ctx context.Context) error {
	_ = a.ensureCatalog(ctx, a.Provider)
	snapshots, err := a.orchestrator.List(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Threads = a.Threads[:0]
	for _, snapshot := range snapshots {
		thread := snapshot.Projection.Thread
		if thread.DeletedAt != nil || (thread.Lineage.RelationshipToParent != nil && *thread.Lineage.RelationshipToParent == "subagent") {
			continue
		}
		status := "idle"
		if len(snapshot.Projection.Runs) > 0 {
			status = string(snapshot.Projection.Runs[len(snapshot.Projection.Runs)-1].Status)
		}
		a.Threads = append(a.Threads, ThreadSummary{ID: thread.ID, Title: thread.Title, Status: status,
			Provider: thread.ProviderInstanceID, UpdatedAt: thread.UpdatedAt})
	}
	sortThreads(a.Threads)
	if a.SelectedID == "" && len(a.Threads) > 0 {
		a.SelectedID = a.Threads[0].ID
	}
	if a.SelectedID != "" {
		snapshot, err := a.orchestrator.Snapshot(ctx, a.SelectedID)
		if err != nil {
			a.Selected = selectedSnapshot{}
			return err
		}
		a.Selected = selectedSnapshot{ThreadSnapshot: snapshot, Thread: snapshot.Projection.Thread}
		a.Children = a.Children[:0]
		for _, task := range snapshot.Projection.Subagents {
			if task.ChildThreadID == nil {
				continue
			}
			child, err := a.orchestrator.Snapshot(ctx, *task.ChildThreadID)
			if err != nil {
				return err
			}
			a.Children = append(a.Children, child)
		}
		a.Provider = snapshot.Projection.Thread.ProviderInstanceID
		a.Model = snapshot.Projection.Thread.ModelSelection.Model
		a.Options = cloneOptions(snapshot.Projection.Thread.ModelSelection.Options)
		a.Mode = snapshot.Projection.Thread.RuntimeMode
	}
	return nil
}

func sortThreads(threads []ThreadSummary) {
	for i := 0; i < len(threads); i++ {
		for j := i + 1; j < len(threads); j++ {
			if threads[j].UpdatedAt.After(threads[i].UpdatedAt) {
				threads[i], threads[j] = threads[j], threads[i]
			}
		}
	}
}

func (a *App) SelectItem(id orchestration.TurnItemID) {
	a.mu.Lock()
	a.SelectedItemID = id
	a.RightOpen = true
	a.mu.Unlock()
}

func (a *App) CloseDetails() {
	a.mu.Lock()
	a.RightOpen = false
	a.mu.Unlock()
}

func (a *App) NewThread(ctx context.Context) (orchestration.ThreadID, error) {
	_ = a.ensureCatalog(ctx, a.Provider)
	id := orchestration.ThreadID(newID("thread"))
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandThreadCreate,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: id,
		Payload: orchestration.CreateThread{ProjectID: a.config.ProjectID, Title: "New thread",
			ModelSelection: a.modelSelection(),
			RuntimeMode:    a.Mode, InteractionMode: orchestration.InteractionDefault,
			WorktreePath: a.config.WorktreePath, CreatedBy: orchestration.ActorUser, CreationSource: orchestration.CreationWeb}})
	if err != nil {
		return "", err
	}
	a.finishEffects(result.Effects)
	a.mu.Lock()
	a.SelectedID = id
	a.Error = ""
	a.mu.Unlock()
	return id, a.Refresh(ctx)
}

func (a *App) SelectThread(ctx context.Context, id orchestration.ThreadID) error {
	a.mu.Lock()
	a.SelectedID = id
	a.mu.Unlock()
	return a.Refresh(ctx)
}

func (a *App) Send(ctx context.Context) error {
	text := strings.TrimSpace(a.Draft)
	if text == "" || a.SelectedID == "" {
		return nil
	}
	messageID := orchestration.MessageID(newID("message"))
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandMessageDispatch,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: a.SelectedID,
		Payload: orchestration.DispatchMessage{MessageID: messageID, Text: text, TitleSeed: text,
			Attachments: []orchestration.Attachment{}, DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately},
			CreatedBy: orchestration.ActorUser, CreationSource: orchestration.CreationWeb}})
	if err != nil {
		return a.setError(err)
	}
	a.Draft = ""
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) Delegate(ctx context.Context) error {
	text := strings.TrimSpace(a.Draft)
	if text == "" || a.Selected.Thread.ID == "" {
		return nil
	}
	run, ok := a.Selected.Projection.ActiveRun()
	if !ok || run.RootNodeID == nil {
		return a.setErrorString("Delegation needs an active parent run.")
	}
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandDelegatedTaskRequest,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: a.Selected.Thread.ID,
		Payload: orchestration.DelegateTask{ParentThreadID: a.Selected.Thread.ID, ParentRunID: run.ID,
			ParentNodeID: *run.RootNodeID, Task: text, ModelSelection: a.Selected.Thread.ModelSelection,
			RuntimeMode: a.Selected.Thread.RuntimeMode, InteractionMode: a.Selected.Thread.InteractionMode,
			CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationMCP}})
	if err != nil {
		return a.setError(err)
	}
	a.Draft = ""
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) Interrupt(ctx context.Context) error {
	run, ok := a.Selected.Projection.ActiveRun()
	if !ok {
		return nil
	}
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandRunInterrupt,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: a.Selected.Thread.ID,
		Payload: orchestration.InterruptRun{RunID: run.ID, Reason: "Stopped in g0code", HoldQueue: true}})
	if err != nil {
		return a.setError(err)
	}
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) Respond(ctx context.Context, requestID orchestration.RuntimeRequestID, decision string) error {
	return a.RespondOnThread(ctx, a.Selected.Thread.ID, requestID, decision)
}

func (a *App) RespondOnThread(ctx context.Context, threadID orchestration.ThreadID, requestID orchestration.RuntimeRequestID, decision string) error {
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandRuntimeRequestRespond,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: threadID,
		Payload: orchestration.RuntimeRequestResponse{RequestID: requestID, Decision: &decision}})
	if err != nil {
		return a.setError(err)
	}
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) SetProvider(ctx context.Context, provider orchestration.ProviderInstanceID) error {
	if a.Selected.Thread.ID == "" {
		a.Provider = provider
		return nil
	}
	previous := a.Provider
	a.Provider = provider
	if previous != provider {
		a.Catalog = nil
		a.catalogTried = false
		if err := a.ensureCatalog(ctx, provider); err == nil {
			a.Model = defaultModel(a.Catalog[provider]).ID
			a.Options = make(map[string]any)
		}
	}
	if a.Selected.Thread.ModelSelection.Equal(a.modelSelection()) {
		return nil
	}
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandProviderSwitch,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: a.Selected.Thread.ID,
		Payload: orchestration.ProviderSwitch{ModelSelection: a.modelSelection()}})
	if err != nil {
		return a.setError(err)
	}
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) SetModel(ctx context.Context, model string) error {
	if model == "" {
		return nil
	}
	a.Model = model
	if a.Selected.Thread.ID == "" {
		return nil
	}
	if a.Selected.Thread.ModelSelection.Equal(a.modelSelection()) {
		return nil
	}
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandProviderSwitch,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: a.Selected.Thread.ID,
		Payload: orchestration.ProviderSwitch{ModelSelection: a.modelSelection()}})
	if err != nil {
		return a.setError(err)
	}
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) SetOption(ctx context.Context, id string, value any) error {
	if a.Options == nil {
		a.Options = make(map[string]any)
	}
	a.Options[id] = value
	if a.Selected.Thread.ID == "" {
		return nil
	}
	if a.Selected.Thread.ModelSelection.Equal(a.modelSelection()) {
		return nil
	}
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandProviderSwitch,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: a.Selected.Thread.ID,
		Payload: orchestration.ProviderSwitch{ModelSelection: a.modelSelection()}})
	if err != nil {
		return a.setError(err)
	}
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) SetMode(ctx context.Context, mode orchestration.RuntimeMode) error {
	if a.Selected.Thread.ID == "" {
		a.Mode = mode
		return nil
	}
	result, err := a.orchestrator.Dispatch(ctx, orchestration.Command{Type: orchestration.CommandThreadRuntimeModeSet,
		CommandID: orchestration.CommandID(newID("command")), ThreadID: a.Selected.Thread.ID,
		Payload: orchestration.RuntimeModeChange{RuntimeMode: mode}})
	if err != nil {
		return a.setError(err)
	}
	a.finishEffects(result.Effects)
	return a.Refresh(ctx)
}

func (a *App) finishEffects(effects []orchestration.Effect) {
	if a.outbox != nil {
		return
	}
	for _, effect := range effects {
		go func(effect orchestration.Effect) {
			if err := a.effects.Execute(context.Background(), effect); err != nil {
				_ = a.setError(err)
			}
		}(effect)
	}
}

func (a *App) setError(err error) error {
	a.mu.Lock()
	a.Error = err.Error()
	a.mu.Unlock()
	return err
}

func (a *App) setErrorString(message string) error {
	a.mu.Lock()
	a.Error = message
	a.mu.Unlock()
	return nil
}

func (a *App) modelSelection() orchestration.ModelSelection {
	model := a.Model
	if model == "" {
		model = defaultModel(a.Catalog[a.Provider]).ID
	}
	return orchestration.ModelSelection{InstanceID: a.Provider, Model: model, Options: cloneOptions(a.Options)}
}

func cloneOptions(options map[string]any) map[string]any {
	if len(options) == 0 {
		return nil
	}
	clone := make(map[string]any, len(options))
	for key, value := range options {
		clone[key] = value
	}
	return clone
}

func defaultModel(models []cli.Model) cli.Model {
	for _, model := range models {
		if model.ID == "default" {
			return model
		}
	}
	if len(models) > 0 {
		return models[0]
	}
	return cli.Model{ID: "default", Label: "Default"}
}

func (a *App) ensureCatalog(ctx context.Context, provider orchestration.ProviderInstanceID) error {
	if a.Catalog != nil {
		return nil
	}
	if a.config.Catalog != nil {
		a.Catalog = a.config.Catalog
		if a.Model == "" || a.Model == "default" {
			a.Model = defaultModel(a.Catalog[a.Provider]).ID
		}
		return nil
	}
	if a.catalogTried {
		return nil
	}
	a.catalogTried = true
	executor, ok := a.effects.(interface {
		Models(context.Context, orchestration.ProviderInstanceID, string) ([]cli.Model, error)
	})
	if !ok {
		return nil
	}
	models, err := executor.Models(ctx, provider, *a.config.WorktreePath)
	if err != nil {
		return err
	}
	a.Catalog = map[orchestration.ProviderInstanceID][]cli.Model{provider: models}
	if a.Model == "" || a.Model == "default" {
		a.Model = defaultModel(models).ID
	}
	return nil
}

func newID(prefix string) string {
	var value [16]byte
	_, _ = rand.Read(value[:])
	return prefix + ":" + hex.EncodeToString(value[:])
}
