package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ProviderInfo struct {
	Driver       ProviderDriverKind
	Capabilities ProviderCapabilities
}

type ProviderResolver interface {
	Resolve(context.Context, ProviderInstanceID) (ProviderInfo, error)
}

type ProviderResolverFunc func(context.Context, ProviderInstanceID) (ProviderInfo, error)

func (f ProviderResolverFunc) Resolve(ctx context.Context, id ProviderInstanceID) (ProviderInfo, error) {
	return f(ctx, id)
}

type Option func(*Orchestrator)

func WithClock(now func() time.Time) Option {
	return func(o *Orchestrator) { o.now = now }
}

func WithIDGenerator(generator func(prefix string, parts ...any) (string, error)) Option {
	return func(o *Orchestrator) { o.randomID = generator }
}

func WithProviderResolver(resolver ProviderResolver) Option {
	return func(o *Orchestrator) { o.providers = resolver }
}

type Orchestrator struct {
	store     Store
	now       func() time.Time
	randomID  func(prefix string, parts ...any) (string, error)
	providers ProviderResolver

	locksMu sync.Mutex
	locks   map[ThreadID]*sync.Mutex
}

func New(store Store, options ...Option) *Orchestrator {
	if store == nil {
		store = NewMemoryStore()
	}
	o := &Orchestrator{
		store: store, now: time.Now, randomID: randomID,
		locks: make(map[ThreadID]*sync.Mutex),
	}
	for _, option := range options {
		option(o)
	}
	return o
}

func (o *Orchestrator) lock(threadID ThreadID) *sync.Mutex {
	o.locksMu.Lock()
	defer o.locksMu.Unlock()
	lock := o.locks[threadID]
	if lock == nil {
		lock = &sync.Mutex{}
		o.locks[threadID] = lock
	}
	return lock
}

func (o *Orchestrator) resolveProvider(ctx context.Context, instanceID ProviderInstanceID) (ProviderInfo, error) {
	if o.providers != nil {
		return o.providers.Resolve(ctx, instanceID)
	}
	// Instance IDs are often driver IDs in a single-installation app. A real
	// registry should replace this default before multi-instance support.
	return ProviderInfo{
		Driver: ProviderDriverKind(instanceID),
		Capabilities: ProviderCapabilities{Turns: TurnCapabilities{
			SupportsQueuedMessages: true,
			TerminalStatusQuality:  "strong",
		}},
	}, nil
}

type eventSpec struct {
	eventType  EventType
	threadID   ThreadID
	runID      *RunID
	nodeID     *NodeID
	driver     ProviderDriverKind
	instanceID ProviderInstanceID
	rawEventID *RawEventID
	payload    any
}

type commandPlan struct {
	orchestrator      *Orchestrator
	command           Command
	now               time.Time
	ctx               context.Context
	events            []Event
	effects           []Effect
	projection        Projection
	expectedSequences map[ThreadID]uint64
}

func (p *commandPlan) emit(spec eventSpec) (Event, error) {
	parts := []any{"thread", spec.threadID}
	if p.command.CommandID != "" {
		parts = append(parts, "command", p.command.CommandID)
	}
	id, err := p.orchestrator.randomID("event", parts...)
	if err != nil {
		return Event{}, err
	}
	event := Event{
		ID: EventID(id), Type: spec.eventType, ThreadID: spec.threadID,
		RunID: spec.runID, NodeID: spec.nodeID, Driver: spec.driver,
		ProviderInstanceID: spec.instanceID, RawEventID: spec.rawEventID,
		OccurredAt: p.now, Payload: spec.payload,
	}
	p.events = append(p.events, event)
	return event, nil
}

func (p *commandPlan) emitAndApply(spec eventSpec) (Event, error) {
	event, err := p.emit(spec)
	if err != nil {
		return Event{}, err
	}
	p.projection, err = Apply(p.projection, event)
	return event, err
}

func (p *commandPlan) effect(kind EffectKind, id string, payload any) {
	p.effects = append(p.effects, Effect{
		ID: id, Kind: kind, CommandID: p.command.CommandID, ThreadID: p.command.ThreadID, Payload: payload,
	})
}

func (o *Orchestrator) Dispatch(ctx context.Context, command Command) (DispatchResult, error) {
	if err := validateCommand(command); err != nil {
		return DispatchResult{}, commandError(command, ErrInvalidCommand, err.Error())
	}
	lock := o.lock(command.ThreadID)
	lock.Lock()
	defer lock.Unlock()

	if receipt, ok, err := o.store.GetReceipt(ctx, command.CommandID); err != nil {
		return DispatchResult{}, commandError(command, err, "read command receipt")
	} else if ok {
		return o.replayReceipt(ctx, command, receipt)
	}

	snapshot, loadErr := o.store.Snapshot(ctx, command.ThreadID)
	if loadErr != nil && !(isNotFound(loadErr) && (command.Type == CommandThreadCreate || command.Type == CommandThreadFork)) {
		return DispatchResult{}, commandError(command, loadErr, "load projection")
	}
	plan := &commandPlan{orchestrator: o, command: command, now: o.now().UTC(), projection: snapshot.Projection, ctx: ctx}
	planErr := o.plan(ctx, plan)
	if planErr != nil {
		return o.reject(command, snapshot.SnapshotSequence, planErr)
	}
	if len(plan.events) == 0 {
		return DispatchResult{}, commandError(command, ErrCommandRejected, "command produced no domain events")
	}
	acceptedAt := plan.events[len(plan.events)-1].OccurredAt
	receipt := Receipt{
		CommandID: command.CommandID, ThreadID: command.ThreadID, CommandType: command.Type,
		AcceptedAt: acceptedAt, Status: ReceiptAccepted,
	}
	committed, err := o.store.Commit(ctx, CommitInput{
		ThreadID: command.ThreadID, ExpectedSequence: snapshot.SnapshotSequence,
		Events: plan.events, Receipt: &receipt, ExpectedSequences: plan.expectedSequences,
	})
	if err != nil {
		return DispatchResult{}, commandError(command, err, "commit command")
	}
	if committed.Receipt != nil {
		if err := validateReceipt(command, *committed.Receipt); err != nil {
			return DispatchResult{}, err
		}
	}
	result := DispatchResult{Sequence: committed.Sequence, StoredEvents: committed.StoredEvents, Replayed: !committed.Committed}
	if committed.Committed {
		result.Effects = plan.effects
	}
	return result, nil
}

func (o *Orchestrator) plan(ctx context.Context, plan *commandPlan) error {
	switch plan.command.Type {
	case CommandThreadCreate:
		return o.planThreadCreate(plan)
	case CommandMessageDispatch:
		return o.planMessageDispatch(ctx, plan)
	case CommandRunInterrupt:
		return o.planRunInterrupt(plan)
	case CommandRuntimeRequestRespond:
		return o.planRuntimeRequestRespond(plan)
	case CommandDelegatedTaskRequest:
		return o.planDelegatedTask(ctx, plan)
	case CommandThreadFork:
		return o.planThreadFork(ctx, plan)
	case CommandQueuedRunCancel:
		return o.planQueuedRunCancel(plan)
	case CommandQueueResume:
		return o.planQueueResume(plan)
	case CommandProviderSwitch:
		return o.planProviderSwitch(plan)
	case CommandThreadArchive, CommandThreadUnarchive, CommandThreadDelete:
		return o.planThreadLifecycle(plan)
	default:
		return ErrInvalidCommand
	}
}

func (o *Orchestrator) replayReceipt(ctx context.Context, command Command, receipt Receipt) (DispatchResult, error) {
	if err := validateReceipt(command, receipt); err != nil {
		return DispatchResult{}, err
	}
	events, err := o.store.Read(ctx, EventFilter{CommandID: &command.CommandID})
	if err != nil {
		return DispatchResult{}, commandError(command, err, "read receipt events")
	}
	return DispatchResult{Sequence: receipt.ResultSequence, StoredEvents: events, Replayed: true}, nil
}

func (o *Orchestrator) reject(command Command, sequence uint64, cause error) (DispatchResult, error) {
	detail := cause.Error()
	now := o.now().UTC()
	receipt := Receipt{
		CommandID: command.CommandID, ThreadID: command.ThreadID, CommandType: command.Type,
		AcceptedAt: now, ResultSequence: sequence, Status: ReceiptRejected, Error: &detail,
	}
	_, commitErr := o.store.Commit(context.Background(), CommitInput{
		ThreadID: command.ThreadID, ExpectedSequence: sequence, Receipt: &receipt,
	})
	if commitErr != nil {
		return DispatchResult{}, commandError(command, commitErr, "commit rejection after: "+detail)
	}
	if errorsIsCommandRejected(cause) {
		return DispatchResult{}, cause
	}
	return DispatchResult{}, commandError(command, ErrCommandRejected, detail)
}

func errorsIsCommandRejected(err error) bool {
	_, ok := err.(*Error)
	return ok
}

func (o *Orchestrator) Snapshot(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	return o.store.Snapshot(ctx, threadID)
}

func (o *Orchestrator) List(ctx context.Context) ([]ThreadSnapshot, error) {
	return o.store.List(ctx)
}

func (o *Orchestrator) Events(ctx context.Context, filter EventFilter) ([]StoredEvent, error) {
	return o.store.Read(ctx, filter)
}

func (o *Orchestrator) planThreadCreate(plan *commandPlan) error {
	command, _ := commandPayload[CreateThread](plan.command)
	if plan.projection.Thread.ID != "" {
		return &Error{Op: "create thread", CommandID: plan.command.CommandID, ThreadID: plan.command.ThreadID, Err: ErrThreadAlreadyExists}
	}
	actor, source := creationDefaults(command.CreatedBy, command.CreationSource)
	thread := Thread{
		CreatedBy: actor, CreationSource: source, ID: plan.command.ThreadID,
		ProjectID: command.ProjectID, Title: command.Title, ModelSelection: command.ModelSelection,
		ProviderInstanceID: command.ModelSelection.InstanceID, RuntimeMode: command.RuntimeMode,
		InteractionMode: command.InteractionMode, Branch: command.Branch, WorktreePath: command.WorktreePath,
		Lineage: ThreadLineage{RootThreadID: plan.command.ThreadID}, CreatedAt: plan.now, UpdatedAt: plan.now,
	}
	threadDefaults(&thread)
	_, err := plan.emit(eventSpec{eventType: EventThreadCreated, threadID: thread.ID,
		instanceID: thread.ProviderInstanceID, payload: thread})
	return err
}

func (o *Orchestrator) planThreadLifecycle(plan *commandPlan) error {
	thread := plan.projection.Thread
	var eventType EventType
	switch plan.command.Type {
	case CommandThreadArchive:
		eventType = EventThreadArchived
		thread.ArchivedAt = &plan.now
	case CommandThreadUnarchive:
		eventType = EventThreadUnarchived
		thread.ArchivedAt = nil
	case CommandThreadDelete:
		eventType = EventThreadDeleted
		thread.DeletedAt = &plan.now
	}
	thread.UpdatedAt = plan.now
	_, err := plan.emit(eventSpec{eventType: eventType, threadID: thread.ID,
		instanceID: thread.ProviderInstanceID, payload: thread})
	return err
}

func (o *Orchestrator) planProviderSwitch(plan *commandPlan) error {
	command, _ := commandPayload[ProviderSwitch](plan.command)
	thread := plan.projection.Thread
	if thread.ModelSelection.Equal(command.ModelSelection) {
		return fmt.Errorf("provider selection is unchanged: %w", ErrCommandRejected)
	}
	thread.ModelSelection = command.ModelSelection
	thread.ProviderInstanceID = command.ModelSelection.InstanceID
	thread.UpdatedAt = plan.now
	_, err := plan.emit(eventSpec{eventType: EventThreadProviderSwitched, threadID: thread.ID,
		instanceID: thread.ProviderInstanceID, payload: thread})
	return err
}

func nextTurnItemOrdinal(projection Projection) int {
	ordinal := 1
	for _, item := range projection.TurnItems {
		if item.Ordinal >= ordinal {
			ordinal = item.Ordinal + 1
		}
	}
	return ordinal
}
