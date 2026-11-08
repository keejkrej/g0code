package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/keejkrej/g0code/orchestration"
	store "github.com/keejkrej/g0code/orchestration/sqlite"
)

func TestDurableEventsReceiptsAndOutboxAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	store1, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := orchestration.New(store1)
	created := dispatch(t, o, createCommand("create", "thread-1"))
	sent := dispatch(t, o, messageCommand("send", "thread-1", "message-1"))
	if len(sent.Effects) != 1 {
		t.Fatalf("start effect missing: %+v", sent)
	}
	replayed := dispatch(t, o, messageCommand("send", "thread-1", "message-1"))
	if !replayed.Replayed || replayed.Sequence != sent.Sequence || len(replayed.Effects) != 0 {
		t.Fatalf("receipt did not replay: %+v", replayed)
	}
	effects, err := store1.ListEffects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 1 || effects[0].Status != orchestration.EffectPending {
		t.Fatalf("effects before restart = %+v", effects)
	}
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	snapshot, err := store2.Snapshot(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SnapshotSequence != sent.Sequence || len(snapshot.Projection.Runs) != 1 || snapshot.Projection.Messages[0].Text != "hello" {
		t.Fatalf("restored snapshot mismatch: %+v", snapshot)
	}
	events, err := store2.Read(ctx, orchestration.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("sequence %d = %d", index, event.Sequence)
		}
	}
	claimed, err := store2.ClaimEffects(ctx, "worker-1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("claim = %+v", claimed)
	}
	if _, ok := claimed[0].Effect.Payload.(orchestration.TurnStartEffect); !ok {
		t.Fatalf("effect payload lost its concrete type: %T", claimed[0].Effect.Payload)
	}
	if err := store2.CompleteEffect(ctx, sent.Effects[0].ID, "worker-1", nil); err != nil {
		t.Fatal(err)
	}
	effects, _ = store2.ListEffects(ctx)
	if effects[0].Status != orchestration.EffectCompleted {
		t.Fatalf("effect did not complete: %+v", effects[0])
	}
	claimed, err = store2.ClaimEffects(ctx, "worker-2", 10, time.Minute)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("completed effect reclaimed: %+v, %v", claimed, err)
	}
	_ = created
}

func TestExpiredEffectLeaseCanBeReclaimedOnce(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o := orchestration.New(s)
	dispatch(t, o, createCommand("create", "thread-1"))
	dispatch(t, o, messageCommand("send", "thread-1", "message-1"))
	first, err := s.ClaimEffects(ctx, "worker-1", 1, time.Millisecond)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %+v, %v", first, err)
	}
	time.Sleep(10 * time.Millisecond)
	second, err := s.ClaimEffects(ctx, "worker-2", 1, time.Minute)
	if err != nil || len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("expired claim = %+v, %v", second, err)
	}
	if err := s.CompleteEffect(ctx, first[0].Effect.ID, "worker-1", nil); err == nil {
		t.Fatal("stale worker completed a reclaimed effect")
	}
	if err := s.CompleteEffect(ctx, second[0].Effect.ID, "worker-2", errors.New("provider unavailable")); err != nil {
		t.Fatal(err)
	}
	effects, _ := s.ListEffects(ctx)
	if effects[0].Status != orchestration.EffectFailed || effects[0].LastError == nil {
		t.Fatalf("failure not durable: %+v", effects[0])
	}
}

func TestWorkerCompletesProviderTurnAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	s1, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o1 := orchestration.New(s1)
	dispatch(t, o1, createCommand("create", "thread-1"))
	dispatch(t, o1, messageCommand("send", "thread-1", "message-1"))
	s1.Close()

	s2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	o2 := orchestration.New(s2)
	worker := &orchestration.EffectWorker{Store: s2, WorkerID: "worker-restart", Lease: time.Minute,
		Executor: orchestration.EffectExecutorFunc(func(ctx context.Context, effect orchestration.Effect) error {
			start := effect.Payload.(orchestration.TurnStartEffect)
			turn := orchestration.ProviderTurn{ID: "turn-restored", ProviderThreadID: *start.Run.ProviderThreadID,
				NodeID: *start.Run.RootNodeID, RunAttemptID: start.Run.ActiveAttemptID, Ordinal: 1,
				Status: orchestration.ProviderTurnRunning}
			if _, err := o2.Ingest(ctx, orchestration.IngestInput{ThreadID: start.Thread.ID, RunID: &start.Run.ID,
				ProviderInstanceID: start.Run.ProviderInstanceID, Event: orchestration.ProviderEvent{
					Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}}); err != nil {
				return err
			}
			turn.Status = orchestration.ProviderTurnCompleted
			_, err := o2.Ingest(ctx, orchestration.IngestInput{ThreadID: start.Thread.ID, RunID: &start.Run.ID,
				ProviderInstanceID: start.Run.ProviderInstanceID, Event: orchestration.ProviderEvent{
					Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}})
			return err
		})}
	count, err := worker.RunOnce(ctx)
	if err != nil || count != 1 {
		t.Fatalf("worker count = %d, err=%v", count, err)
	}
	snapshot, err := o2.Snapshot(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Projection.Runs[0].Status != orchestration.RunCompleted {
		t.Fatalf("run did not complete: %+v", snapshot.Projection.Runs[0])
	}
}

func TestWorkerRenewsLongRunningEffectLease(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o := orchestration.New(s)
	dispatch(t, o, createCommand("create", "thread-1"))
	dispatch(t, o, messageCommand("send", "thread-1", "message-1"))
	started := make(chan struct{})
	release := make(chan struct{})
	worker := &orchestration.EffectWorker{Store: s, WorkerID: "worker-long", Lease: 30 * time.Millisecond,
		Executor: orchestration.EffectExecutorFunc(func(context.Context, orchestration.Effect) error {
			close(started)
			<-release
			return nil
		})}
	done := make(chan error, 1)
	go func() {
		_, err := worker.RunOnce(ctx)
		done <- err
	}()
	<-started
	time.Sleep(70 * time.Millisecond)
	claimed, err := s.ClaimEffects(ctx, "worker-other", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("running effect was reclaimed despite renewal: %+v", claimed)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	effects, _ := s.ListEffects(ctx)
	if len(effects) != 1 || effects[0].Status != orchestration.EffectCompleted || effects[0].Attempts != 1 {
		t.Fatalf("long-running effect did not complete on its original lease: %+v", effects)
	}
}

func TestMultiThreadDelegationSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	o := orchestration.New(s)
	dispatch(t, o, createCommand("create", "parent"))
	dispatch(t, o, messageCommand("send", "parent", "parent-message"))
	parent, err := o.Snapshot(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	run := parent.Projection.Runs[0]
	result := dispatch(t, o, orchestration.Command{Type: orchestration.CommandDelegatedTaskRequest,
		CommandID: "delegate", ThreadID: "parent", Payload: orchestration.DelegateTask{
			ParentThreadID: "parent", ParentRunID: run.ID, ParentNodeID: *run.RootNodeID,
			Task: "child task", ModelSelection: orchestration.ModelSelection{InstanceID: "codex", Model: "default"},
			CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationMCP}})
	if len(result.Effects) != 1 {
		t.Fatalf("delegation effects = %+v", result.Effects)
	}
	childID := orchestration.DeriveDelegatedTaskThreadID("delegate")
	s.Close()
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	parentAfter, err := reopened.Snapshot(ctx, "parent")
	if err != nil {
		t.Fatal(err)
	}
	childAfter, err := reopened.Snapshot(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}
	if len(parentAfter.Projection.Subagents) != 1 || len(childAfter.Projection.Runs) != 1 ||
		len(childAfter.Projection.ContextTransfers) != 1 {
		t.Fatalf("delegation did not restore: parent=%+v child=%+v", parentAfter.Projection, childAfter.Projection)
	}
	encodedParent, _ := json.Marshal(parentAfter.Projection.Subagents[0])
	if !strings.Contains(string(encodedParent), "app_owned") {
		t.Fatalf("wrong subagent: %s", encodedParent)
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

func createCommand(commandID, threadID string) orchestration.Command {
	return orchestration.Command{Type: orchestration.CommandThreadCreate,
		CommandID: orchestration.CommandID(commandID), ThreadID: orchestration.ThreadID(threadID),
		Payload: orchestration.CreateThread{ProjectID: "project", Title: "Durable thread",
			ModelSelection: orchestration.ModelSelection{InstanceID: "codex", Model: "default"},
			CreatedBy:      orchestration.ActorUser, CreationSource: orchestration.CreationWeb}}
}

func messageCommand(commandID, threadID, messageID string) orchestration.Command {
	return orchestration.Command{Type: orchestration.CommandMessageDispatch,
		CommandID: orchestration.CommandID(commandID), ThreadID: orchestration.ThreadID(threadID),
		Payload: orchestration.DispatchMessage{MessageID: orchestration.MessageID(messageID), Text: "hello",
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately},
			CreatedBy:    orchestration.ActorUser, CreationSource: orchestration.CreationWeb}}
}
