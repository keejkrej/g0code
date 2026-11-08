package orchestration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/keejkrej/g0code/orchestration"
)

func TestMemoryStoreCommitIsAtomicOnEventFailure(t *testing.T) {
	store := orchestration.NewMemoryStore()
	o := orchestration.New(store)
	ctx := context.Background()
	created, err := o.Dispatch(ctx, orchestration.Command{
		Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: "thread-1",
		Payload: orchestration.CreateThread{ProjectID: "project", Title: "Before", ModelSelection: model("codex")},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Snapshot(ctx, "thread-1")
	changed := snapshot.Projection.Thread
	changed.Title = "After"
	_, err = store.Commit(ctx, orchestration.CommitInput{ThreadID: "thread-1", ExpectedSequence: created.Sequence,
		Events: []orchestration.Event{
			{ID: "event-valid", Type: orchestration.EventThreadMetadataUpdated, ThreadID: "thread-1",
				OccurredAt: snapshot.Projection.Thread.UpdatedAt, Payload: changed},
			{ID: "event-invalid", Type: orchestration.EventThreadMetadataUpdated, ThreadID: "other-thread",
				OccurredAt: snapshot.Projection.Thread.UpdatedAt, Payload: changed},
		}})
	if err == nil {
		t.Fatal("expected invalid cross-thread event")
	}
	events, readErr := store.Read(ctx, orchestration.EventFilter{})
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(events) != 1 || events[0].Sequence != created.Sequence {
		t.Fatalf("failed batch published events: %+v", events)
	}
	after, _ := store.Snapshot(ctx, "thread-1")
	if after.Projection.Thread.Title != "Before" || after.SnapshotSequence != created.Sequence {
		t.Fatalf("failed batch changed projection: %+v", after)
	}
}

func TestMemoryStoreDetectsConcurrentSequence(t *testing.T) {
	store := orchestration.NewMemoryStore()
	_, err := store.Commit(context.Background(), orchestration.CommitInput{
		ThreadID: "missing", ExpectedSequence: 1,
		Events: []orchestration.Event{{ID: "event", Type: orchestration.EventThreadCreated, ThreadID: "missing"}},
	})
	if !errors.Is(err, orchestration.ErrSequenceConflict) {
		t.Fatalf("expected sequence conflict, got %v", err)
	}
}

func TestDeterministicIDsMatchT3CodeEncoding(t *testing.T) {
	got := orchestration.DeriveRunID("thread:provider/native conversation", 2)
	want := orchestration.RunID("run:thread:thread%3Aprovider%2Fnative%20conversation:ordinal:2")
	if got != want {
		t.Fatalf("run ID = %q, want %q", got, want)
	}
	gotProvider := orchestration.DeriveProviderThreadID("codex", "work/account", "native/1")
	wantProvider := orchestration.ProviderThreadID("provider-thread:provider:codex:provider-instance:work%2Faccount:native-thread:native%2F1")
	if gotProvider != wantProvider {
		t.Fatalf("provider thread ID = %q, want %q", gotProvider, wantProvider)
	}
}
