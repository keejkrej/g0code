package orchestration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

type harness struct {
	orchestrator *orchestration.Orchestrator
	now          time.Time
	ids          int
	raw          int
}

func newHarness(t *testing.T, options ...orchestration.Option) *harness {
	t.Helper()
	h := &harness{now: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)}
	baseOptions := []orchestration.Option{
		orchestration.WithClock(func() time.Time { return h.now }),
		orchestration.WithIDGenerator(func(prefix string, parts ...any) (string, error) {
			h.ids++
			return fmt.Sprintf("%s:test-%d", prefix, h.ids), nil
		}),
	}
	h.orchestrator = orchestration.New(nil, append(baseOptions, options...)...)
	return h
}

func ref[T any](value T) *T { return &value }

func model(instance string) orchestration.ModelSelection {
	return orchestration.ModelSelection{InstanceID: orchestration.ProviderInstanceID(instance), Model: "model-1"}
}

func (h *harness) createThread(t *testing.T, id string) orchestration.DispatchResult {
	t.Helper()
	result, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandThreadCreate, CommandID: orchestration.CommandID("create-" + id), ThreadID: orchestration.ThreadID(id),
		Payload: orchestration.CreateThread{ProjectID: "project-1", Title: "Thread " + id,
			ModelSelection: model("codex"), RuntimeMode: orchestration.RuntimeFullAccess,
			InteractionMode: orchestration.InteractionDefault, CreatedBy: orchestration.ActorUser, CreationSource: orchestration.CreationWeb},
	})
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	return result
}

func (h *harness) send(t *testing.T, threadID, commandID, messageID, text string) orchestration.DispatchResult {
	t.Helper()
	result, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandMessageDispatch, CommandID: orchestration.CommandID(commandID), ThreadID: orchestration.ThreadID(threadID),
		Payload: orchestration.DispatchMessage{MessageID: orchestration.MessageID(messageID), Text: text,
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately},
			CreatedBy:    orchestration.ActorUser, CreationSource: orchestration.CreationWeb},
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	return result
}

func (h *harness) nextRaw() *orchestration.RawEventID {
	h.raw++
	return ref(orchestration.RawEventID(fmt.Sprintf("raw-%d", h.raw)))
}

func (h *harness) startProviderTurn(t *testing.T, threadID string) (orchestration.Run, orchestration.ProviderTurn) {
	t.Helper()
	snapshot, err := h.orchestrator.Snapshot(context.Background(), orchestration.ThreadID(threadID))
	if err != nil {
		t.Fatal(err)
	}
	run, ok := snapshot.Projection.ActiveRun()
	if !ok {
		t.Fatal("no active run")
	}
	providerThread, ok := snapshot.Projection.ProviderThread(*run.ProviderThreadID)
	if !ok {
		t.Fatal("provider thread missing")
	}
	sessionID := orchestration.ProviderSessionID("session-" + threadID)
	session := orchestration.ProviderSession{ID: sessionID, Driver: providerThread.Driver,
		ProviderInstanceID: providerThread.ProviderInstanceID, Status: orchestration.SessionReady,
		CWD: "/workspace", CreatedAt: h.now, UpdatedAt: h.now}
	_, err = h.orchestrator.Ingest(context.Background(), orchestration.IngestInput{
		ThreadID: run.ThreadID, ProviderSessionID: sessionID, ProviderInstanceID: run.ProviderInstanceID,
		RawEventID: h.nextRaw(), Event: orchestration.ProviderEvent{Type: orchestration.ProviderEventSessionUpdated,
			Driver: providerThread.Driver, ProviderSession: &session},
	})
	if err != nil {
		t.Fatalf("attach provider session: %v", err)
	}
	providerThread.ProviderSessionID = &sessionID
	providerThread.NativeThreadRef = &orchestration.ProviderRef{Driver: providerThread.Driver,
		NativeID: ref("native-" + threadID), Strength: orchestration.NativeRefStrong}
	providerThread.Status = orchestration.ProviderThreadActive
	_, err = h.orchestrator.Ingest(context.Background(), orchestration.IngestInput{
		ThreadID: run.ThreadID, ProviderSessionID: sessionID, ProviderInstanceID: run.ProviderInstanceID,
		RawEventID: h.nextRaw(), Event: orchestration.ProviderEvent{Type: orchestration.ProviderEventThreadUpdated,
			Driver: providerThread.Driver, ProviderThread: &providerThread},
	})
	if err != nil {
		t.Fatalf("attach provider thread: %v", err)
	}
	turn := orchestration.ProviderTurn{ID: orchestration.DeriveProviderTurnID(providerThread.Driver, "turn-1"),
		ProviderThreadID: providerThread.ID, NodeID: *run.RootNodeID, RunAttemptID: run.ActiveAttemptID,
		NativeTurnRef: &orchestration.ProviderRef{Driver: providerThread.Driver, NativeID: ref("turn-1"), Strength: orchestration.NativeRefStrong},
		Ordinal:       1, Status: orchestration.ProviderTurnRunning}
	_, err = h.orchestrator.Ingest(context.Background(), orchestration.IngestInput{
		ThreadID: run.ThreadID, RunID: &run.ID, NodeID: run.RootNodeID, ProviderSessionID: sessionID,
		ProviderInstanceID: run.ProviderInstanceID, RawEventID: h.nextRaw(),
		Event: orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, Driver: providerThread.Driver, ProviderTurn: &turn},
	})
	if err != nil {
		t.Fatalf("start provider turn: %v", err)
	}
	return run, turn
}

func (h *harness) finishProviderTurn(t *testing.T, run orchestration.Run, turn orchestration.ProviderTurn, status orchestration.ProviderTurnStatus) orchestration.IngestResult {
	t.Helper()
	turn.Status = status
	result, err := h.orchestrator.Ingest(context.Background(), orchestration.IngestInput{
		ThreadID: run.ThreadID, RunID: &run.ID, NodeID: run.RootNodeID,
		ProviderSessionID:  orchestration.ProviderSessionID("session-" + string(run.ThreadID)),
		ProviderInstanceID: run.ProviderInstanceID, RawEventID: h.nextRaw(),
		Event: orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn},
	})
	if err != nil {
		t.Fatalf("finish provider turn: %v", err)
	}
	return result
}

func TestDispatchCreatesRunGraphAndReplaysReceipt(t *testing.T) {
	h := newHarness(t)
	created := h.createThread(t, "thread-1")
	if created.Sequence != 1 || len(created.StoredEvents) != 1 {
		t.Fatalf("unexpected create result: %+v", created)
	}
	sent := h.send(t, "thread-1", "send-1", "message-1", "hello")
	if len(sent.Effects) != 1 || sent.Effects[0].Kind != orchestration.EffectProviderTurnStart {
		t.Fatalf("expected one start effect, got %+v", sent.Effects)
	}
	snapshot, err := h.orchestrator.Snapshot(context.Background(), "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	projection := snapshot.Projection
	if snapshot.SnapshotSequence != sent.Sequence || len(projection.Runs) != 1 || len(projection.Attempts) != 1 ||
		len(projection.Nodes) != 1 || len(projection.Messages) != 1 || len(projection.TurnItems) != 1 {
		t.Fatalf("incomplete graph: %+v", projection)
	}
	run := projection.Runs[0]
	if run.ID != orchestration.DeriveRunID("thread-1", 1) || run.Status != orchestration.RunStarting || run.Ordinal != 1 {
		t.Fatalf("unexpected run: %+v", run)
	}
	if projection.Nodes[0].Kind != orchestration.NodeRootTurn || !projection.Nodes[0].CountsForRun {
		t.Fatalf("unexpected root: %+v", projection.Nodes[0])
	}

	replayed, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandMessageDispatch, CommandID: "send-1", ThreadID: "thread-1",
		Payload: orchestration.DispatchMessage{MessageID: "message-1", Text: "hello",
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}},
	})
	if err != nil {
		t.Fatalf("replay receipt: %v", err)
	}
	if !replayed.Replayed || replayed.Sequence != sent.Sequence || len(replayed.Effects) != 0 ||
		len(replayed.StoredEvents) != len(sent.StoredEvents) {
		t.Fatalf("receipt did not replay safely: %+v", replayed)
	}

	_, err = h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandRunInterrupt, CommandID: "send-1", ThreadID: "thread-1",
		Payload: orchestration.InterruptRun{RunID: run.ID},
	})
	if !errors.Is(err, orchestration.ErrCommandIDConflict) {
		t.Fatalf("expected command ID conflict, got %v", err)
	}
}

func TestQueuedRunStartsOnlyAfterRootTerminal(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "first")
	run, turn := h.startProviderTurn(t, "thread-1")
	queued := h.send(t, "thread-1", "send-2", "message-2", "second")
	if len(queued.Effects) != 0 {
		t.Fatalf("queued run started too early: %+v", queued.Effects)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), "thread-1")
	if snapshot.Projection.Runs[1].Status != orchestration.RunQueued {
		t.Fatalf("second run is not queued: %+v", snapshot.Projection.Runs[1])
	}

	finished := h.finishProviderTurn(t, run, turn, orchestration.ProviderTurnCompleted)
	if len(finished.Effects) != 1 || finished.Effects[0].Kind != orchestration.EffectProviderTurnStart {
		t.Fatalf("next run was not released: %+v", finished.Effects)
	}
	snapshot, _ = h.orchestrator.Snapshot(context.Background(), "thread-1")
	if snapshot.Projection.Runs[0].Status != orchestration.RunCompleted || snapshot.Projection.Runs[1].Status != orchestration.RunStarting {
		t.Fatalf("unexpected queue settlement: %+v", snapshot.Projection.Runs)
	}
	for _, item := range snapshot.Projection.TurnItems {
		if item.Type == "user_message" && item.RunID != nil && *item.RunID == snapshot.Projection.Runs[1].ID {
			if item.Data["inputIntent"] != "queued_turn" || item.Status != orchestration.TurnItemCompleted {
				t.Fatalf("promoted item lost queued intent: %+v", item)
			}
		}
	}
}

func TestChildTurnCompletionDoesNotCompleteParentRun(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "parent")
	run, rootTurn := h.startProviderTurn(t, "thread-1")
	childNode := orchestration.ExecutionNode{ID: "node-child", ThreadID: run.ThreadID, RunID: &run.ID,
		ParentNodeID: run.RootNodeID, RootNodeID: *run.RootNodeID, Kind: orchestration.NodeSubagent,
		Status: orchestration.NodeRunning, CountsForRun: false, ProviderThreadID: run.ProviderThreadID}
	_, err := h.orchestrator.Ingest(context.Background(), orchestration.IngestInput{ThreadID: run.ThreadID,
		RunID: &run.ID, RawEventID: h.nextRaw(), Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventNodeUpdated, Node: &childNode}})
	if err != nil {
		t.Fatal(err)
	}
	childTurn := orchestration.ProviderTurn{ID: "provider-turn-child", ProviderThreadID: *run.ProviderThreadID,
		NodeID: childNode.ID, Ordinal: 1, Status: orchestration.ProviderTurnCompleted}
	_, err = h.orchestrator.Ingest(context.Background(), orchestration.IngestInput{ThreadID: run.ThreadID,
		RunID: &run.ID, NodeID: &childNode.ID, RawEventID: h.nextRaw(), Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &childTurn}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), "thread-1")
	currentRun, _ := snapshot.Projection.Run(run.ID)
	currentChild, _ := snapshot.Projection.Node(childNode.ID)
	if currentRun.Status != orchestration.RunRunning || currentChild.Status != orchestration.NodeCompleted {
		t.Fatalf("child terminal leaked into parent: run=%s child=%s", currentRun.Status, currentChild.Status)
	}
	h.finishProviderTurn(t, run, rootTurn, orchestration.ProviderTurnCompleted)
	snapshot, _ = h.orchestrator.Snapshot(context.Background(), "thread-1")
	currentRun, _ = snapshot.Projection.Run(run.ID)
	if currentRun.Status != orchestration.RunCompleted {
		t.Fatalf("root did not complete run: %s", currentRun.Status)
	}
}

func TestInterruptAcknowledgementIsNotTerminal(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "work")
	run, turn := h.startProviderTurn(t, "thread-1")
	result, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandRunInterrupt, CommandID: "stop-1", ThreadID: run.ThreadID,
		Payload: orchestration.InterruptRun{RunID: run.ID, Reason: "user stop", HoldQueue: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Effects) != 1 || result.Effects[0].Kind != orchestration.EffectProviderTurnInterrupt {
		t.Fatalf("missing interrupt effect: %+v", result.Effects)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), run.ThreadID)
	current, _ := snapshot.Projection.Run(run.ID)
	if current.Status != orchestration.RunRunning {
		t.Fatalf("interrupt acknowledgement settled run: %s", current.Status)
	}
	h.finishProviderTurn(t, run, turn, orchestration.ProviderTurnInterrupted)
	snapshot, _ = h.orchestrator.Snapshot(context.Background(), run.ThreadID)
	current, _ = snapshot.Projection.Run(run.ID)
	if current.Status != orchestration.RunInterrupted {
		t.Fatalf("provider terminal did not interrupt run: %s", current.Status)
	}
	foundResult := false
	for _, item := range snapshot.Projection.TurnItems {
		if item.Type == "run_interrupt_result" {
			foundResult = true
			if item.Status != orchestration.TurnItemInterrupted || item.ParentItemID == nil ||
				*item.ParentItemID != orchestration.DeriveRunSignalTurnItemID(run.ID, "interrupt-request") {
				t.Fatalf("interrupt result is not linked to its request: %+v", item)
			}
		}
	}
	if !foundResult {
		t.Fatal("missing run_interrupt_result")
	}
}

func TestStopHoldsAutomaticWorkUntilQueueResume(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "work")
	run, turn := h.startProviderTurn(t, "thread-1")
	_, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandRunInterrupt, CommandID: "stop-1", ThreadID: run.ThreadID,
		Payload: orchestration.InterruptRun{RunID: run.ID, HoldQueue: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandMessageDispatch, CommandID: "wake-1", ThreadID: run.ThreadID,
		Payload: orchestration.DispatchMessage{MessageID: "wake-message", Text: "background wake",
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchQueueAfterActive},
			CreatedBy:    orchestration.ActorAgent, CreationSource: orchestration.CreationProvider},
	})
	if err != nil {
		t.Fatal(err)
	}
	finished := h.finishProviderTurn(t, run, turn, orchestration.ProviderTurnInterrupted)
	if len(finished.Effects) != 0 {
		t.Fatalf("held queue started automatically: %+v", finished.Effects)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), run.ThreadID)
	if !snapshot.Projection.Runs[1].QueueHeld || snapshot.Projection.Runs[1].Status != orchestration.RunQueued {
		t.Fatalf("automatic wake did not remain held: %+v", snapshot.Projection.Runs[1])
	}
	resumed, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandQueueResume, CommandID: "resume-1", ThreadID: run.ThreadID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.Effects) != 1 || resumed.Effects[0].Kind != orchestration.EffectProviderTurnStart {
		t.Fatalf("queue resume did not release run: %+v", resumed)
	}
	snapshot, _ = h.orchestrator.Snapshot(context.Background(), run.ThreadID)
	if snapshot.Projection.Runs[1].Status != orchestration.RunStarting || snapshot.Projection.Runs[1].QueueHeld {
		t.Fatalf("queue did not resume: %+v", snapshot.Projection.Runs[1])
	}
}

func TestRuntimeRequestResponseRequiresLiveCallback(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "work")
	run, turn := h.startProviderTurn(t, "thread-1")
	nodeID := orchestration.NodeID("approval-node")
	request := orchestration.RuntimeRequest{ID: "request-1", NodeID: nodeID, ProviderTurnID: &turn.ID,
		Kind: orchestration.RequestCommand, Status: orchestration.RequestPending,
		ResponseCapability: orchestration.ResponseCapability{Type: "live", ProviderSessionID: ref(orchestration.ProviderSessionID("session-thread-1"))},
		CreatedAt:          h.now}
	_, err := h.orchestrator.Ingest(context.Background(), orchestration.IngestInput{ThreadID: run.ThreadID,
		RunID: &run.ID, NodeID: &nodeID, RawEventID: h.nextRaw(), Event: orchestration.ProviderEvent{
			Type: orchestration.ProviderEventRuntimeRequestUpdated, RuntimeRequest: &request}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandRuntimeRequestRespond, CommandID: "answer-1", ThreadID: run.ThreadID,
		Payload: orchestration.RuntimeRequestResponse{RequestID: request.ID, Decision: ref("accept")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Effects) != 1 || response.Effects[0].Kind != orchestration.EffectProviderRuntimeRequestRespond {
		t.Fatalf("missing response effect: %+v", response.Effects)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), run.ThreadID)
	resolved, _ := snapshot.Projection.RuntimeRequest(request.ID)
	if resolved.Status != orchestration.RequestResolved || resolved.Decision == nil || *resolved.Decision != "accept" {
		t.Fatalf("request did not resolve: %+v", resolved)
	}
}

func TestForkIsLazyAndResolvesNativeOnFirstRun(t *testing.T) {
	resolver := orchestration.ProviderResolverFunc(func(_ context.Context, id orchestration.ProviderInstanceID) (orchestration.ProviderInfo, error) {
		return orchestration.ProviderInfo{Driver: orchestration.ProviderDriverKind(id), Capabilities: orchestration.ProviderCapabilities{
			Threads: orchestration.ThreadCapabilities{CanForkThread: true},
			Turns:   orchestration.TurnCapabilities{SupportsQueuedMessages: true},
		}}, nil
	})
	h := newHarness(t, orchestration.WithProviderResolver(resolver))
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "source")
	run, turn := h.startProviderTurn(t, "thread-1")
	h.finishProviderTurn(t, run, turn, orchestration.ProviderTurnCompleted)

	forked, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandThreadFork, CommandID: "fork-1", ThreadID: "thread-2",
		Payload: orchestration.ForkThread{SourceThreadID: "thread-1",
			SourcePoint: orchestration.ForkSourcePoint{Type: "run", RunID: run.ID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(forked.StoredEvents) != 2 {
		t.Fatalf("fork should create only thread and transfer: %+v", forked.StoredEvents)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), "thread-2")
	if len(snapshot.Projection.ProviderThreads) != 0 || snapshot.Projection.ContextTransfers[0].Status != "pending" {
		t.Fatalf("fork eagerly resolved provider state: %+v", snapshot.Projection)
	}
	h.send(t, "thread-2", "send-2", "message-2", "continue")
	snapshot, _ = h.orchestrator.Snapshot(context.Background(), "thread-2")
	transfer := snapshot.Projection.ContextTransfers[0]
	providerThread := snapshot.Projection.ProviderThreads[0]
	if transfer.Status != "consumed" || transfer.Resolution == nil || transfer.Resolution.Strategy != "native_fork" ||
		providerThread.ForkedFrom == nil || len(snapshot.Projection.ContextHandoffs) != 0 {
		t.Fatalf("fork did not resolve natively: transfer=%+v providerThread=%+v", transfer, providerThread)
	}
}

func TestReplayMatchesSnapshotAndRawEventsAreIdempotent(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "hello")
	run, turn := h.startProviderTurn(t, "thread-1")
	raw := h.nextRaw()
	turn.Status = orchestration.ProviderTurnCompleted
	input := orchestration.IngestInput{ThreadID: run.ThreadID, RunID: &run.ID, NodeID: run.RootNodeID,
		ProviderSessionID: "session-thread-1", ProviderInstanceID: run.ProviderInstanceID, RawEventID: raw,
		Event: orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnUpdated, ProviderTurn: &turn}}
	first, err := h.orchestrator.Ingest(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.orchestrator.Ingest(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.Sequence != first.Sequence {
		t.Fatalf("raw event was applied twice: first=%+v second=%+v", first, second)
	}
	events, err := h.orchestrator.Events(context.Background(), orchestration.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := orchestration.Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), run.ThreadID)
	expected, _ := json.Marshal(snapshot.Projection)
	actual, _ := json.Marshal(replayed[run.ThreadID])
	if string(expected) != string(actual) {
		t.Fatalf("replay diverged\nexpected: %s\nactual: %s", expected, actual)
	}
	afterSnapshot := snapshot.SnapshotSequence
	remaining, err := h.orchestrator.Events(context.Background(), orchestration.EventFilter{AfterSequence: &afterSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("snapshot cursor replayed already-reflected events: %+v", remaining)
	}
}

func TestProviderReturnUsesDeltaHandoff(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	h.send(t, "thread-1", "send-1", "message-1", "codex work")
	run1, turn1 := h.startProviderTurn(t, "thread-1")
	h.finishProviderTurn(t, run1, turn1, orchestration.ProviderTurnCompleted)

	claude := model("claude")
	_, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandMessageDispatch, CommandID: "send-2", ThreadID: "thread-1",
		Payload: orchestration.DispatchMessage{MessageID: "message-2", Text: "claude work", ModelSelection: &claude,
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}},
	})
	if err != nil {
		t.Fatal(err)
	}
	run2, turn2 := h.startProviderTurn(t, "thread-1")
	h.finishProviderTurn(t, run2, turn2, orchestration.ProviderTurnCompleted)

	codex := model("codex")
	result, err := h.orchestrator.Dispatch(context.Background(), orchestration.Command{
		Type: orchestration.CommandMessageDispatch, CommandID: "send-3", ThreadID: "thread-1",
		Payload: orchestration.DispatchMessage{MessageID: "message-3", Text: "back to codex", ModelSelection: &codex,
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}},
	})
	if err != nil {
		t.Fatal(err)
	}
	start, ok := result.Effects[0].Payload.(orchestration.TurnStartEffect)
	if !ok || start.ContextHandoff == nil {
		t.Fatalf("missing handoff in start effect: %+v", result.Effects)
	}
	handoff := start.ContextHandoff
	if handoff.Strategy != "delta_since_target_last_seen" || handoff.CoveredRunOrdinals.From != 2 ||
		handoff.CoveredRunOrdinals.To != 2 || !contains(handoff.SummaryText, "claude work") ||
		contains(handoff.SummaryText, "codex work") {
		t.Fatalf("expected run-2-only delta handoff: %+v", handoff)
	}
	snapshot, _ := h.orchestrator.Snapshot(context.Background(), "thread-1")
	if len(snapshot.Projection.ContextTransfers) != 2 || len(snapshot.Projection.ContextHandoffs) != 2 ||
		snapshot.Projection.Runs[2].ContextHandoffID == nil {
		t.Fatalf("handoffs were not durable: %+v", snapshot.Projection)
	}
}

func contains(text, part string) bool {
	return strings.Contains(text, part)
}

func TestRejectedCommandReceiptIsStable(t *testing.T) {
	h := newHarness(t)
	h.createThread(t, "thread-1")
	command := orchestration.Command{Type: orchestration.CommandRunInterrupt, CommandID: "bad-stop", ThreadID: "thread-1",
		Payload: orchestration.InterruptRun{RunID: "missing-run"}}
	_, err := h.orchestrator.Dispatch(context.Background(), command)
	if !errors.Is(err, orchestration.ErrCommandRejected) {
		t.Fatalf("expected rejection, got %v", err)
	}
	_, err = h.orchestrator.Dispatch(context.Background(), command)
	if !errors.Is(err, orchestration.ErrCommandPreviouslyRejected) {
		t.Fatalf("expected stable rejected receipt, got %v", err)
	}
}

func TestEventJSONRoundTripPreservesConcretePayload(t *testing.T) {
	h := newHarness(t)
	created := h.createThread(t, "thread-1")
	data, err := json.Marshal(created.StoredEvents[0])
	if err != nil {
		t.Fatal(err)
	}
	var decoded orchestration.StoredEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	thread, ok := decoded.Event.Payload.(orchestration.Thread)
	if !ok || thread.ID != "thread-1" || decoded.Sequence != 1 {
		t.Fatalf("payload did not round-trip: %+v", decoded)
	}
}
