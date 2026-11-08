package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/keejkrej/g0code/orchestration"
)

type ToolCall struct {
	ID       string
	Type     string
	Title    string
	Status   orchestration.TurnItemStatus
	Input    any
	Output   any
	ExitCode *int
	FileName string
	Diff     string
}

func emitToolCall(ctx context.Context, request RunRequest, emit EmitFunc, call ToolCall, raw json.RawMessage) error {
	now := time.Now().UTC()
	providerThread := request.currentProviderThread()
	providerTurn := request.currentTurn()
	nodeID := orchestration.DeriveNodeFromProviderItemID(providerThread.Driver, call.ID)
	nodeStatus := orchestration.NodeRunning
	switch call.Status {
	case orchestration.TurnItemPending, orchestration.TurnItemWaiting:
		nodeStatus = orchestration.NodeStatus(call.Status)
	case orchestration.TurnItemCompleted, orchestration.TurnItemFailed, orchestration.TurnItemCancelled, orchestration.TurnItemInterrupted:
		nodeStatus = orchestration.NodeStatus(call.Status)
	}
	node := orchestration.ExecutionNode{ID: nodeID, ThreadID: request.Start.Thread.ID, RunID: &request.Start.Run.ID,
		ParentNodeID: request.Start.Run.RootNodeID, RootNodeID: *request.Start.Run.RootNodeID, Kind: orchestration.NodeToolCall,
		Status: nodeStatus, CountsForRun: false, ProviderThreadID: &providerThread.ID, ProviderTurnID: &providerTurn.ID,
		NativeItemRef: &orchestration.ProviderRef{Driver: providerThread.Driver, NativeID: &call.ID, Strength: orchestration.NativeRefStrong},
		StartedAt:     &now}
	if nodeStatus != orchestration.NodeRunning && nodeStatus != orchestration.NodePending && nodeStatus != orchestration.NodeWaiting {
		node.CompletedAt = &now
	}
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventNodeUpdated, Node: &node}, raw); err != nil {
		return err
	}
	item := orchestration.TurnItem{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
		ID: orchestration.DeriveTurnItemFromProviderItemID(providerThread.Driver, call.ID), ThreadID: request.Start.Thread.ID,
		RunID: &request.Start.Run.ID, NodeID: &nodeID, ProviderThreadID: &providerThread.ID, ProviderTurnID: &providerTurn.ID,
		NativeItemRef: node.NativeItemRef, Ordinal: 1, Status: call.Status, Title: call.Title, Type: call.Type,
		UpdatedAt: now, Data: map[string]any{"input": call.Input, "output": call.Output}}
	if call.ExitCode != nil {
		item.Data["exitCode"] = *call.ExitCode
	}
	if call.FileName != "" {
		item.Data["fileName"] = call.FileName
	}
	if call.Diff != "" {
		item.Data["diffStr"] = call.Diff
	}
	if node.CompletedAt != nil {
		item.CompletedAt = node.CompletedAt
	}
	return emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated, TurnItem: &item}, raw)
}

type pendingNativeRequest struct {
	id      orchestration.RuntimeRequestID
	channel chan orchestration.RuntimeRequestResponse
}

type requestBroker struct {
	mu          sync.Mutex
	pending     map[string]pendingNativeRequest
	done        chan struct{}
	closeOnceMu sync.Once
}

func newRequestBroker() *requestBroker {
	return &requestBroker{pending: make(map[string]pendingNativeRequest), done: make(chan struct{})}
}

func (b *requestBroker) ask(ctx context.Context, request RunRequest, emit EmitFunc, nativeID string,
	kind orchestration.RuntimeRequestKind, title string, input any) (orchestration.RuntimeRequestResponse, error) {
	requestID := orchestration.RuntimeRequestID(stableID("runtime-request:cli:", string(request.currentTurn().ID)+":"+nativeID))
	nodeID := orchestration.NodeID("node:runtime-request:" + string(requestID))
	providerTurnID := request.currentTurn().ID
	providerThread := request.currentProviderThread()
	nativeRef := &orchestration.ProviderRef{Driver: providerThread.Driver, NativeID: &nativeID, Strength: orchestration.NativeRefStrong}
	runtimeRequest := orchestration.RuntimeRequest{ID: requestID, NodeID: nodeID, ProviderTurnID: &providerTurnID,
		NativeRequestRef: nativeRef, Kind: kind, Status: orchestration.RequestPending,
		ResponseCapability: orchestration.ResponseCapability{Type: "live", ProviderSessionID: &request.Session.ID},
		CreatedAt:          time.Now().UTC()}
	node := orchestration.ExecutionNode{ID: nodeID, ThreadID: request.Start.Thread.ID, RunID: &request.Start.Run.ID,
		ParentNodeID: request.Start.Run.RootNodeID, RootNodeID: *request.Start.Run.RootNodeID, Kind: orchestration.NodeApprovalRequest,
		Status: orchestration.NodeWaiting, CountsForRun: false, ProviderThreadID: &providerThread.ID,
		ProviderTurnID: &providerTurnID, RuntimeRequestID: &requestID, StartedAt: ref(time.Now().UTC())}
	item := orchestration.TurnItem{CreatedBy: orchestration.ActorAgent, CreationSource: orchestration.CreationProvider,
		ID: orchestration.TurnItemID("turn-item:runtime-request:" + string(requestID)), ThreadID: request.Start.Thread.ID,
		RunID: &request.Start.Run.ID, NodeID: &nodeID, ProviderThreadID: &providerThread.ID, ProviderTurnID: &providerTurnID,
		Status: orchestration.TurnItemWaiting, Title: title, Type: "approval_request", RequestID: requestID,
		UpdatedAt: time.Now().UTC(), Data: map[string]any{"input": input}}
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventNodeUpdated, Node: &node}, nil); err != nil {
		return orchestration.RuntimeRequestResponse{}, err
	}
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventRuntimeRequestUpdated, RuntimeRequest: &runtimeRequest}, nil); err != nil {
		return orchestration.RuntimeRequestResponse{}, err
	}
	if err := emit(ctx, orchestration.ProviderEvent{Type: orchestration.ProviderEventTurnItemUpdated, TurnItem: &item}, nil); err != nil {
		return orchestration.RuntimeRequestResponse{}, err
	}
	pending := pendingNativeRequest{id: requestID, channel: make(chan orchestration.RuntimeRequestResponse, 1)}
	b.mu.Lock()
	b.pending[nativeID] = pending
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.pending, nativeID)
		b.mu.Unlock()
	}()
	select {
	case response := <-pending.channel:
		return response, nil
	case <-ctx.Done():
		return orchestration.RuntimeRequestResponse{}, ctx.Err()
	case <-b.done:
		return orchestration.RuntimeRequestResponse{Decision: ref("cancel")}, nil
	}
}

func (b *requestBroker) respond(response orchestration.RuntimeRequestRespondEffect) error {
	nativeRef := response.Request.NativeRequestRef
	if nativeRef == nil || nativeRef.NativeID == nil {
		return fmt.Errorf("runtime request %s has no native ref", response.Request.ID)
	}
	b.mu.Lock()
	pending, ok := b.pending[*nativeRef.NativeID]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("runtime request %s is no longer live: %w", response.Request.ID, orchestration.ErrRequestNotRespondable)
	}
	pending.channel <- response.Response
	return nil
}

func (b *requestBroker) cancelAll() {
	b.mu.Lock()
	pending := make([]pendingNativeRequest, 0, len(b.pending))
	for _, request := range b.pending {
		pending = append(pending, request)
	}
	b.pending = make(map[string]pendingNativeRequest)
	b.mu.Unlock()
	for _, request := range pending {
		request.channel <- orchestration.RuntimeRequestResponse{Decision: ref("cancel")}
	}
}

func (b *requestBroker) close() {
	b.closeOnce()
}

func (b *requestBroker) closeOnce() {
	b.closeOnceMu.Do(func() { close(b.done) })
}
