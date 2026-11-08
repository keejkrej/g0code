package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type EffectStatus string

const (
	EffectPending   EffectStatus = "pending"
	EffectRunning   EffectStatus = "running"
	EffectCompleted EffectStatus = "completed"
	EffectFailed    EffectStatus = "failed"
	EffectCancelled EffectStatus = "cancelled"
)

type OutboxEffect struct {
	Effect         Effect
	Status         EffectStatus
	Attempts       int
	LastError      *string
	ClaimedBy      string
	LeaseExpiresAt *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type OutboxStore interface {
	ClaimEffects(context.Context, string, int, time.Duration) ([]OutboxEffect, error)
	CompleteEffect(context.Context, string, string, error) error
	RenewEffect(context.Context, string, string, time.Duration) (bool, error)
	ListEffects(context.Context) ([]OutboxEffect, error)
}

type EffectExecutor interface {
	Execute(context.Context, Effect) error
}

type EffectExecutorFunc func(context.Context, Effect) error

func (f EffectExecutorFunc) Execute(ctx context.Context, effect Effect) error { return f(ctx, effect) }

type EffectWorker struct {
	Store    OutboxStore
	Executor EffectExecutor
	WorkerID string
	Limit    int
	Lease    time.Duration
	Poll     time.Duration

	runMu sync.Mutex
}

func (w *EffectWorker) RunOnce(ctx context.Context) (int, error) {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	if w.Executor == nil {
		return 0, fmt.Errorf("effect executor is required")
	}
	claimed, err := w.Store.ClaimEffects(ctx, w.WorkerID, w.Limit, w.Lease)
	if err != nil {
		return 0, err
	}
	for _, claimedEffect := range claimed {
		executionErr := w.executeWithLease(ctx, claimedEffect)
		if err := w.Store.CompleteEffect(context.WithoutCancel(ctx), claimedEffect.Effect.ID, w.WorkerID, executionErr); err != nil {
			return len(claimed), err
		}
	}
	return len(claimed), nil
}

func (w *EffectWorker) executeWithLease(ctx context.Context, claimed OutboxEffect) error {
	lease := w.Lease
	if lease <= 0 {
		lease = time.Minute
	}
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErr := make(chan error, 1)
	go func() {
		interval := lease / 3
		if interval <= 0 {
			interval = time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-execCtx.Done():
				return
			case <-ticker.C:
				renewed, err := w.Store.RenewEffect(execCtx, claimed.Effect.ID, w.WorkerID, lease)
				if err != nil {
					heartbeatErr <- err
					cancel()
					return
				}
				if !renewed {
					heartbeatErr <- fmt.Errorf("effect %s lost its worker lease", claimed.Effect.ID)
					cancel()
					return
				}
			}
		}
	}()
	executionErr := w.Executor.Execute(execCtx, claimed.Effect)
	cancel()
	select {
	case err := <-heartbeatErr:
		if executionErr == nil {
			executionErr = err
		}
	default:
	}
	return executionErr
}

func (w *EffectWorker) Run(ctx context.Context) error {
	poll := w.Poll
	if poll <= 0 {
		poll = 50 * time.Millisecond
	}
	for {
		count, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		if count > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

func (w *EffectWorker) String() string {
	return fmt.Sprintf("orchestration effect worker %s", w.WorkerID)
}
