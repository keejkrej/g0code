package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/keejkrej/g0code/orchestration"
)

type frameHandler interface {
	Adapter
	handleFrame(context.Context, RunRequest, json.RawMessage, EmitFunc) error
}

// ReplayAdapter drives a production frame normalizer with recorded native
// frames. Only the process transport is replaced.
type ReplayAdapter struct {
	Adapter frameHandler
	Frames  []json.RawMessage
}

func NewReplayAdapter(adapter Adapter, frames []json.RawMessage) (ReplayAdapter, error) {
	handler, ok := adapter.(frameHandler)
	if !ok {
		return ReplayAdapter{}, fmt.Errorf("%T does not expose a native frame normalizer", adapter)
	}
	return ReplayAdapter{Adapter: handler, Frames: frames}, nil
}

func (r ReplayAdapter) Driver() orchestration.ProviderDriverKind { return r.Adapter.Driver() }
func (r ReplayAdapter) Capabilities() orchestration.ProviderCapabilities {
	return r.Adapter.Capabilities()
}
func (r ReplayAdapter) Run(ctx context.Context, request RunRequest, emit EmitFunc) error {
	for _, frame := range r.Frames {
		if err := r.Adapter.handleFrame(ctx, request, frame, emit); err != nil {
			return err
		}
	}
	return nil
}

type SequenceAdapter struct {
	Adapter frameHandler
	Turns   [][]json.RawMessage
	index   int
}

func (s *SequenceAdapter) Driver() orchestration.ProviderDriverKind { return s.Adapter.Driver() }
func (s *SequenceAdapter) Capabilities() orchestration.ProviderCapabilities {
	return s.Adapter.Capabilities()
}
func (s *SequenceAdapter) Run(ctx context.Context, request RunRequest, emit EmitFunc) error {
	if s.index >= len(s.Turns) {
		return nil
	}
	frames := s.Turns[s.index]
	s.index++
	return (ReplayAdapter{Adapter: s.Adapter, Frames: frames}).Run(ctx, request, emit)
}
