// Package mcp exposes orchestration through the Model Context Protocol.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/keejkrej/g0code/orchestration"
)

type EffectExecutor interface {
	Execute(context.Context, orchestration.Effect) error
}

type EffectExecutorFunc func(context.Context, orchestration.Effect) error

func (f EffectExecutorFunc) Execute(ctx context.Context, effect orchestration.Effect) error {
	return f(ctx, effect)
}

type Provider struct {
	InstanceID   orchestration.ProviderInstanceID `json:"instanceId"`
	Driver       orchestration.ProviderDriverKind `json:"driver"`
	Models       []string                         `json:"models"`
	Available    bool                             `json:"available"`
	RuntimeModes []orchestration.RuntimeMode      `json:"runtimeModes,omitempty"`
}

type Config struct {
	Providers           []Provider
	DefaultProjectID    orchestration.ProjectID
	DefaultWorktreePath *string
	DefaultTarget       orchestration.ModelSelection
	// ParentThreadID scopes delegated task tools to an existing parent. If it
	// is empty, delegate_task accepts parentThreadId as a local testing input.
	ParentThreadID orchestration.ThreadID
	OnEffectError  func(orchestration.Effect, error)
}

type Server struct {
	orchestrator *orchestration.Orchestrator
	effects      EffectExecutor
	config       Config

	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]context.CancelFunc
}

func NewServer(o *orchestration.Orchestrator, effects EffectExecutor, config Config) *Server {
	return &Server{orchestrator: o, effects: effects, config: config, pending: make(map[string]context.CancelFunc)}
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *Server) Serve(ctx context.Context, input io.Reader, output io.Writer) error {
	reader := bufio.NewReader(input)
	var workers sync.WaitGroup
	defer func() {
		s.pendingMu.Lock()
		for _, cancel := range s.pending {
			cancel()
		}
		s.pendingMu.Unlock()
		workers.Wait()
	}()
	for {
		line, err := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var request request
			if decodeErr := json.Unmarshal(line, &request); decodeErr != nil {
				s.write(output, response{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: decodeErr.Error()}})
			} else {
				if request.Method == "exit" {
					return nil
				}
				if len(request.ID) == 0 {
					s.handleNotification(request)
					continue
				}
				requestCtx, cancel := context.WithCancel(ctx)
				key := requestKey(request.ID)
				s.pendingMu.Lock()
				s.pending[key] = cancel
				s.pendingMu.Unlock()
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer cancel()
					defer func() {
						s.pendingMu.Lock()
						delete(s.pending, key)
						s.pendingMu.Unlock()
					}()
					result, rpcErr := s.handle(requestCtx, request)
					s.write(output, response{JSONRPC: "2.0", ID: request.ID, Result: result, Error: rpcErr})
				}()
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (s *Server) handleNotification(request request) {
	if request.Method != "notifications/cancelled" {
		return
	}
	var params struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if json.Unmarshal(request.Params, &params) == nil {
		s.pendingMu.Lock()
		cancel := s.pending[requestKey(params.RequestID)]
		s.pendingMu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
}

func (s *Server) handle(ctx context.Context, request request) (any, *rpcError) {
	switch request.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "g0code-orchestrator", "title": "g0code Orchestrator", "version": "0.1.0"},
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": s.tools()}, nil
	case "tools/call":
		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return nil, &rpcError{Code: -32602, Message: err.Error()}
		}
		result, failure := s.callTool(ctx, params.Name, params.Arguments)
		if failure != nil {
			return callToolResult(failure, true), nil
		}
		return callToolResult(result, false), nil
	case "shutdown":
		return nil, nil
	default:
		return nil, &rpcError{Code: -32601, Message: "method not found: " + request.Method}
	}
}

func callToolResult(value any, isError bool) map[string]any {
	text, _ := json.Marshal(value)
	return map[string]any{"content": []any{map[string]any{"type": "text", "text": string(text)}},
		"structuredContent": value, "isError": isError}
}

func (s *Server) write(output io.Writer, response response) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = json.NewEncoder(output).Encode(response)
}

func requestKey(id json.RawMessage) string {
	var text string
	if len(id) > 0 && id[0] == '"' && json.Unmarshal(id, &text) == nil {
		return text
	}
	return string(id)
}

func (s *Server) runEffects(effects []orchestration.Effect) {
	for _, effect := range effects {
		if s.effects == nil {
			continue
		}
		go func(effect orchestration.Effect) {
			if err := s.effects.Execute(context.Background(), effect); err != nil && s.config.OnEffectError != nil {
				s.config.OnEffectError(effect, err)
			}
		}(effect)
	}
}

func invalidRequest(detail string) *Failure {
	return &Failure{Tag: "OrchestratorMcpFailure", Code: "invalid_request", Message: detail}
}

func failureFromError(err error) *Failure {
	code := "orchestration_error"
	switch {
	case err == nil:
		return nil
	case isError(err, orchestration.ErrThreadNotFound):
		code = "thread_not_found"
	case isError(err, orchestration.ErrRunNotInterruptible):
		code = "thread_not_interruptible"
	case isError(err, orchestration.ErrUnsupportedCapability):
		code = "provider_unavailable"
	case isError(err, orchestration.ErrInvalidCommand):
		code = "invalid_request"
	}
	return &Failure{Tag: "OrchestratorMcpFailure", Code: code, Message: err.Error()}
}

func isError(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

func (s *Server) String() string {
	return fmt.Sprintf("g0code MCP server with %d providers", len(s.config.Providers))
}
