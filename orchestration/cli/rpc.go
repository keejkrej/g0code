package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"
)

type rpcResponse struct {
	ID     json.RawMessage
	Result json.RawMessage
	Error  *rpcError
	Raw    json.RawMessage
}

type rpcRequest struct {
	ID        json.RawMessage
	RequestID json.RawMessage
	Type      string
	Method    string
	Params    json.RawMessage
	Raw       json.RawMessage
}

type rpcClientOptions struct {
	Binary         string
	Args           []string
	Dir            string
	Env            []string
	IDPrefix       string
	OnNotification func(json.RawMessage)
	OnResponse     func(json.RawMessage)
	OnRequest      func(context.Context, rpcRequest) (any, *rpcError)
}

type rpcClient struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stderr  bytes.Buffer

	writeMu  sync.Mutex
	nextID   int
	idPrefix string

	pendingMu sync.Mutex
	pending   map[string]chan rpcResponse

	onNotification func(json.RawMessage)
	onResponse     func(json.RawMessage)
	onRequest      func(context.Context, rpcRequest) (any, *rpcError)
	done           chan error
	closeOnce      sync.Once
}

func startRPCClient(options rpcClientOptions) (*rpcClient, error) {
	binary, err := resolveProviderBinary(options.Binary, options.Binary)
	if err != nil {
		return nil, err
	}
	command := exec.Command(binary, options.Args...)
	command.Dir = options.Dir
	command.Env = nativeEnvironment(options.Env)
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	client := &rpcClient{command: command, stdin: stdin, idPrefix: options.IDPrefix,
		pending: make(map[string]chan rpcResponse), onNotification: options.OnNotification,
		onResponse: options.OnResponse, onRequest: options.OnRequest, done: make(chan error, 1)}
	command.Stderr = &client.stderr
	if err := command.Start(); err != nil {
		return nil, &processError{Command: options.Binary, Err: err}
	}
	go client.readLoop(bufio.NewReader(stdout))
	return client, nil
}

func (c *rpcClient) nextRequestID() (any, string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.nextID++
	if c.idPrefix != "" {
		id := fmt.Sprintf("%s%d", c.idPrefix, c.nextID)
		return id, id
	}
	id := c.nextID
	return id, fmt.Sprint(id)
}

func (c *rpcClient) call(ctx context.Context, method string, params any, result any) error {
	return c.requestRecord(ctx, map[string]any{"jsonrpc": "2.0", "method": method, "params": params}, result)
}

func (c *rpcClient) requestRecord(ctx context.Context, record map[string]any, result any) error {
	id, key := c.nextRequestID()
	record["id"] = id
	pending := make(chan rpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[key] = pending
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, key)
		c.pendingMu.Unlock()
	}()
	if err := c.write(record); err != nil {
		return err
	}
	operation, _ := record["method"].(string)
	if operation == "" {
		operation, _ = record["type"].(string)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c.done:
		if err == nil {
			err = io.EOF
		}
		return fmt.Errorf("%s transport closed: %w; stderr=%s", operation, err, boundedStderr(c.stderr.String()))
	case response := <-pending:
		if response.Error != nil {
			return response.Error
		}
		if result != nil {
			if len(response.Result) > 0 {
				return json.Unmarshal(response.Result, result)
			}
			return json.Unmarshal(response.Raw, result)
		}
		return nil
	}
}

func (c *rpcClient) notify(method string, params any) error {
	message := map[string]any{"jsonrpc": "2.0", "method": method}
	if params != nil {
		message["params"] = params
	}
	return c.write(message)
}

func (c *rpcClient) sendRecord(record map[string]any) error { return c.write(record) }

func (c *rpcClient) controlRequest(ctx context.Context, request any) error {
	id, key := c.nextRequestID()
	pending := make(chan rpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[key] = pending
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, key)
		c.pendingMu.Unlock()
	}()
	if err := c.write(map[string]any{"type": "control_request", "request_id": id, "request": request}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c.done:
		return fmt.Errorf("control transport closed: %w", err)
	case response := <-pending:
		if response.Error != nil {
			return response.Error
		}
		return nil
	}
}

func (c *rpcClient) write(message any) error {
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.stdin.Write(append(encoded, '\n'))
	if err != nil {
		return fmt.Errorf("write native request: %w; stderr=%s", err, boundedStderr(c.stderr.String()))
	}
	return err
}

func (c *rpcClient) readLoop(reader *bufio.Reader) {
	var terminal error
	for {
		line, err := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			raw := json.RawMessage(bytes.TrimSpace(line))
			if json.Valid(raw) {
				c.route(raw)
			}
		}
		if err != nil {
			terminal = err
			break
		}
	}
	c.pendingMu.Lock()
	for key, pending := range c.pending {
		delete(c.pending, key)
		pending <- rpcResponse{Error: &rpcError{Code: -1, Message: "process transport closed: " + boundedStderr(c.stderr.String())}}
	}
	c.pendingMu.Unlock()
	c.done <- terminal
}

func (c *rpcClient) route(raw json.RawMessage) {
	var frame struct {
		ID        json.RawMessage `json:"id"`
		Method    string          `json:"method"`
		Params    json.RawMessage `json:"params"`
		Result    json.RawMessage `json:"result"`
		Error     *rpcError       `json:"error"`
		Type      string          `json:"type"`
		JSONRPC   string          `json:"jsonrpc"`
		Kind      string          `json:"kind"`
		Command   string          `json:"command"`
		RequestID json.RawMessage `json:"request_id"`
		Response  struct {
			RequestID json.RawMessage `json:"request_id"`
		} `json:"response"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return
	}
	isResponse := (len(frame.Result) > 0 && len(frame.ID) > 0) || frame.Error != nil || frame.Type == "response" || frame.Kind == "response" || frame.Type == "control_response"
	if isResponse {
		id := frame.ID
		if len(id) == 0 && len(frame.RequestID) > 0 {
			id = frame.RequestID
		}
		if len(id) == 0 && len(frame.Response.RequestID) > 0 {
			id = frame.Response.RequestID
		}
		key := normalizeRPCID(id)
		c.pendingMu.Lock()
		pending := c.pending[key]
		c.pendingMu.Unlock()
		if pending != nil {
			pending <- rpcResponse{ID: id, Result: frame.Result, Error: frame.Error, Raw: raw}
		} else if c.onResponse != nil {
			c.onResponse(raw)
		}
		return
	}
	if (frame.Method != "" && len(frame.ID) > 0 && frame.Type != "extension_ui_request") || (frame.Type == "control_request" && len(frame.RequestID) > 0) {
		requestID := frame.ID
		if frame.Type == "control_request" {
			requestID = frame.RequestID
		}
		request := rpcRequest{ID: requestID, RequestID: frame.RequestID, Type: frame.Type, Method: frame.Method, Params: frame.Params, Raw: raw}
		var result any
		var rpcErr *rpcError
		if c.onRequest != nil {
			result, rpcErr = c.onRequest(context.Background(), request)
		} else {
			rpcErr = &rpcError{Code: -32601, Message: "client callback not supported"}
		}
		response := map[string]any{"jsonrpc": "2.0", "id": frame.ID}
		if frame.Type == "control_request" {
			response = map[string]any{"type": "control_response", "response": map[string]any{"request_id": requestID}}
		}
		if rpcErr != nil {
			if frame.Type == "control_request" {
				response["response"].(map[string]any)["subtype"] = "error"
				response["response"].(map[string]any)["error"] = rpcErr.Message
			} else {
				response["error"] = rpcErr
			}
		} else if frame.Type == "control_request" {
			response["response"].(map[string]any)["subtype"] = "success"
			response["response"].(map[string]any)["result"] = result
		} else {
		}
		_ = c.write(response)
		return
	}
	if frame.Method != "" || frame.Type != "" {
		if c.onNotification != nil {
			c.onNotification(raw)
		}
	}
}

func normalizeRPCID(id json.RawMessage) string {
	var stringID string
	if len(id) > 0 && id[0] == '"' && json.Unmarshal(id, &stringID) == nil {
		return stringID
	}
	return string(id)
}

func (c *rpcClient) close() {
	c.closeOnce.Do(func() {
		_ = c.stdin.Close()
		done := make(chan struct{})
		go func() {
			_ = c.command.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			if c.command.Process != nil {
				_ = c.command.Process.Kill()
			}
			<-done
		}
	})
}
