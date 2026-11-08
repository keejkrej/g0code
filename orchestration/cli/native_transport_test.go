package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/keejkrej/g0code/orchestration"
)

func TestNativeTransportMatchesT3CodeSimpleFixture(t *testing.T) {
	tests := []struct {
		name       string
		instanceID orchestration.ProviderInstanceID
		model      string
		options    map[string]any
		newAdapter func(binary string) Adapter
	}{
		{name: "codex", instanceID: "codex", model: "gpt-6-luna", newAdapter: func(binary string) Adapter {
			return &CodexAdapter{Binary: binary}
		}},
		{name: "pi", instanceID: "pi", model: "openrouter/deepseek/deepseek-v4-flash", newAdapter: func(binary string) Adapter {
			return &PiAdapter{Binary: binary, SessionDir: t.TempDir()}
		}},
		{name: "fx", instanceID: "fx", model: "default", options: map[string]any{"reasoningEffort": "medium"},
			newAdapter: func(binary string) Adapter {
				return &ACPAdapter{Binary: binary, Args: []string{"acp"}, DriverKind: "fx", Mode: "ask"}
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, err := filepath.Abs(filepath.Join("testdata", "t3code-v0.1.7", "simple", test.name+"_transcript.ndjson"))
			if err != nil {
				t.Fatal(err)
			}
			binary := nativeHelperBinary(t, fixture, test.name)
			ctx := context.Background()
			store := orchestration.NewMemoryStore()
			o := orchestration.New(store)
			executor := NewExecutor(o)
			executor.Register(test.instanceID, test.newAdapter(binary))
			defer executor.Close()
			threadID := orchestration.ThreadID("native-" + test.name)
			workspace := t.TempDir()
			title := "Replay fixture: simple"
			dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
				Payload: orchestration.CreateThread{ProjectID: "project", Title: title,
					ModelSelection: orchestration.ModelSelection{InstanceID: test.instanceID, Model: test.model, Options: test.options},
					WorktreePath:   &workspace, RuntimeMode: orchestration.RuntimeFullAccess, InteractionMode: orchestration.InteractionDefault}})
			result := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "message", ThreadID: threadID,
				Payload: orchestration.DispatchMessage{MessageID: "user-message", Text: "Respond with the following text: fixture simple ok",
					DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}}})
			if err := executor.Execute(ctx, result.Effects[0]); err != nil {
				t.Fatalf("native transport replay: %v", err)
			}
			assertSimpleProjectionParity(t, o, store, threadID)
			if err := executor.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeProviderDiscoveryWithPackagedPATH(t *testing.T) {
	fixture, err := filepath.Abs(filepath.Join("testdata", "t3code-v0.1.7", "simple", "codex_transcript.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	providerPath := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(providerPath, 0o700); err != nil {
		t.Fatal(err)
	}
	source := nativeHelperBinary(t, fixture, "codex")
	content, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(providerPath, "codex"), content, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("PATH", "/usr/bin:/bin")
	ctx := context.Background()
	store := orchestration.NewMemoryStore()
	o := orchestration.New(store)
	executor := NewExecutor(o)
	executor.Register("codex", &CodexAdapter{Binary: "codex"})
	defer executor.Close()
	threadID := orchestration.ThreadID("packaged-path")
	workspace := t.TempDir()
	dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
		Payload: orchestration.CreateThread{ProjectID: "project", Title: "Packaged PATH", WorktreePath: &workspace,
			ModelSelection: orchestration.ModelSelection{InstanceID: "codex", Model: "gpt-6-luna"},
			RuntimeMode:    orchestration.RuntimeFullAccess, InteractionMode: orchestration.InteractionDefault}})
	result := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "message", ThreadID: threadID,
		Payload: orchestration.DispatchMessage{MessageID: "user-message", Text: "Respond with the following text: fixture simple ok",
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}}})
	if err := executor.Execute(ctx, result.Effects[0]); err != nil {
		t.Fatalf("provider discovery failed: %v", err)
	}
	assertSimpleProjectionParity(t, o, store, threadID)
}

func TestClaudeUsesStreamingSDKQueryTransport(t *testing.T) {
	fixture, err := filepath.Abs(filepath.Join("testdata", "t3code-v0.1.7", "simple", "claude_transcript.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	binary := nativeHelperBinary(t, fixture, "claude")
	ctx := context.Background()
	store := orchestration.NewMemoryStore()
	o := orchestration.New(store)
	executor := NewExecutor(o)
	executor.Register("claudeAgent", &ClaudeAdapter{Binary: binary})
	defer executor.Close()
	threadID := orchestration.ThreadID("native-claude")
	workspace := t.TempDir()
	dispatch(t, o, orchestration.Command{Type: orchestration.CommandThreadCreate, CommandID: "create", ThreadID: threadID,
		Payload: orchestration.CreateThread{ProjectID: "project", Title: "Replay fixture: simple",
			ModelSelection: orchestration.ModelSelection{InstanceID: "claudeAgent", Model: "default"}, WorktreePath: &workspace}})
	result := dispatch(t, o, orchestration.Command{Type: orchestration.CommandMessageDispatch, CommandID: "message", ThreadID: threadID,
		Payload: orchestration.DispatchMessage{MessageID: "user-message", Text: "Respond with the following text: fixture simple ok",
			DispatchMode: orchestration.DispatchMode{Type: orchestration.DispatchStartImmediately}}})
	if err := executor.Execute(ctx, result.Effects[0]); err != nil {
		t.Fatalf("Claude streaming query replay: %v", err)
	}
	assertSimpleProjectionParity(t, o, store, threadID)
}

func nativeHelperBinary(t *testing.T, fixture, provider string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "native-peer.sh")
	content := "#!/bin/sh\nexec " + shellQuote(os.Args[0]) + " -test.run=^TestNativeTransportHelper$ -- " +
		shellQuote(fixture) + " " + shellQuote(provider) + " \"$@\"\n"
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func TestNativeTransportHelper(t *testing.T) {
	if len(os.Args) < 4 || os.Args[len(os.Args)-2] == "--" {
		return
	}
	separator := -1
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator+2 >= len(os.Args) {
		return
	}
	fixture := os.Args[separator+1]
	provider := os.Args[separator+2]
	if err := runNativePeer(fixture, provider, os.Args[separator+3:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func runNativePeer(fixture, provider string, launchArgs []string) error {
	switch provider {
	case "codex":
		if len(launchArgs) == 0 || launchArgs[0] != "app-server" {
			return fmt.Errorf("Codex launch args = %v, want app-server; argv=%v", launchArgs, os.Args)
		}
	case "pi":
		if !hasAdjacentArg(launchArgs, "--mode", "rpc") {
			return fmt.Errorf("Pi launch args = %v, want --mode rpc", launchArgs)
		}
	case "claude":
		if !hasAdjacentArg(launchArgs, "--input-format", "stream-json") ||
			!hasAdjacentArg(launchArgs, "--output-format", "stream-json") {
			return fmt.Errorf("Claude launch args = %v, want streaming SDK query", launchArgs)
		}
		return runClaudeNativePeer(fixture)
	default:
		if len(launchArgs) == 0 || launchArgs[0] != "acp" {
			return fmt.Errorf("ACP launch args = %v, want acp", launchArgs)
		}
	}
	return runRecordedNativePeer(fixture)
}

func runRecordedNativePeer(fixture string) error {
	entries := readTranscriptEntries(fixture)
	writer := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	requestIDs := make(map[string]any)
	for index := 0; index < len(entries); {
		for index < len(entries) && entries[index].Type != "expect_outbound" {
			index++
		}
		if index >= len(entries) {
			break
		}
		expected := entries[index]
		if expected.Label == "process_start" {
			index++
			continue
		}
		actual, err := readNativeRecord(reader)
		if err != nil {
			return fmt.Errorf("read %s: %w", expected.Label, err)
		}
		if method, ok := actual["method"].(string); ok && actual["kind"] != "response" {
			requestIDs[method] = actual["id"]
		}
		if err := assertSameNativeOperation(expected.Frame, actual); err != nil {
			return fmt.Errorf("%s: %w", expected.Label, err)
		}
		index++
		for index < len(entries) && entries[index].Type == "emit_inbound" {
			frame := decodeNativeMap(entries[index].Frame)
			if frame["kind"] == "request" && frame["id"] == nil {
				frame["id"] = fmt.Sprintf("server-%d", index)
			}
			if nativeFrameIsResponse(frame) {
				frame["id"] = actual["id"]
				if method, ok := frame["method"].(string); ok && requestIDs[method] != nil {
					frame["id"] = requestIDs[method]
				}
			}
			if err := writeNativeRecord(writer, frame); err != nil {
				return err
			}
			index++
		}
	}
	_, err := io.ReadAll(reader)
	return err
}

func runClaudeNativePeer(fixture string) error {
	reader := bufio.NewReader(os.Stdin)
	record, err := readNativeRecord(reader)
	if err != nil {
		return err
	}
	if record["type"] != "user" {
		return fmt.Errorf("first Claude query input type = %v, want user", record["type"])
	}
	message, _ := record["message"].(map[string]any)
	if message["role"] != "user" || message["content"] != "Respond with the following text: fixture simple ok" {
		return fmt.Errorf("Claude query input mismatch: %+v", record)
	}
	writer := bufio.NewWriter(os.Stdout)
	for _, entry := range readTranscriptEntries(fixture) {
		if entry.Type != "emit_inbound" {
			continue
		}
		if err := writeNativeRecord(writer, decodeNativeMap(entry.Frame)); err != nil {
			return err
		}
	}
	_, err = io.ReadAll(reader)
	return err
}

func readTranscriptEntries(path string) []transcriptEntry {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	var entries []transcriptEntry
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		var entry transcriptEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			panic(err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func readNativeRecord(reader *bufio.Reader) (map[string]any, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return decodeNativeMap(line), nil
}

func decodeNativeMap(raw json.RawMessage) map[string]any {
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		panic(err)
	}
	return record
}

func writeNativeRecord(writer *bufio.Writer, record map[string]any) error {
	if err := json.NewEncoder(writer).Encode(record); err != nil {
		return err
	}
	return writer.Flush()
}

func assertSameNativeOperation(expectedRaw json.RawMessage, actual map[string]any) error {
	expected := decodeNativeMap(expectedRaw)
	if expected["kind"] == "response" {
		return nil
	}
	for _, key := range []string{"method", "type"} {
		if expected[key] != nil && expected[key] != actual[key] {
			return fmt.Errorf("%s = %v, want %v; actual=%+v", key, actual[key], expected[key], actual)
		}
	}
	if method := expected["method"]; method == "turn/start" || method == "session/prompt" || expected["type"] == "prompt" {
		if params, ok := expected["params"].(map[string]any); ok && params["input"] != nil {
			if actual["params"] == nil {
				return fmt.Errorf("missing turn input")
			}
		}
	}
	return nil
}

func nativeFrameIsResponse(frame map[string]any) bool {
	return frame["kind"] == "response" || frame["type"] == "response" || (frame["result"] != nil && frame["method"] != "session/update")
}

func hasAdjacentArg(args []string, flag, value string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == flag && args[index+1] == value {
			return true
		}
	}
	return false
}
