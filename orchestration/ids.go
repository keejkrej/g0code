package orchestration

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

type (
	CommandID          string
	EventID            string
	RawEventID         string
	ProjectID          string
	ThreadID           string
	MessageID          string
	RunID              string
	RunAttemptID       string
	NodeID             string
	ProviderSessionID  string
	ProviderThreadID   string
	ProviderTurnID     string
	RuntimeRequestID   string
	TurnItemID         string
	CheckpointScopeID  string
	CheckpointID       string
	ContextHandoffID   string
	ContextTransferID  string
	PlanID             string
	ProviderDriverKind string
	ProviderInstanceID string
)

func ptr[T any](value T) *T { return &value }

// encodeIDPart matches JavaScript's encodeURIComponent, which t3code uses for
// deterministic orchestration IDs.
func encodeIDPart(value string) string {
	const hexDigits = "0123456789ABCDEF"
	var out strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
			(b >= '0' && b <= '9') || strings.ContainsRune("-_.!~*'()", rune(b)) {
			out.WriteByte(b)
			continue
		}
		out.WriteByte('%')
		out.WriteByte(hexDigits[b>>4])
		out.WriteByte(hexDigits[b&15])
	}
	return out.String()
}

func joinID(prefix string, parts ...any) string {
	var out strings.Builder
	out.WriteString(prefix)
	for _, part := range parts {
		out.WriteByte(':')
		out.WriteString(encodeIDPart(fmt.Sprint(part)))
	}
	return out.String()
}

func DeriveRunID(threadID ThreadID, ordinal int) RunID {
	return RunID(joinID("run", "thread", threadID, "ordinal", ordinal))
}

func DeriveRunAttemptID(runID RunID, ordinal int) RunAttemptID {
	return RunAttemptID(joinID("run-attempt", "run", runID, "attempt", ordinal))
}

func DeriveRootNodeID(runID RunID) NodeID {
	return NodeID(joinID("node", "run", runID, "root"))
}

func DeriveDelegatedTaskNodeID(commandID CommandID) NodeID {
	return NodeID(joinID("node", "delegated-task", commandID))
}

func DeriveDelegatedTaskThreadID(commandID CommandID) ThreadID {
	return ThreadID(joinID("thread", "delegated-task", commandID))
}

func DeriveDelegatedTaskMessageID(commandID CommandID) MessageID {
	return MessageID(joinID("message", "delegated-task", commandID))
}

func DeriveDelegatedTaskTurnItemID(commandID CommandID) TurnItemID {
	return TurnItemID(joinID("turn-item", "delegated-task", commandID))
}

func DeriveProviderThreadID(driver ProviderDriverKind, instanceID ProviderInstanceID, nativeID string) ProviderThreadID {
	parts := []any{"provider", driver}
	if instanceID != "" {
		parts = append(parts, "provider-instance", instanceID)
	}
	parts = append(parts, "native-thread", nativeID)
	return ProviderThreadID(joinID("provider-thread", parts...))
}

func DeriveProviderTurnID(driver ProviderDriverKind, nativeID string) ProviderTurnID {
	return ProviderTurnID(joinID("provider-turn", "provider", driver, "native-turn", nativeID))
}

func DeriveNodeFromProviderItemID(driver ProviderDriverKind, nativeID string) NodeID {
	return NodeID(joinID("node", "provider", driver, "native-item", nativeID))
}

func DeriveMessageFromProviderItemID(driver ProviderDriverKind, nativeID string) MessageID {
	return MessageID(joinID("message", "provider", driver, "native-item", nativeID))
}

func DeriveTurnItemFromProviderItemID(driver ProviderDriverKind, nativeID string) TurnItemID {
	return TurnItemID(joinID("turn-item", "provider", driver, "native-item", nativeID))
}

func DeriveUserTurnItemID(messageID MessageID) TurnItemID {
	return TurnItemID(joinID("turn-item", "message", messageID))
}

func DeriveRunSignalTurnItemID(runID RunID, signal string) TurnItemID {
	return TurnItemID(joinID("turn-item", "run", runID, "signal", signal))
}

func DeriveCheckpointScopeID(threadID ThreadID, name string) CheckpointScopeID {
	return CheckpointScopeID(joinID("checkpoint-scope", "thread", threadID, "name", name))
}

func randomID(prefix string, parts ...any) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("allocate %s ID: %w", prefix, err)
	}
	// UUID v4, without depending on a UUID package.
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	uuid := hex.EncodeToString(bytes[:])
	uuid = uuid[:8] + "-" + uuid[8:12] + "-" + uuid[12:16] + "-" + uuid[16:20] + "-" + uuid[20:]
	parts = append(parts, uuid)
	return joinID(prefix, parts...), nil
}
