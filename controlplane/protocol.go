// Package controlplane implements the Go side of the t3code control-plane protocol v1.
//
// The normative wire contract is PROTOCOL.md in the t3code-control-plane repository
// (/Users/jack/workspace/t3code-control-plane/protocol/PROTOCOL.md). This package provides:
//
//   - protocol constants, scopes, and the MCP tool → scope table;
//   - pairing-code normalization, key fingerprints, and the pairing-exchange message;
//   - Ed25519 device identities (generate, persist, sign the exchange message);
//   - a dependency-free connect-token (EdDSA JWT) verifier that follows the normative
//     device-side verification steps exactly;
//   - a JWKS cache, a thread-safe DeviceAuthorizer, and net/http middleware for the device API;
//   - an HTTP client for the device (pairing, heartbeat) and GUI (authorization, devices,
//     connect tokens) halves of the protocol;
//   - a small persisted device-state file.
//
// The package uses only the Go standard library.
package controlplane

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"time"
)

// Protocol-level constants. They mirror src/lib/protocol.ts in the control-plane repository.
const (
	ProtocolVersion = 1

	ConnectTokenTTLSeconds    = 300
	HeartbeatIntervalSeconds  = 60
	PairingTTLSeconds         = 600
	PollIntervalSeconds       = 5
	SlowDownIncrementSeconds  = 5
	ExchangeClockSkewSeconds  = 300
	VerifyLeewaySeconds       = 30
	MaxConnectTokenLifetime   = 900
	JWKSRefetchMinIntervalSec = 60

	PairingExchangeContext = "t3cp-pairing-exchange-v1"
	DeviceCredentialPrefix = "t3cpd_"
	ClientTokenPrefix      = "t3cpc_"

	// CodeAlphabet is the unambiguous consonant alphabet used for pairing and user codes.
	CodeAlphabet = "BCDFGHJKLMNPQRSTVWXZ"
	// CodeLength is the number of characters in a normalized code.
	CodeLength = 8

	// WellKnownPath is the discovery document path relative to the control-plane base URL.
	WellKnownPath = "/.well-known/t3code-control-plane"
)

// Scope is an orchestration authorization scope.
type Scope string

const (
	ScopeRead  Scope = "orchestration:read"
	ScopeWrite Scope = "orchestration:write"
)

// SupportedScopes lists every v1 scope in canonical order.
var SupportedScopes = []Scope{ScopeRead, ScopeWrite}

// ReadTools are MCP tools that need only orchestration:read.
var ReadTools = []string{
	"orchestrator_capabilities",
	"t3_thread_list",
	"t3_thread_read",
	"t3_thread_wait",
	"task_status",
}

// WriteTools are MCP tools that need orchestration:write.
var WriteTools = []string{
	"create_threads",
	"delegate_task",
	"task_cancel",
	"t3_thread_send",
	"t3_thread_interrupt",
	"t3_pending_request",
}

// ScopeForTool returns the scope required to call an MCP tool. Unknown tools default to
// orchestration:write.
func ScopeForTool(name string) Scope {
	for _, tool := range ReadTools {
		if tool == name {
			return ScopeRead
		}
	}
	return ScopeWrite
}

// RequiredScopeForMethod returns the scope an MCP JSON-RPC request needs. needsScope is false
// for methods that only require a valid connect token (initialize, tools/list, ping, and every
// notifications/* message, so read-only clients can complete the handshake). For tools/call the
// scope comes from ScopeForTool(toolName). Every other method conservatively requires
// orchestration:write.
func RequiredScopeForMethod(method, toolName string) (scope Scope, needsScope bool) {
	switch {
	case method == "initialize" || method == "tools/list" || method == "ping" ||
		strings.HasPrefix(method, "notifications/"):
		return "", false
	case method == "tools/call":
		return ScopeForTool(toolName), true
	default:
		return ScopeWrite, true
	}
}

// ScopesCover reports whether a granted scope list authorizes an operation that requires
// required. orchestration:write implies orchestration:read. An empty required scope is always
// covered.
func ScopesCover(granted []string, required Scope) bool {
	has := func(s Scope) bool {
		for _, g := range granted {
			if g == string(s) {
				return true
			}
		}
		return false
	}
	switch required {
	case "":
		return true
	case ScopeRead:
		return has(ScopeRead) || has(ScopeWrite)
	default:
		return has(required)
	}
}

// NormalizeUserCode uppercases ASCII letters, drops every character that is not an ASCII letter
// or digit, and returns the result only if exactly CodeLength characters remain and all are in
// CodeAlphabet. ok is false for invalid codes.
func NormalizeUserCode(input string) (normalized string, ok bool) {
	var b strings.Builder
	for i := 0; i < len(input); i++ {
		c := input[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			b.WriteByte(c)
		}
	}
	out := b.String()
	if len(out) != CodeLength {
		return "", false
	}
	for i := 0; i < len(out); i++ {
		if strings.IndexByte(CodeAlphabet, out[i]) < 0 {
			return "", false
		}
	}
	return out, true
}

// FormatUserCode renders a normalized code for display as XXXX-XXXX. Inputs that are not
// exactly CodeLength bytes are returned unchanged.
func FormatUserCode(normalized string) string {
	if len(normalized) != CodeLength {
		return normalized
	}
	return normalized[:4] + "-" + normalized[4:]
}

// PublicKeyFingerprint returns "SHA256:" + base64url(SHA-256(raw public key)) for a base64url
// encoded raw Ed25519 public key.
func PublicKeyFingerprint(publicKey string) (string, error) {
	raw, err := decodeB64URL(publicKey)
	if err != nil {
		return "", errors.New("controlplane: invalid public key encoding")
	}
	return FingerprintRaw(raw), nil
}

// FingerprintRaw returns the protocol fingerprint for raw public key bytes.
func FingerprintRaw(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "SHA256:" + base64.RawURLEncoding.EncodeToString(sum[:])
}

// PairingExchangeMessage builds the exact message a device signs during the pairing exchange:
// the context string, normalized code, pairing ID, and timestamp joined by "\n" with no trailing
// newline. timestamp must be passed exactly as it will be sent.
func PairingExchangeMessage(normalizedCode, pairingID, timestamp string) string {
	return strings.Join([]string{PairingExchangeContext, normalizedCode, pairingID, timestamp}, "\n")
}

// FormatTimestamp renders t as an RFC 3339 UTC timestamp with millisecond precision, the format
// used throughout the protocol (e.g. 2026-11-09T20:00:00.000Z).
func FormatTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// isB64URL reports whether s is a non-empty string over the base64url alphabet (no padding).
func isB64URL(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// decodeB64URL decodes unpadded base64url after checking the alphabet explicitly (the stdlib
// decoder otherwise silently skips CR/LF).
func decodeB64URL(s string) ([]byte, error) {
	if !isB64URL(s) {
		return nil, errors.New("invalid base64url")
	}
	return base64.RawURLEncoding.DecodeString(s)
}
