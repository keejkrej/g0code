package controlplane

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// VerifyErrorCode is a device-API error code for connect-token failures.
type VerifyErrorCode string

const (
	CodeInvalidToken      VerifyErrorCode = "invalid_token"
	CodeTokenExpired      VerifyErrorCode = "token_expired"
	CodeWrongAudience     VerifyErrorCode = "wrong_audience"
	CodeStaleEpoch        VerifyErrorCode = "stale_epoch"
	CodeDeviceRevoked     VerifyErrorCode = "device_revoked"
	CodeInsufficientScope VerifyErrorCode = "insufficient_scope"
)

// VerifyError is returned by VerifyConnectToken. Code is the protocol error code; Reason is
// human-readable detail and must not be branched on.
type VerifyError struct {
	Code   VerifyErrorCode
	Reason string
}

func (e *VerifyError) Error() string {
	return fmt.Sprintf("controlplane: %s: %s", e.Code, e.Reason)
}

// HTTPStatus returns the device-API status for the error: 403 for insufficient_scope (step 10)
// and 401 for everything else (steps 1–9).
func (e *VerifyError) HTTPStatus() int {
	if e.Code == CodeInsufficientScope {
		return 403
	}
	return 401
}

func verifyFail(code VerifyErrorCode, reason string) *VerifyError {
	return &VerifyError{Code: code, Reason: reason}
}

// JWK is an RFC 8037 OKP public key as published in the control-plane JWKS.
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Kid string `json:"kid"`
	Alg string `json:"alg,omitempty"`
	Use string `json:"use,omitempty"`
}

// JWKThumbprint returns the RFC 7638 thumbprint (the protocol's kid) of an Ed25519 OKP key with
// base64url public key x: base64url(SHA-256(`{"crv":"Ed25519","kty":"OKP","x":"<x>"}`)).
func JWKThumbprint(x string) string {
	sum := sha256.Sum256([]byte(`{"crv":"Ed25519","kty":"OKP","x":"` + x + `"}`))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// JWKS is a JSON Web Key Set.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// lookup mirrors the reference verifier: the first key whose kid matches and that is OKP/Ed25519.
func (s JWKS) lookup(kid string) (JWK, bool) {
	for _, k := range s.Keys {
		if k.Kid == kid && k.Kty == "OKP" && k.Crv == "Ed25519" {
			return k, true
		}
	}
	return JWK{}, false
}

// Has reports whether the set contains a usable OKP/Ed25519 key with kid.
func (s JWKS) Has(kid string) bool {
	k, ok := s.lookup(kid)
	return ok && k.X != ""
}

// VerificationContext is the device-side state a connect token is checked against.
type VerificationContext struct {
	// Issuer stored at pairing (exact string match, no normalization).
	Issuer string
	// DeviceID is this device's ID; the token audience must be exactly this value.
	DeviceID string
	// RevocationEpoch is the last epoch learned from pairing or heartbeat.
	RevocationEpoch int64
	// Revoked is true once the control plane answered a heartbeat with 403 device_revoked.
	Revoked bool
	// JWKS holds the control plane's published verification keys.
	JWKS JWKS
	// Now is the verification time. The zero value means time.Now(). Only whole seconds are used.
	Now time.Time
}

// ConnectClaims are the validated claims of a connect token.
type ConnectClaims struct {
	Issuer    string
	Subject   string
	Audience  string
	IssuedAt  int64
	NotBefore int64
	ExpiresAt int64
	JTI       string
	Scope     string
	// Scopes is Scope split on spaces with empty entries removed, in token order.
	Scopes  []string
	Epoch   int64
	Version int64
	// Raw holds every payload member, including unknown ones.
	Raw map[string]json.RawMessage
}

// HasScope reports whether the claims authorize an operation requiring s.
func (c *ConnectClaims) HasScope(s Scope) bool { return ScopesCover(c.Scopes, s) }

// Expiry returns exp as a time.
func (c *ConnectClaims) Expiry() time.Time { return time.Unix(c.ExpiresAt, 0) }

// maxSafeInteger is 2^53 − 1, the largest integer exactly representable in an IEEE-754 double.
const maxSafeInteger = 1<<53 - 1

// VerifyConnectToken verifies a connect token following the normative device-side verification
// steps in PROTOCOL.md, in order, mirroring the TypeScript reference verifyConnectToken. required
// is the scope the operation needs; pass "" for operations that only need a valid token
// (initialize, tools/list, ping), in which case step 10 is skipped.
//
// The returned error, if any, is always a *VerifyError.
func VerifyConnectToken(token string, ctx VerificationContext, required Scope) (*ConnectClaims, error) {
	claims, verr := verifyConnectToken(token, ctx, required)
	if verr != nil {
		return nil, verr
	}
	return claims, nil
}

func verifyConnectToken(token string, ctx VerificationContext, required Scope) (*ConnectClaims, *VerifyError) {
	now := ctx.Now
	if now.IsZero() {
		now = time.Now()
	}
	nowSec := now.Unix()

	// 1. Structure and header.
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, verifyFail(CodeInvalidToken, "malformed token")
	}
	h, p, s := parts[0], parts[1], parts[2]
	header, ok := decodeSegment(h)
	if !ok {
		return nil, verifyFail(CodeInvalidToken, "malformed header")
	}
	if alg, ok := jsonString(header, "alg"); !ok || alg != "EdDSA" {
		return nil, verifyFail(CodeInvalidToken, "unsupported alg")
	}
	if _, present := header["typ"]; present {
		if typ, ok := jsonString(header, "typ"); !ok || typ != "JWT" {
			return nil, verifyFail(CodeInvalidToken, "unsupported typ")
		}
	}
	kid, ok := jsonString(header, "kid")
	if !ok {
		return nil, verifyFail(CodeInvalidToken, "missing kid")
	}

	// 2. Key lookup.
	jwk, found := ctx.JWKS.lookup(kid)
	if !found || jwk.X == "" {
		return nil, verifyFail(CodeInvalidToken, "unknown kid")
	}

	// 3. Signature over the ASCII bytes "<header>.<payload>".
	if !isB64URL(s) {
		return nil, verifyFail(CodeInvalidToken, "malformed signature")
	}
	sig, err := decodeB64URL(s)
	pub, perr := decodeB64URL(jwk.X)
	if err != nil || perr != nil || len(sig) != ed25519.SignatureSize || len(pub) != ed25519.PublicKeySize ||
		!ed25519.Verify(ed25519.PublicKey(pub), []byte(h+"."+p), sig) {
		return nil, verifyFail(CodeInvalidToken, "bad signature")
	}

	payload, ok := decodeSegment(p)
	if !ok {
		return nil, verifyFail(CodeInvalidToken, "malformed payload")
	}

	// 4. Issuer.
	if iss, ok := jsonString(payload, "iss"); !ok || iss != ctx.Issuer {
		return nil, verifyFail(CodeInvalidToken, "wrong issuer")
	}

	// 5. Audience: a string, or an array containing exactly that one ID.
	audience, ok := singleAudience(payload["aud"])
	if !ok || audience != ctx.DeviceID {
		return nil, verifyFail(CodeWrongAudience, "token is for another device")
	}

	// 6. Time, with 30 s leeway and a 900 s maximum lifetime.
	exp, okExp := jsonInt(payload, "exp")
	nbf, okNbf := jsonInt(payload, "nbf")
	iat, okIat := jsonInt(payload, "iat")
	if !okExp || !okNbf || !okIat {
		return nil, verifyFail(CodeInvalidToken, "missing time claims")
	}
	if exp <= nowSec-VerifyLeewaySeconds {
		return nil, verifyFail(CodeTokenExpired, "token expired")
	}
	if nbf > nowSec+VerifyLeewaySeconds {
		return nil, verifyFail(CodeInvalidToken, "token not yet valid")
	}
	if exp-iat > MaxConnectTokenLifetime {
		return nil, verifyFail(CodeInvalidToken, "token lifetime too long")
	}

	// 7. Version.
	ver, ok := jsonInt(payload, "t3cp_ver")
	if !ok || ver != ProtocolVersion {
		return nil, verifyFail(CodeInvalidToken, "unsupported protocol version")
	}

	// 8. Revocation epoch.
	epoch, ok := jsonInt(payload, "t3cp_epoch")
	if !ok {
		return nil, verifyFail(CodeInvalidToken, "missing epoch")
	}
	if epoch < ctx.RevocationEpoch {
		return nil, verifyFail(CodeStaleEpoch, "token predates revocation")
	}

	// 9. Device revocation.
	if ctx.Revoked {
		return nil, verifyFail(CodeDeviceRevoked, "device revoked")
	}

	// 10. Scope.
	scopeStr, _ := jsonString(payload, "scope")
	scopes := []string{}
	for _, part := range strings.Split(scopeStr, " ") {
		if part != "" {
			scopes = append(scopes, part)
		}
	}
	if !ScopesCover(scopes, required) {
		return nil, verifyFail(CodeInsufficientScope, "requires "+string(required))
	}

	sub, _ := jsonString(payload, "sub")
	jti, _ := jsonString(payload, "jti")
	return &ConnectClaims{
		Issuer:    ctx.Issuer,
		Subject:   sub,
		Audience:  audience,
		IssuedAt:  iat,
		NotBefore: nbf,
		ExpiresAt: exp,
		JTI:       jti,
		Scope:     scopeStr,
		Scopes:    scopes,
		Epoch:     epoch,
		Version:   ver,
		Raw:       payload,
	}, nil
}

// decodeSegment base64url-decodes a JWS segment and parses it as a JSON object. Arrays, null,
// scalars, and trailing data are rejected.
func decodeSegment(segment string) (map[string]json.RawMessage, bool) {
	raw, err := decodeB64URL(segment)
	if err != nil {
		return nil, false
	}
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return nil, false
	}
	return obj, true
}

// jsonString returns obj[key] if it is present and a JSON string.
func jsonString(obj map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := obj[key]
	if !ok {
		return "", false
	}
	return rawString(raw)
}

func rawString(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// jsonInt returns obj[key] if it is present, a JSON number, and an integer value (a JSON number
// whose value is integral, so 1.0 and 1e3 are accepted as in JavaScript's Number.isInteger)
// within ±(2^53 − 1).
func jsonInt(obj map[string]json.RawMessage, key string) (int64, bool) {
	raw, ok := obj[key]
	if !ok {
		return 0, false
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || !(raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9')) {
		return 0, false
	}
	if n, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
		if n > maxSafeInteger || n < -maxSafeInteger {
			return 0, false
		}
		return n, true
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) || f != math.Trunc(f) {
		return 0, false
	}
	if f > maxSafeInteger || f < -maxSafeInteger {
		return 0, false
	}
	return int64(f), true
}

// singleAudience accepts aud as a string or a one-element array of strings.
func singleAudience(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return "", false
	}
	if raw[0] == '"' {
		return rawString(raw)
	}
	if raw[0] != '[' {
		return "", false
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil || len(arr) != 1 {
		return "", false
	}
	return rawString(arr[0])
}

// tokenKID extracts the header kid without verifying anything. It is used to decide whether the
// JWKS cache needs a refresh before verification.
func tokenKID(token string) (string, bool) {
	dot := strings.IndexByte(token, '.')
	if dot < 0 {
		return "", false
	}
	header, ok := decodeSegment(token[:dot])
	if !ok {
		return "", false
	}
	return jsonString(header, "kid")
}
