package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DeviceAuthorizer holds the device-side verification state (issuer, device ID, revocation epoch,
// revoked flag) and verifies connect tokens against a JWKS cache. It is safe for concurrent use;
// heartbeat results can be applied while requests are being verified.
type DeviceAuthorizer struct {
	jwks *JWKSCache
	now  func() time.Time // test seam

	mu       sync.RWMutex
	issuer   string
	deviceID string
	epoch    int64
	revoked  bool
}

// NewDeviceAuthorizer creates an authorizer for a paired device.
func NewDeviceAuthorizer(issuer, deviceID string, revocationEpoch int64, jwks *JWKSCache) *DeviceAuthorizer {
	return &DeviceAuthorizer{issuer: issuer, deviceID: deviceID, epoch: revocationEpoch, jwks: jwks}
}

// NewDeviceAuthorizerFromState creates an authorizer from persisted device state, with a JWKS
// cache for state.JWKSURI.
func NewDeviceAuthorizerFromState(state *DeviceState, client *http.Client) *DeviceAuthorizer {
	return NewDeviceAuthorizer(state.Issuer, state.DeviceID, state.RevocationEpoch, NewJWKSCache(state.JWKSURI, client))
}

// JWKS returns the authorizer's key cache.
func (a *DeviceAuthorizer) JWKS() *JWKSCache { return a.jwks }

// Snapshot returns the current verification state (without keys).
func (a *DeviceAuthorizer) Snapshot() (issuer, deviceID string, revocationEpoch int64, revoked bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.issuer, a.deviceID, a.epoch, a.revoked
}

// RevocationEpoch returns the stored revocation epoch.
func (a *DeviceAuthorizer) RevocationEpoch() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.epoch
}

// Revoked reports whether the device has been told it is revoked.
func (a *DeviceAuthorizer) Revoked() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.revoked
}

// SetRevocationEpoch stores a new epoch. The stored epoch never decreases.
func (a *DeviceAuthorizer) SetRevocationEpoch(epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if epoch > a.epoch {
		a.epoch = epoch
	}
}

// MarkRevoked makes the authorizer reject every connect token (heartbeat returned 403
// device_revoked). It cannot be undone; re-pairing creates a new authorizer.
func (a *DeviceAuthorizer) MarkRevoked() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.revoked = true
}

// ApplyHeartbeat updates state from a successful heartbeat response: the revocation epoch (never
// decreasing), a "revoked" status, and a changed jwksUri.
func (a *DeviceAuthorizer) ApplyHeartbeat(resp *HeartbeatResponse) {
	if resp == nil {
		return
	}
	a.SetRevocationEpoch(resp.RevocationEpoch)
	if resp.Status == "revoked" {
		a.MarkRevoked()
	}
	if resp.JWKSURI != "" && a.jwks != nil && a.jwks.uri() != resp.JWKSURI {
		a.jwks.SetURI(resp.JWKSURI)
	}
}

// ApplyHeartbeatError marks the device revoked if err is ErrDeviceRevoked. It reports whether
// the device is now revoked.
func (a *DeviceAuthorizer) ApplyHeartbeatError(err error) bool {
	if isDeviceRevoked(err) {
		a.MarkRevoked()
		return true
	}
	return false
}

// Verify checks token against the current state, refreshing the JWKS on an unknown kid when the
// rate limit allows. required may be "" for operations that only need a valid token.
func (a *DeviceAuthorizer) Verify(ctx context.Context, token string, required Scope) (*ConnectClaims, error) {
	var keys JWKS
	if a.jwks != nil {
		kid, _ := tokenKID(token)
		keys, _ = a.jwks.Ensure(ctx, kid) // fetch errors surface as "unknown kid"
	}
	issuer, deviceID, epoch, revoked := a.Snapshot()
	now := time.Now()
	if a.now != nil {
		now = a.now()
	}
	return VerifyConnectToken(token, VerificationContext{
		Issuer:          issuer,
		DeviceID:        deviceID,
		RevocationEpoch: epoch,
		Revoked:         revoked,
		JWKS:            keys,
		Now:             now,
	}, required)
}

// ScopeFunc decides which scope a request needs. needsScope=false means a valid token suffices.
type ScopeFunc func(r *http.Request) (scope Scope, needsScope bool)

// RequireScope returns a ScopeFunc that always requires s.
func RequireScope(s Scope) ScopeFunc {
	return func(*http.Request) (Scope, bool) { return s, true }
}

// AnyValidToken is a ScopeFunc that only requires a valid token. Handlers can then gate
// individual operations with CheckScope (for example per MCP tool after parsing the body).
func AnyValidToken(*http.Request) (Scope, bool) { return "", false }

type claimsKey struct{}

// WithClaims returns a context carrying verified claims.
func WithClaims(ctx context.Context, claims *ConnectClaims) context.Context {
	return context.WithValue(ctx, claimsKey{}, claims)
}

// ClaimsFromContext returns the verified claims stored by the middleware.
func ClaimsFromContext(ctx context.Context) (*ConnectClaims, bool) {
	c, ok := ctx.Value(claimsKey{}).(*ConnectClaims)
	return c, ok && c != nil
}

// CheckScope returns a *VerifyError with insufficient_scope if the claims in ctx do not cover
// required, or invalid_token if ctx carries no claims.
func CheckScope(ctx context.Context, required Scope) error {
	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return verifyFail(CodeInvalidToken, "no verified connect token")
	}
	if !claims.HasScope(required) {
		return verifyFail(CodeInsufficientScope, "requires "+string(required))
	}
	return nil
}

// BearerToken extracts the credential from an "Authorization: Bearer <value>" header. The scheme
// is matched case-insensitively.
func BearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	scheme, value, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	value = strings.TrimSpace(value)
	return value, value != ""
}

// ErrorBody is the protocol error body {"error": "<code>", "message": "<text>"}.
type ErrorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// WriteError writes a protocol error response.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorBody{Error: code, Message: message})
}

// WriteVerifyError writes the 401/403 response for a verification failure, including an RFC 6750
// WWW-Authenticate challenge.
func WriteVerifyError(w http.ResponseWriter, err error) {
	verr, ok := err.(*VerifyError)
	if !ok {
		verr = verifyFail(CodeInvalidToken, err.Error())
	}
	challenge := `Bearer error="invalid_token"`
	if verr.Code == CodeInsufficientScope {
		challenge = `Bearer error="insufficient_scope"`
	}
	w.Header().Set("WWW-Authenticate", challenge)
	WriteError(w, verr.HTTPStatus(), string(verr.Code), verr.Reason)
}

// Middleware returns net/http middleware that requires a valid connect token in the
// Authorization header, enforces the scope chosen by scopeFor (nil means AnyValidToken), answers
// failures with 401/403 and the protocol error body, and stores the claims in the request
// context (see ClaimsFromContext).
func (a *DeviceAuthorizer) Middleware(scopeFor ScopeFunc) func(http.Handler) http.Handler {
	if scopeFor == nil {
		scopeFor = AnyValidToken
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := BearerToken(r)
			if !ok {
				WriteVerifyError(w, verifyFail(CodeInvalidToken, "missing bearer token"))
				return
			}
			scope, needs := scopeFor(r)
			if !needs {
				scope = ""
			}
			claims, err := a.Verify(r.Context(), token, scope)
			if err != nil {
				WriteVerifyError(w, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithClaims(r.Context(), claims)))
		})
	}
}
