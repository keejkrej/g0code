package controlplane

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// jwksServer serves the current key set and counts fetches.
type jwksServer struct {
	mu    sync.Mutex
	keys  JWKS
	count atomic.Int32
	srv   *httptest.Server
}

func newJWKSServer(keys ...JWK) *jwksServer {
	j := &jwksServer{keys: JWKS{Keys: keys}}
	j.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		j.count.Add(1)
		j.mu.Lock()
		defer j.mu.Unlock()
		writeJSON(w, 200, j.keys)
	}))
	return j
}

func (j *jwksServer) set(keys ...JWK) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.keys = JWKS{Keys: keys}
}

func TestJWKSCacheRefetchRateLimit(t *testing.T) {
	a, b := newTestSigner(1), newTestSigner(2)
	js := newJWKSServer(a.jwk())
	defer js.srv.Close()

	clock := time.Unix(tNow, 0)
	cache := NewJWKSCache(js.srv.URL, js.srv.Client())
	cache.now = func() time.Time { return clock }
	ctx := context.Background()

	keys, err := cache.Ensure(ctx, a.kid)
	if err != nil || !keys.Has(a.kid) || js.count.Load() != 1 {
		t.Fatalf("initial fetch: err=%v count=%d", err, js.count.Load())
	}
	// Known kid: no fetch.
	_, _ = cache.Ensure(ctx, a.kid)
	if js.count.Load() != 1 {
		t.Fatalf("known kid refetched: %d", js.count.Load())
	}
	// Rotation: unknown kid within 60 s of the last fetch must not refetch.
	js.set(a.jwk(), b.jwk())
	clock = clock.Add(30 * time.Second)
	keys, _ = cache.Ensure(ctx, b.kid)
	if keys.Has(b.kid) || js.count.Load() != 1 {
		t.Fatalf("refetched within 60s: count=%d", js.count.Load())
	}
	// After 60 s an unknown kid triggers exactly one refetch.
	clock = clock.Add(31 * time.Second)
	keys, _ = cache.Ensure(ctx, b.kid)
	if !keys.Has(b.kid) || js.count.Load() != 2 {
		t.Fatalf("expected refetch: count=%d", js.count.Load())
	}
	// A still-unknown kid right after is rate-limited again.
	_, _ = cache.Ensure(ctx, "nope")
	if js.count.Load() != 2 {
		t.Fatalf("unknown kid refetched too soon: %d", js.count.Load())
	}
}

func TestJWKSCacheConcurrentUnknownKid(t *testing.T) {
	a := newTestSigner(1)
	js := newJWKSServer(a.jwk())
	defer js.srv.Close()
	cache := NewJWKSCache(js.srv.URL, js.srv.Client())
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cache.Ensure(context.Background(), "unknown")
		}()
	}
	wg.Wait()
	if js.count.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", js.count.Load())
	}
}

type mwFixture struct {
	signer *testSigner
	auth   *DeviceAuthorizer
	srv    *httptest.Server
	jwks   *jwksServer
}

func newMWFixture(t *testing.T) *mwFixture {
	t.Helper()
	s := newTestSigner(9)
	js := newJWKSServer(s.jwk())
	t.Cleanup(js.srv.Close)
	auth := NewDeviceAuthorizer(tIssuer, tDevice, 2, NewJWKSCache(js.srv.URL, js.srv.Client()))
	auth.now = func() time.Time { return time.Unix(tNow, 0) }

	mux := http.NewServeMux()
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, found := ClaimsFromContext(r.Context())
		if !found {
			t.Error("claims missing from context")
		}
		writeJSON(w, 200, map[string]any{"sub": claims.Subject, "scopes": claims.Scopes})
	})
	mux.Handle("/read", auth.Middleware(RequireScope(ScopeRead))(ok))
	mux.Handle("/write", auth.Middleware(RequireScope(ScopeWrite))(ok))
	// MCP-style endpoint: token checked by middleware, tool scope checked inside the handler.
	mux.Handle("/mcp", auth.Middleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				Name string `json:"name"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if scope, needs := RequiredScopeForMethod(req.Method, req.Params.Name); needs {
			if err := CheckScope(r.Context(), scope); err != nil {
				WriteVerifyError(w, err)
				return
			}
		}
		w.WriteHeader(200)
	})))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &mwFixture{signer: s, auth: auth, srv: srv, jwks: js}
}

func (f *mwFixture) call(t *testing.T, path, authz, body string) (int, ErrorBody, http.Header) {
	t.Helper()
	method, reqBody := http.MethodGet, io.Reader(nil)
	if body != "" {
		method, reqBody = http.MethodPost, strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, f.srv.URL+path, reqBody)
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	resp, err := f.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var eb ErrorBody
	_ = json.NewDecoder(resp.Body).Decode(&eb)
	return resp.StatusCode, eb, resp.Header
}

func TestMiddleware(t *testing.T) {
	f := newMWFixture(t)
	readTok := f.signer.sign(baseClaims(map[string]any{"scope": "orchestration:read"}))
	writeTok := f.signer.sign(baseClaims(nil))

	cases := []struct {
		name, path, authz, body string
		status                  int
		code                    string
	}{
		{"missing header", "/read", "", "", 401, "invalid_token"},
		{"basic scheme", "/read", "Basic abc", "", 401, "invalid_token"},
		{"garbage token", "/read", "Bearer not-a-token", "", 401, "invalid_token"},
		{"read ok", "/read", "Bearer " + readTok, "", 200, ""},
		{"lowercase scheme", "/read", "bearer " + readTok, "", 200, ""},
		{"read token on write", "/write", "Bearer " + readTok, "", 403, "insufficient_scope"},
		{"write ok", "/write", "Bearer " + writeTok, "", 200, ""},
		{"expired", "/read", "Bearer " + f.signer.sign(baseClaims(map[string]any{"iat": tNow - 400, "nbf": tNow - 400, "exp": tNow - 100})), "", 401, "token_expired"},
		{"wrong audience", "/read", "Bearer " + f.signer.sign(baseClaims(map[string]any{"aud": "dev_other"})), "", 401, "wrong_audience"},
		{"stale epoch", "/read", "Bearer " + f.signer.sign(baseClaims(map[string]any{"t3cp_epoch": 1})), "", 401, "stale_epoch"},
		{"mcp initialize with empty scope", "/mcp", "Bearer " + f.signer.sign(baseClaims(map[string]any{"scope": ""})), `{"method":"initialize"}`, 200, ""},
		{"mcp read tool with read token", "/mcp", "Bearer " + readTok, `{"method":"tools/call","params":{"name":"t3_thread_read"}}`, 200, ""},
		{"mcp write tool with read token", "/mcp", "Bearer " + readTok, `{"method":"tools/call","params":{"name":"t3_thread_send"}}`, 403, "insufficient_scope"},
		{"mcp unknown tool with read token", "/mcp", "Bearer " + readTok, `{"method":"tools/call","params":{"name":"mystery"}}`, 403, "insufficient_scope"},
		{"mcp write tool with write token", "/mcp", "Bearer " + writeTok, `{"method":"tools/call","params":{"name":"delegate_task"}}`, 200, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, body, hdr := f.call(t, c.path, c.authz, c.body)
			if status != c.status {
				t.Fatalf("status = %d want %d (%+v)", status, c.status, body)
			}
			if c.code != "" {
				if body.Error != c.code || body.Message == "" {
					t.Fatalf("body = %+v want error %s", body, c.code)
				}
				if hdr.Get("WWW-Authenticate") == "" {
					t.Fatal("missing WWW-Authenticate")
				}
				if ct := hdr.Get("Content-Type"); ct != "application/json; charset=utf-8" {
					t.Fatalf("content-type = %q", ct)
				}
			}
		})
	}

	// Heartbeat-driven state changes take effect immediately.
	f.auth.ApplyHeartbeat(&HeartbeatResponse{RevocationEpoch: 3, Status: "active"})
	if status, body, _ := f.call(t, "/read", "Bearer "+readTok, ""); status != 401 || body.Error != "stale_epoch" {
		t.Fatalf("after epoch bump: %d %+v", status, body)
	}
	newer := f.signer.sign(baseClaims(map[string]any{"t3cp_epoch": 3, "scope": "orchestration:read"}))
	if status, _, _ := f.call(t, "/read", "Bearer "+newer, ""); status != 200 {
		t.Fatalf("newer epoch rejected: %d", status)
	}
	f.auth.MarkRevoked()
	if status, body, _ := f.call(t, "/read", "Bearer "+newer, ""); status != 401 || body.Error != "device_revoked" {
		t.Fatalf("after revoke: %d %+v", status, body)
	}
}

func TestMiddlewareKeyRotation(t *testing.T) {
	f := newMWFixture(t)
	clock := time.Unix(tNow, 0)
	f.auth.JWKS().now = func() time.Time { return clock }
	old := f.signer.sign(baseClaims(nil))
	if status, _, _ := f.call(t, "/read", "Bearer "+old, ""); status != 200 {
		t.Fatalf("status %d", status)
	}
	rotated := newTestSigner(10)
	f.jwks.set(f.signer.jwk(), rotated.jwk())
	tok := rotated.sign(baseClaims(nil))
	if status, body, _ := f.call(t, "/read", "Bearer "+tok, ""); status != 401 || body.Error != "invalid_token" {
		t.Fatalf("rotated key accepted within refetch window: %d %+v", status, body)
	}
	clock = clock.Add(61 * time.Second)
	if status, body, _ := f.call(t, "/read", "Bearer "+tok, ""); status != 200 {
		t.Fatalf("rotated key rejected after window: %d %+v", status, body)
	}
}
