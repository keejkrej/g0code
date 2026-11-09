package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// recordingSleep replaces Client.sleep and records requested waits.
type recordingSleep struct {
	mu    sync.Mutex
	waits []time.Duration
	max   int
}

func (r *recordingSleep) sleep(ctx context.Context, d time.Duration) error {
	r.mu.Lock()
	r.waits = append(r.waits, d)
	n := len(r.waits)
	r.mu.Unlock()
	if r.max > 0 && n > r.max {
		return errors.New("too many polls")
	}
	return ctx.Err()
}

func TestDiscoverUsesEndpoints(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case WellKnownPath:
			writeJSON(w, 200, map[string]any{
				"protocolVersion": 1, "issuer": srv.URL, "jwksUri": srv.URL + "/.well-known/jwks.json",
				"devicePairingEndpoint":       srv.URL + "/custom/pairings",
				"clientAuthorizationEndpoint": srv.URL + "/v1/client-authorizations",
				"clientTokenEndpoint":         srv.URL + "/v1/client-authorizations/token",
				"devicesEndpoint":             srv.URL + "/v1/devices",
				"scopesSupported":             []string{"orchestration:read", "orchestration:write"},
				"connectTokenTtlSeconds":      300, "heartbeatIntervalSeconds": 60, "futureField": true,
			})
		case "/custom/pairings":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req["publicKey"] == "" || req["hostname"] != "mac-mini" {
				t.Errorf("bad pairing body %v", req)
			}
			if _, ok := req["capabilities"].([]any); !ok {
				t.Errorf("capabilities should be an array: %v", req["capabilities"])
			}
			writeJSON(w, 201, map[string]any{"pairingId": "dpr_1", "code": "BCDF-GHJK", "interval": 5,
				"verificationUri": srv.URL + "/pair", "verificationUriComplete": srv.URL + "/pair?code=BCDF-GHJK",
				"expiresAt": "2026-11-09T20:10:00.000Z"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL+"/", srv.Client())
	d, err := c.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.ConnectTokenTTLSeconds != 300 || d.JWKSURI == "" || c.Discovery == nil {
		t.Fatalf("discovery = %+v", d)
	}
	id, _ := GenerateDeviceIdentity()
	start, err := c.StartPairing(context.Background(), id, PairingRequest{
		Hostname: "mac-mini", Platform: "darwin", Arch: "arm64",
		Client: ClientInfo{Kind: "g0code", Version: "0.1.0"}, Tailscale: TailscaleInfo{IP: "100.101.102.103"}, Port: 7420,
	})
	if err != nil {
		t.Fatal(err)
	}
	if start.PairingID != "dpr_1" || start.ExpiresAt.IsZero() {
		t.Fatalf("start = %+v", start)
	}
}

func TestPollPairingPendingSlowDownApproved(t *testing.T) {
	id, _ := GenerateDeviceIdentity()
	fixed := time.Date(2026, 11, 9, 20, 1, 0, 0, time.UTC)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/device-pairings/BCDFGHJK/exchange" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var body PairingExchangeRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Timestamp != "2026-11-09T20:01:00.000Z" {
			t.Errorf("timestamp = %q", body.Timestamp)
		}
		msg := PairingExchangeMessage("BCDFGHJK", "dpr_1", body.Timestamp)
		if want := id.Sign([]byte(msg)); body.Signature != want {
			t.Errorf("signature mismatch")
		}
		switch calls.Add(1) {
		case 1:
			writeJSON(w, 400, ErrorBody{Error: "authorization_pending", Message: "waiting"})
		case 2:
			writeJSON(w, 400, ErrorBody{Error: "slow_down", Message: "slow"})
		case 3:
			writeJSON(w, 400, ErrorBody{Error: "authorization_pending"})
		default:
			writeJSON(w, 200, map[string]any{"deviceId": "dev_1", "deviceCredential": "t3cpd_x", "ownerUserId": "usr_1",
				"issuer": "https://cp.test", "jwksUri": "https://cp.test/.well-known/jwks.json", "revocationEpoch": 3,
				"heartbeatIntervalSeconds": 60})
		}
	}))
	defer srv.Close()

	rs := &recordingSleep{max: 10}
	c := NewClient(srv.URL, srv.Client())
	c.sleep = rs.sleep
	c.now = func() time.Time { return fixed }
	res, err := c.PollPairing(context.Background(), id, &PairingStart{PairingID: "dpr_1", Code: "BCDF-GHJK", Interval: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.DeviceID != "dev_1" || res.RevocationEpoch != 3 || res.DeviceCredential != "t3cpd_x" {
		t.Fatalf("result = %+v", res)
	}
	want := []time.Duration{2 * time.Second, 2 * time.Second, 7 * time.Second, 7 * time.Second}
	if len(rs.waits) != len(want) {
		t.Fatalf("waits = %v", rs.waits)
	}
	for i := range want {
		if rs.waits[i] != want[i] {
			t.Fatalf("waits = %v want %v", rs.waits, want)
		}
	}
}

func TestPollPairingTerminalErrors(t *testing.T) {
	for _, tc := range []struct {
		code     string
		status   int
		sentinel error
	}{
		{"access_denied", 400, ErrAccessDenied},
		{"expired_token", 400, ErrExpiredToken},
		{"invalid_signature", 401, ErrInvalidSignature},
		{"not_found", 404, ErrNotFound},
	} {
		t.Run(tc.code, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, ErrorBody{Error: tc.code, Message: "nope"})
			}))
			defer srv.Close()
			c := NewClient(srv.URL, srv.Client())
			rs := &recordingSleep{max: 3}
			c.sleep = rs.sleep
			id, _ := GenerateDeviceIdentity()
			_, err := c.PollPairing(context.Background(), id, &PairingStart{PairingID: "dpr_1", Code: "BCDFGHJK"})
			if !errors.Is(err, tc.sentinel) {
				t.Fatalf("err = %v", err)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Code != tc.code || apiErr.Status != tc.status || apiErr.Message != "nope" {
				t.Fatalf("apiErr = %+v", apiErr)
			}
			if len(rs.waits) != 1 || rs.waits[0] != 5*time.Second {
				t.Fatalf("waits = %v (default interval should be 5s)", rs.waits)
			}
		})
	}
}

func TestPollHonorsContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 400, ErrorBody{Error: "authorization_pending"})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, srv.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.PollClientToken(ctx, &ClientAuthorization{DeviceCode: "dc", Interval: 0})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestHeartbeat(t *testing.T) {
	revoked := atomic.Bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/devices/dev_1/heartbeat" || r.Header.Get("Authorization") != "Bearer t3cpd_secret" {
			writeJSON(w, 401, ErrorBody{Error: "invalid_credential", Message: "bad"})
			return
		}
		body, _ := io.ReadAll(r.Body)
		if revoked.Load() {
			writeJSON(w, 403, ErrorBody{Error: "device_revoked", Message: "revoked"})
			return
		}
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		if _, ok := m["tailscale"]; ok {
			t.Errorf("absent fields must be omitted: %s", body)
		}
		if m["port"] != float64(7420) {
			t.Errorf("port missing: %s", body)
		}
		writeJSON(w, 200, map[string]any{"deviceId": "dev_1", "status": "active", "revocationEpoch": 4,
			"issuer": "https://cp.test", "jwksUri": "https://cp.test/jwks2", "heartbeatIntervalSeconds": 60,
			"serverTime": "2026-11-09T20:02:00.000Z"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, srv.Client())
	auth := NewDeviceAuthorizer("https://cp.test", "dev_1", 1, NewJWKSCache("https://cp.test/jwks", nil))
	resp, err := c.Heartbeat(context.Background(), "dev_1", "t3cpd_secret", HeartbeatRequest{Port: 7420})
	if err != nil {
		t.Fatal(err)
	}
	auth.ApplyHeartbeat(resp)
	if auth.RevocationEpoch() != 4 || auth.Revoked() || auth.JWKS().uri() != "https://cp.test/jwks2" {
		t.Fatalf("authorizer not updated: epoch=%d revoked=%v", auth.RevocationEpoch(), auth.Revoked())
	}
	auth.SetRevocationEpoch(2)
	if auth.RevocationEpoch() != 4 {
		t.Fatal("epoch must not decrease")
	}

	_, err = c.Heartbeat(context.Background(), "dev_1", "wrong", HeartbeatRequest{})
	if !errors.Is(err, ErrInvalidCredential) || errors.Is(err, ErrDeviceRevoked) {
		t.Fatalf("err = %v", err)
	}

	revoked.Store(true)
	_, err = c.Heartbeat(context.Background(), "dev_1", "t3cpd_secret", HeartbeatRequest{Port: 7420})
	if !errors.Is(err, ErrDeviceRevoked) {
		t.Fatalf("err = %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 403 {
		t.Fatalf("apiErr = %+v", apiErr)
	}
	if !auth.ApplyHeartbeatError(err) || !auth.Revoked() {
		t.Fatal("authorizer should be revoked")
	}
}

func TestGUIFlows(t *testing.T) {
	var tokenCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/client-authorizations":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req["clientName"] != "g0code desktop" {
				t.Errorf("body %v", req)
			}
			writeJSON(w, 201, map[string]any{"authorizationId": "cau_1", "deviceCode": "dc_secret", "userCode": "BCDF-GHJK",
				"verificationUri": "u", "verificationUriComplete": "u?code=BCDF-GHJK", "expiresAt": "2026-11-09T20:10:00.000Z", "interval": 1})
		case r.Method == "POST" && r.URL.Path == "/v1/client-authorizations/token":
			var req map[string]string
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req["deviceCode"] != "dc_secret" {
				t.Errorf("deviceCode = %q", req["deviceCode"])
			}
			if tokenCalls.Add(1) < 2 {
				writeJSON(w, 400, ErrorBody{Error: "authorization_pending"})
				return
			}
			writeJSON(w, 200, map[string]any{"accessToken": "t3cpc_tok", "tokenType": "Bearer",
				"scope": "orchestration:read orchestration:write", "userId": "usr_1", "expiresAt": "2027-02-07T20:00:00.000Z"})
		case authz != "Bearer t3cpc_tok":
			writeJSON(w, 401, ErrorBody{Error: "invalid_token", Message: "expired"})
		case r.Method == "GET" && r.URL.Path == "/v1/me":
			writeJSON(w, 200, map[string]any{"user": map[string]any{"id": "usr_1", "name": nil, "username": "jack",
				"email": "j@example.com", "avatarUrl": nil}, "scope": "orchestration:read"})
		case r.Method == "GET" && r.URL.Path == "/v1/devices":
			writeJSON(w, 200, map[string]any{"devices": []any{
				map[string]any{"id": "dev_1", "status": "active", "online": true, "endpoint": "http://m.ts.net:7420",
					"tailscale": map[string]any{"magicDnsName": "m.ts.net"}, "port": 7420, "createdAt": "2026-11-09T20:00:00.000Z",
					"lastSeenAt": "2026-11-09T20:01:00.000Z", "publicKeyFingerprint": "SHA256:abc"},
				map[string]any{"id": "dev_2", "status": "revoked", "online": false, "createdAt": "2026-11-09T20:00:00.000Z", "lastSeenAt": nil},
			}})
		case r.Method == "POST" && r.URL.Path == "/v1/devices/dev_1/connect-tokens":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			scopes, _ := req["scopes"].([]any)
			if len(scopes) != 1 || scopes[0] != "orchestration:write" {
				t.Errorf("scopes = %v", req["scopes"])
			}
			writeJSON(w, 201, map[string]any{"token": "jwt", "tokenType": "Bearer", "scope": "orchestration:read orchestration:write",
				"expiresIn": 300, "expiresAt": "2026-11-09T20:05:00.000Z", "deviceId": "dev_1", "endpoint": "http://m.ts.net:7420"})
		case r.Method == "POST" && r.URL.Path == "/v1/devices/dev_2/connect-tokens":
			writeJSON(w, 409, ErrorBody{Error: "device_revoked", Message: "revoked"})
		case r.Method == "POST" && r.URL.Path == "/v1/client-tokens/revoke":
			w.WriteHeader(204)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ctx := context.Background()
	c := NewClient(srv.URL, srv.Client())
	rs := &recordingSleep{max: 5}
	c.sleep = rs.sleep
	auth, err := c.StartClientAuthorization(ctx, ClientAuthorizationRequest{ClientName: "g0code desktop",
		Client: ClientInfo{Kind: "g0code", Version: "0.1.0"}, Scopes: []Scope{ScopeRead, ScopeWrite}})
	if err != nil {
		t.Fatal(err)
	}
	tok, err := c.PollClientToken(ctx, auth)
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "t3cpc_tok" || len(rs.waits) != 2 || rs.waits[0] != time.Second {
		t.Fatalf("tok=%+v waits=%v", tok, rs.waits)
	}
	me, err := c.Me(ctx, tok.AccessToken)
	if err != nil || me.User.Username != "jack" {
		t.Fatalf("me=%+v err=%v", me, err)
	}
	devices, err := c.ListDevices(ctx, tok.AccessToken)
	if err != nil || len(devices) != 2 || devices[0].Endpoint == "" || devices[1].LastSeenAt != nil || devices[0].LastSeenAt == nil {
		t.Fatalf("devices=%+v err=%v", devices, err)
	}
	ct, err := c.MintConnectToken(ctx, tok.AccessToken, "dev_1", ScopeWrite)
	if err != nil || ct.Token != "jwt" || ct.ExpiresIn != 300 {
		t.Fatalf("ct=%+v err=%v", ct, err)
	}
	if _, err := c.MintConnectToken(ctx, tok.AccessToken, "dev_2"); !errors.Is(err, ErrDeviceRevoked) {
		t.Fatalf("err = %v", err)
	}
	if err := c.RevokeClientToken(ctx, tok.AccessToken); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Me(ctx, "t3cpc_other"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("err = %v", err)
	}
}

func TestAPIErrorNonProtocolBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream exploded", 502)
	}))
	defer srv.Close()
	_, err := NewClient(srv.URL, srv.Client()).Me(context.Background(), "x")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 502 || apiErr.Code != "" || !strings.Contains(apiErr.Message, "exploded") {
		t.Fatalf("err = %#v", err)
	}
}

func TestIdentityAndStatePersistence(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "nested", "device.key")
	id, err := LoadOrCreateDeviceIdentity(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(keyPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key perms = %v err=%v", info.Mode().Perm(), err)
	}
	again, err := LoadOrCreateDeviceIdentity(keyPath)
	if err != nil || again.PublicKeyBase64() != id.PublicKeyBase64() {
		t.Fatalf("reload mismatch: %v", err)
	}
	fp, _ := PublicKeyFingerprint(id.PublicKeyBase64())
	if fp != id.Fingerprint() {
		t.Fatal("fingerprint mismatch")
	}
	if _, err := ParseDeviceIdentityPEM([]byte("garbage")); err == nil {
		t.Fatal("expected parse error")
	}

	statePath := filepath.Join(dir, "state.json")
	st := NewDeviceState("https://cp.test", &PairingResult{DeviceID: "dev_1", DeviceCredential: "t3cpd_x",
		Issuer: "https://cp.test", JWKSURI: "https://cp.test/jwks", RevocationEpoch: 2})
	if err := st.Save(statePath); err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(statePath)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state perms = %v", info.Mode().Perm())
	}
	raw, _ := os.ReadFile(statePath)
	for _, field := range []string{`"deviceId"`, `"deviceCredential"`, `"issuer"`, `"revocationEpoch"`, `"controlPlaneUrl"`} {
		if !strings.Contains(string(raw), field) {
			t.Errorf("state missing %s: %s", field, raw)
		}
	}
	loaded, err := LoadDeviceState(statePath)
	if err != nil || *loaded != *st {
		t.Fatalf("loaded = %+v err=%v", loaded, err)
	}
	if err := DeleteDeviceState(statePath); err != nil {
		t.Fatal(err)
	}
	if err := DeleteDeviceState(statePath); err != nil {
		t.Fatal(err)
	}
	if err := (&DeviceState{DeviceID: "x"}).Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}
