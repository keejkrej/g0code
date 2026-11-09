# controlplane

Go implementation of the t3code control-plane protocol v1. It's standard library only. The normative spec is
`/Users/jack/workspace/t3code-control-plane/protocol/PROTOCOL.md`.

> Status: this is a library. The g0code device daemon (pairing CLI, heartbeat loop, authenticated
> HTTP/MCP listener on the Tailscale interface) is **not wired up yet**.

## Contents

| Area | API |
| --- | --- |
| Constants and scopes | `ProtocolVersion`, `ScopeRead`, `ScopeWrite`, `ScopeForTool`, `RequiredScopeForMethod`, `ScopesCover` |
| Codes and keys | `NormalizeUserCode`, `FormatUserCode`, `PublicKeyFingerprint`, `PairingExchangeMessage`, `FormatTimestamp`, `JWKThumbprint` |
| Device identity | `GenerateDeviceIdentity`, `LoadOrCreateDeviceIdentity` (PKCS#8 PEM, `0600`), `(*DeviceIdentity).SignPairingExchange` |
| Token verification | `VerifyConnectToken` → `*ConnectClaims` or `*VerifyError{Code, Reason}` |
| Device runtime | `JWKSCache` (refetches an unknown `kid` at most once per 60 s), `DeviceAuthorizer`, `(*DeviceAuthorizer).Middleware`, `ClaimsFromContext`, `CheckScope` |
| HTTP client | `Client.Discover`, `StartPairing`, `ExchangePairing`, `PollPairing`, `Heartbeat`, `StartClientAuthorization`, `PollClientToken`, `Me`, `ListDevices`, `MintConnectToken`, `RevokeDevice`, `RevokeClientToken` |
| Persistence | `DeviceState` (`LoadDeviceState`, `Save` with `0600`, `DeleteDeviceState`) |

Control-plane errors are returned as `*APIError{Status, Code, Message}`. `errors.Is` matches the sentinel for
the protocol code, for example `errors.Is(err, controlplane.ErrDeviceRevoked)`.

## Device side

```go
cp := controlplane.NewClient("https://cp.example.com", nil)
if _, err := cp.Discover(ctx); err != nil { … }

id, _ := controlplane.LoadOrCreateDeviceIdentity(filepath.Join(dir, "device.key"))
start, _ := cp.StartPairing(ctx, id, controlplane.PairingRequest{
	Hostname: host, Platform: "darwin", Arch: "arm64",
	Client:    controlplane.ClientInfo{Kind: "g0code", Version: version},
	Tailscale: controlplane.TailscaleInfo{MagicDNSName: "mac-mini.tailnet-abc.ts.net"},
	Port:      7420, Capabilities: []string{"mcp", "orchestration"},
})
fmt.Println("Approve at", start.VerificationURIComplete, "code", start.Code, "key", id.Fingerprint())
res, err := cp.PollPairing(ctx, id, start) // handles authorization_pending and slow_down
state := controlplane.NewDeviceState(cp.BaseURL, res)
_ = state.Save(filepath.Join(dir, "device.json"))

auth := controlplane.NewDeviceAuthorizerFromState(state, nil)
// Heartbeat loop:
hb, err := cp.Heartbeat(ctx, state.DeviceID, state.DeviceCredential, controlplane.HeartbeatRequest{Port: 7420})
if auth.ApplyHeartbeatError(err) { /* delete credential, require re-pairing */ }
auth.ApplyHeartbeat(hb)

// Device API: verify the token on every request (or WebSocket upgrade) and gate MCP tools per call.
http.Handle("/mcp", auth.Middleware(controlplane.AnyValidToken)(mcpHandler))
// Inside mcpHandler, after parsing the JSON-RPC request:
if scope, needs := controlplane.RequiredScopeForMethod(method, toolName); needs {
	if err := controlplane.CheckScope(r.Context(), scope); err != nil { controlplane.WriteVerifyError(w, err); return }
}
```

The middleware answers `401` (steps 1–9) or `403` (`insufficient_scope`) with
`{"error": "<code>", "message": "…"}` and stores `*ConnectClaims` in the request context. The device should
close long-lived WebSockets when `claims.Expiry()` passes.

## GUI side

```go
auth, _ := cp.StartClientAuthorization(ctx, controlplane.ClientAuthorizationRequest{
	ClientName: "g0code desktop on mac-mini", Client: controlplane.ClientInfo{Kind: "g0code", Version: version},
})
openBrowser(auth.VerificationURIComplete)
tok, _ := cp.PollClientToken(ctx, auth)
devices, _ := cp.ListDevices(ctx, tok.AccessToken)
ct, _ := cp.MintConnectToken(ctx, tok.AccessToken, devices[0].ID, controlplane.ScopeWrite)
// Connect to ct.Endpoint with "Authorization: Bearer " + ct.Token.
```

## Tests

`testdata/test-vectors.json` is copied verbatim from the control-plane repo's `protocol/test-vectors.json`.
Re-copy it whenever the protocol changes. The tests check every code-normalization, fingerprint,
pairing-exchange, and connect-token vector. They also run `httptest` coverage for the client flows, the JWKS
cache, and the middleware.

```sh
go test -race ./controlplane/...
```
