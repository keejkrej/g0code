# Remote devices: Tailscale-first architecture

## Decision

Use direct connections over Tailscale between the MyGo GUI and user-owned macOS/Linux devices. Vercel hosts only the control plane. Cloudflare Tunnel and a general-purpose traffic relay are not required for the first version.

This fits private devices such as a Mac mini or DGX Spark that have internet access but no public IP. Tailscale supplies private reachability and WireGuard encryption; the application still authenticates and authorizes every request.

## Stack

- **Control plane:** Next.js + TypeScript on Vercel.
- **Identity:** Vercel OAuth through a Vercel integration, with the control plane mapping Vercel users to owned devices.
- **Database:** Postgres (Neon) with Drizzle migrations. No Redis is necessary initially.
- **GUI:** MyGo desktop application.
- **Device daemon:** Go on macOS and Linux, running the orchestrator, MCP server, and a small HTTP/WebSocket device API.
- **Network:** Tailscale IP or MagicDNS name. Devices must share a tailnet or use Tailscale node sharing/ACLs.

Do not put long-lived device tunnels in Vercel Functions. If direct connectivity later needs a fallback, add a separate long-running Go relay without changing the control-plane or device identity model.

## Registration and pairing

1. The device CLI generates an Ed25519 device keypair; the private key stays in the OS keychain or a permission-restricted file.
2. The CLI requests a one-time pairing code from the control plane and displays a verification URL.
3. The user signs in with Vercel OAuth and approves the device. The control plane stores the device public key, owner, hostname, Tailscale IP/MagicDNS name, and requested capabilities.
4. The daemon exchanges the pairing code for a long-lived device credential and starts heartbeat reports.
5. The GUI lists devices from the control plane, but connects to the selected device directly over Tailscale.

Suggested control-plane endpoints:

```text
POST /v1/device-pairings
GET  /v1/device-pairings/:code
POST /v1/device-pairings/:code/approve
POST /v1/device-pairings/:code/exchange
GET  /v1/devices
POST /v1/devices/:id/connect-tokens
POST /v1/devices/:id/heartbeat
POST /v1/devices/:id/revoke
```

## Connection authorization

Tailscale reachability alone is not application authorization.

- The GUI asks the control plane for a short-lived token whose audience is exactly one device and whose scope names allowed operations.
- The device verifies the token's signature, expiry, audience, scope, and revocation epoch before accepting MCP/WebSocket requests.
- Mutating orchestration calls require an explicit `orchestration:write` scope; read-only clients receive `orchestration:read`.
- Device revocation increments the revocation epoch, immediately invalidating existing tokens.
- Pairing codes, device credentials, and tokens are logged only as metadata, never as secrets.

## Runtime flow

```text
device daemon
  -> pairing + heartbeat to Vercel control plane
  -> listens on its Tailscale address

MyGo GUI
  -> Vercel OAuth
  -> list devices + obtain short-lived device token
  -> direct Tailscale HTTP/WebSocket connection
  -> device verifies token
  -> MCP/orchestration requests execute locally on that device
```

A relay is only a later fallback for unsupported network layouts. It is not needed merely because devices have private IP addresses.
