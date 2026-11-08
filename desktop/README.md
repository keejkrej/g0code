# MyGo desktop MVP

The native desktop app uses MyGo `v0.8.3`, with Go `1.27.3` and the MyGo CLI pinned as a Go tool. It needs no frontend, webview, Bun, or cgo.

## Features

- Durable local thread list and selection.
- Message/run timeline with tool activity and delegated child links.
- Codex, Claude, Pi, Agy, and Fx provider selection.
- Approval-required, auto-accept-edits, auto, and explicit full-access modes.
- Parent/child pending-request controls.
- Delegation, native interruption, and child result publication.
- Shared SQLite state and leased outbox worker with heartbeat renewal.

## Develop and build

```sh
go tool mygo dev
go tool mygo vet .
go tool mygo build
```

The default workspace is the current directory. `cmd/g0code` also accepts `-workspace`, `-provider`, and `-state` when run as a Go binary. State defaults to `~/.g0code/orchestration.sqlite`.

## Tests

Headless MyGo `ui.Tester` tests cover thread creation, prompt/response rendering, approvals, and delegation without a visible window.

```sh
go test ./desktop
G0CODE_CAPTURE=/tmp/g0code-mvp.png go test ./desktop -run TestDesktopThreadPromptAndApproval
```

The optional capture writes the actual rendered frame as PNG.

Live tests use real Codex coding tasks in temporary workspaces:

```sh
G0CODE_LIVE=1 go test -tags=live ./desktop -run 'TestLiveDesktop(WorkspaceEdit|DelegatedWorkspaceEdit)' -v
```

The delegated test verifies child file creation, parent result publication, approval routing, and native parent interruption.
