package cli

import (
	"os"
	"path/filepath"
)

const piPermissionExtension = `export default function(pi) {
  const readOnly = new Set(["read", "grep", "find", "ls"]);
  const edits = new Set(["edit", "write"]);
  pi.on("tool_call", async (event, ctx) => {
    const mode = process.env.G0CODE_RUNTIME_MODE || "approval-required";
    if (mode === "full-access" || readOnly.has(event.toolName)) return;
    if (mode === "auto-accept-edits" && edits.has(event.toolName)) return;
    const approved = await ctx.ui.confirm(
      "Allow " + event.toolName + "?",
      JSON.stringify(event.input),
      { signal: ctx.signal },
    );
    if (!approved) {
      return { block: true, reason: event.toolName + " was declined in g0code." };
    }
  });
}
`

func (a *PiAdapter) permissionExtension() (string, error) {
	dir := a.SessionDir
	if dir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(cache, "g0code", "pi")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "g0code-permission.ts")
	if err := os.WriteFile(path, []byte(piPermissionExtension), 0o600); err != nil {
		return "", err
	}
	return path, nil
}
