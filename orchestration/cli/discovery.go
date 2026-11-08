package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func resolveProviderBinary(configured, name string) (string, error) {
	candidate := configured
	if candidate == "" {
		candidate = name
	}
	if strings.ContainsRune(candidate, filepath.Separator) {
		if executableFile(candidate) {
			return candidate, nil
		}
		return "", fmt.Errorf("provider executable %s is not executable", candidate)
	}
	if path, err := exec.LookPath(candidate); err == nil {
		return path, nil
	}
	for _, directory := range knownProviderDirectories() {
		path := filepath.Join(directory, candidate)
		if executableFile(path) {
			return path, nil
		}
	}
	// Finder-packaged macOS apps inherit a minimal PATH. Ask the user's shell
	// without interpolating the provider name into shell source.
	shell := "/bin/zsh"
	if runtime.GOOS == "windows" {
		shell = "where.exe"
	}
	if runtime.GOOS != "windows" {
		command := exec.Command(shell, "-lic", `command -v "$1"`, "g0code", candidate)
		command.Env = nativeEnvironment(nil)
		if output, err := command.Output(); err == nil {
			lines := strings.Fields(strings.TrimSpace(string(output)))
			if len(lines) > 0 {
				path := lines[len(lines)-1]
				if executableFile(path) {
					return path, nil
				}
			}
		}
	}
	return "", fmt.Errorf("provider executable %q was not found; install it or configure its absolute binary path", candidate)
}

func knownProviderDirectories() []string {
	directories := []string{"/opt/homebrew/bin", "/usr/local/bin"}
	home, err := os.UserHomeDir()
	if err != nil {
		return directories
	}
	return append(directories,
		filepath.Join(home, ".local", "bin"),
		filepath.Join(home, ".pi", "agent", "bin"),
		filepath.Join(home, ".local", "share", "mise", "shims"),
	)
}

func nativeEnvironment(overrides []string) []string {
	environment := os.Environ()
	if overrides != nil {
		environment = overrides
	}
	path := environmentValue(environment, "PATH")
	parts := strings.Split(path, string(os.PathListSeparator))
	if runtime.GOOS != "windows" {
		// Read PATH once per native process launch. Use only the last non-empty
		// line so interactive shell banners do not become path entries.
		command := exec.Command("/bin/zsh", "-lic", `printf '\n%s\n' "$PATH"`)
		command.Env = environment
		if output, err := command.Output(); err == nil {
			lines := strings.Split(strings.TrimSpace(string(output)), "\n")
			if len(lines) > 0 {
				loginPath := strings.TrimSpace(lines[len(lines)-1])
				parts = append(strings.Split(loginPath, string(os.PathListSeparator)), parts...)
			}
		}
	}
	parts = append(parts, knownProviderDirectories()...)
	seen := make(map[string]bool)
	merged := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || seen[part] {
			continue
		}
		seen[part] = true
		merged = append(merged, part)
	}
	return setEnvironmentValue(environment, "PATH", strings.Join(merged, string(os.PathListSeparator)))
}

func executableFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && (runtime.GOOS == "windows" || info.Mode().Perm()&0o111 != 0)
}

func environmentValue(environment []string, key string) string {
	prefix := key + "="
	for _, entry := range environment {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func setEnvironmentValue(environment []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environment)+1)
	found := false
	for _, entry := range environment {
		if strings.HasPrefix(entry, prefix) {
			if !found {
				result = append(result, prefix+value)
				found = true
			}
			continue
		}
		result = append(result, entry)
	}
	if !found {
		result = append(result, prefix+value)
	}
	return result
}
