package cli

import (
	"fmt"
	"strings"
)

type processError struct {
	Command string
	Err     error
	Stderr  string
}

func (e *processError) Error() string {
	message := fmt.Sprintf("%s: %v", e.Command, e.Err)
	if strings.TrimSpace(e.Stderr) != "" {
		message += ": " + strings.TrimSpace(e.Stderr)
	}
	return message
}

func (e *processError) Unwrap() error { return e.Err }

func boundedStderr(stderr string) string {
	const max = 16 * 1024
	if len(stderr) > max {
		return stderr[len(stderr)-max:]
	}
	return stderr
}
