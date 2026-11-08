package cli

import "context"

func (a *ClaudeAdapter) Models(context.Context, string) ([]Model, error) {
	options := []ModelOption{
		{ID: "effort", Label: "Thinking", Type: "select", Values: []string{"low", "medium", "high", "xhigh", "max"}, Default: "high"},
		{ID: "fastMode", Label: "Fast mode", Type: "boolean", Default: false},
	}
	return []Model{
		{ID: "default", Label: "Default", Options: options},
		{ID: "claude-opus-5-5", Label: "Claude Opus 5.5", Options: options},
		{ID: "claude-sonnet-5-5", Label: "Claude Sonnet 5.5", Options: options},
		{ID: "claude-haiku-5-5", Label: "Claude Haiku 5.5", Options: options},
	}, nil
}
