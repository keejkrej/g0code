package cli

import (
	"context"
)

type piNativeModel struct {
	ID               string             `json:"id"`
	Name             string             `json:"name"`
	Provider         string             `json:"provider"`
	Reasoning        bool               `json:"reasoning"`
	ThinkingLevelMap map[string]*string `json:"thinkingLevelMap"`
}

func (a *PiAdapter) Models(ctx context.Context, cwd string) ([]Model, error) {
	binary := a.Binary
	if binary == "" {
		binary = "pi"
	}
	args := []string{"--mode", "rpc", "--no-tools", "--no-extensions", "--no-mcp", "--no-skills",
		"--no-prompt-templates", "--no-context-files", "--no-approve", "--offline"}
	if a.SessionDir != "" {
		args = append(args, "--session-dir", a.SessionDir)
	}
	client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: cwd, IDPrefix: "g0code-"})
	if err != nil {
		return nil, err
	}
	defer client.close()
	var state struct {
		Data struct {
			Model piNativeModel `json:"model"`
		} `json:"data"`
	}
	if err := client.requestRecord(ctx, map[string]any{"type": "get_state"}, &state); err != nil {
		return nil, err
	}
	var response struct {
		Data struct {
			Models []piNativeModel `json:"models"`
		} `json:"data"`
	}
	if err := client.requestRecord(ctx, map[string]any{"type": "get_available_models"}, &response); err != nil {
		return nil, err
	}
	models := make([]Model, 0, len(response.Data.Models))
	for _, native := range response.Data.Models {
		label := native.Name
		if label == "" {
			label = native.ID
		}
		model := Model{ID: native.Provider + "/" + native.ID, Label: label}
		levels := piThinkingLevels(native)
		if len(levels) > 0 {
			defaultLevel := "medium"
			if native.ThinkingLevelMap["high"] != nil {
				defaultLevel = "high"
			}
			model.Options = append(model.Options, ModelOption{ID: "thinking", Label: "Thinking",
				Type: "select", Values: levels, Default: defaultLevel})
		}
		models = append(models, model)
	}
	return models, nil
}

func piThinkingLevels(model piNativeModel) []string {
	if !model.Reasoning {
		return nil
	}
	levels := make([]string, 0, 7)
	for _, level := range []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"} {
		value, present := model.ThinkingLevelMap[level]
		if !present || value == nil {
			continue
		}
		if (level == "xhigh" || level == "max") && *value == "" {
			continue
		}
		levels = append(levels, level)
	}
	return levels
}
