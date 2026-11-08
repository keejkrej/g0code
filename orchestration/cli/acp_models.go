package cli

import (
	"context"
	"fmt"
)

type acpConfigOption struct {
	ID           string `json:"id"`
	Category     string `json:"category"`
	Type         string `json:"type"`
	CurrentValue any    `json:"currentValue"`
	Options      []struct {
		Value string `json:"value"`
		Name  string `json:"name"`
	} `json:"options"`
}

func (a *ACPAdapter) Models(ctx context.Context, cwd string) ([]Model, error) {
	binary := a.Binary
	if binary == "" {
		return nil, fmt.Errorf("ACP binary is required")
	}
	args := append([]string(nil), a.Args...)
	if len(args) == 0 {
		args = []string{"acp"}
	}
	client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: cwd})
	if err != nil {
		return nil, err
	}
	defer client.close()
	if err := client.call(ctx, "initialize", map[string]any{
		"protocolVersion":    2,
		"clientCapabilities": map[string]any{"fs": map[string]any{"readTextFile": false, "writeTextFile": false}, "terminal": false},
		"clientInfo":         map[string]any{"name": "g0code", "version": "0.1.0"},
	}, nil); err != nil {
		return nil, err
	}
	var response struct {
		SessionID string `json:"sessionId"`
		Models    struct {
			CurrentModelID  string `json:"currentModelId"`
			AvailableModels []struct {
				ModelID string `json:"modelId"`
				Name    string `json:"name"`
			} `json:"availableModels"`
		} `json:"models"`
		ConfigOptions []acpConfigOption `json:"configOptions"`
	}
	if err := client.call(ctx, "session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}}, &response); err != nil {
		return nil, err
	}
	if response.SessionID != "" {
		_ = client.call(context.Background(), "session/close", map[string]any{"sessionId": response.SessionID}, nil)
	}
	current := response.Models.CurrentModelID
	configByID := make(map[string]acpConfigOption)
	for _, option := range response.ConfigOptions {
		configByID[option.ID] = option
	}
	if current == "" {
		if model, ok := configByID["model"]; ok {
			current, _ = model.CurrentValue.(string)
		}
	}
	if current == "" && len(response.Models.AvailableModels) > 0 {
		current = response.Models.AvailableModels[0].ModelID
	}
	if current == "" {
		current = "default"
	}
	makeModel := func(id, label string) Model {
		model := Model{ID: id, Label: label}
		for _, option := range response.ConfigOptions {
			if option.Category == "mode" || option.ID == "model" {
				continue
			}
			if len(option.Options) == 0 {
				continue
			}
			values := make([]string, 0, len(option.Options))
			for _, choice := range option.Options {
				values = append(values, choice.Value)
			}
			model.Options = append(model.Options, ModelOption{ID: option.ID, Label: option.Category,
				Type: option.Type, Values: values, Default: option.CurrentValue})
		}
		return model
	}
	if len(response.Models.AvailableModels) == 0 {
		return []Model{makeModel(current, current)}, nil
	}
	models := make([]Model, 0, len(response.Models.AvailableModels))
	for _, native := range response.Models.AvailableModels {
		models = append(models, makeModel(native.ModelID, native.Name))
	}
	return models, nil
}
