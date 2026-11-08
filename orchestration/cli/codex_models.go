package cli

import (
	"context"
	"os"
)

func (a *CodexAdapter) Models(ctx context.Context, cwd string) ([]Model, error) {
	binary := a.Binary
	if binary == "" {
		binary = "codex"
	}
	args := append([]string{"app-server"}, a.Args...)
	client, err := startRPCClient(rpcClientOptions{Binary: binary, Args: args, Dir: cwd,
		Env: append(os.Environ(), "CODEX_INTERNAL_ORIGINATOR_OVERRIDE=T3 Code")})
	if err != nil {
		return nil, err
	}
	defer client.close()
	var initialized struct {
		UserAgent string `json:"userAgent"`
	}
	if err := client.call(ctx, "initialize", map[string]any{
		"clientInfo":   map[string]any{"name": "T3 Code", "title": "T3 Code", "version": "0.1.7"},
		"capabilities": map[string]any{"experimentalApi": true},
	}, &initialized); err != nil {
		return nil, err
	}
	if err := client.notify("initialized", nil); err != nil {
		return nil, err
	}
	var models []Model
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var response struct {
			Data []struct {
				ID                        string `json:"id"`
				Model                     string `json:"model"`
				DisplayName               string `json:"displayName"`
				IsDefault                 bool   `json:"isDefault"`
				DefaultReasoningEffort    string `json:"defaultReasoningEffort"`
				DefaultServiceTier        string `json:"defaultServiceTier"`
				SupportedReasoningEfforts []struct {
					ReasoningEffort string `json:"reasoningEffort"`
				} `json:"supportedReasoningEfforts"`
				ServiceTiers []struct {
					ID          string `json:"id"`
					Name        string `json:"name"`
					Description string `json:"description"`
				} `json:"serviceTiers"`
				AdditionalSpeedTiers []string `json:"additionalSpeedTiers"`
			} `json:"data"`
			NextCursor string `json:"nextCursor"`
		}
		if err := client.call(ctx, "model/list", params, &response); err != nil {
			return nil, err
		}
		for _, native := range response.Data {
			label := native.DisplayName
			if label == "" {
				label = native.Model
			}
			model := Model{ID: native.Model, Label: label}
			var efforts []string
			defaultEffort := ""
			for _, effort := range native.SupportedReasoningEfforts {
				efforts = append(efforts, effort.ReasoningEffort)
			}
			if len(efforts) > 0 {
				defaultEffort = native.DefaultReasoningEffort
				if defaultEffort == "" {
					defaultEffort = efforts[0]
				}
				model.Options = append(model.Options, ModelOption{ID: "reasoningEffort", Label: "Reasoning",
					Type: "select", Values: efforts, Default: defaultEffort})
			}
			var tiers []string
			for _, tier := range native.ServiceTiers {
				tiers = append(tiers, tier.ID)
			}
			for _, tier := range native.AdditionalSpeedTiers {
				tiers = append(tiers, tier)
			}
			if len(tiers) > 0 {
				model.Options = append(model.Options, ModelOption{ID: "serviceTier", Label: "Service Tier",
					Type: "select", Values: append([]string{"standard"}, tiers...), Default: "standard"})
			}
			models = append(models, model)
		}
		cursor = response.NextCursor
		if cursor == "" {
			break
		}
	}
	return models, nil
}
