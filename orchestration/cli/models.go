package cli

import (
	"context"

	"github.com/keejkrej/g0code/orchestration"
)

type ModelOption struct {
	ID      string
	Label   string
	Type    string // select or boolean
	Values  []string
	Default any
}

type Model struct {
	ID      string
	Label   string
	Options []ModelOption
}

type modelCatalogAdapter interface {
	Models(context.Context, string) ([]Model, error)
}

func (e *Executor) Models(ctx context.Context, instanceID orchestration.ProviderInstanceID, cwd string) ([]Model, error) {
	adapter := e.adapters[instanceID]
	if adapter == nil {
		return nil, orchestration.ErrUnsupportedCapability
	}
	if catalog, ok := adapter.(modelCatalogAdapter); ok {
		return catalog.Models(ctx, cwd)
	}
	return []Model{{ID: "default", Label: "Default"}}, nil
}

func defaultModel(models []Model) Model {
	for _, model := range models {
		if model.ID == "default" {
			return model
		}
	}
	if len(models) > 0 {
		return models[0]
	}
	return Model{ID: "default", Label: "Default"}
}
