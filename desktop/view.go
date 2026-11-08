package desktop

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/egoist/mygo/ui"
	"github.com/keejkrej/g0code/orchestration"
)

func (a *App) View(c *ui.Context) {
	a.mu.RLock()
	threads := append([]ThreadSummary(nil), a.Threads...)
	selected := a.Selected
	children := append([]orchestration.ThreadSnapshot(nil), a.Children...)
	selectedID := a.SelectedID
	errorMessage := a.Error
	a.mu.RUnlock()
	theme := c.Theme()

	ui.Row(c).Fill().Children(func() {
		ui.Column(c).Width(260).FillHeight().Padding(12).Gap(8).Children(func() {
			ui.Text(c, "g0code").FontSize(22).Bold()
			ui.Text(c, "Local agent orchestration").FontSize(12).TextColor(theme.TextMuted)
			if ui.Button(c.Key("new-thread"), "New thread").Clicked() {
				_, _ = a.NewThread(context.Background())
			}
			ui.Divider(c)
			ui.Scroll(c).Grow(1).Children(func() {
				for _, thread := range threads {
					label := thread.Title + "\n" + thread.Status + " · " + string(thread.Provider)
					if ui.Button(c.Key(thread.ID), label).FillWidth().Clicked() {
						_ = a.SelectThread(context.Background(), thread.ID)
					}
				}
			})
		})
		ui.Divider(c)
		ui.Column(c).Grow(1).FillHeight().Padding(16).Gap(10).Children(func() {
			if selectedID == "" {
				ui.Column(c).Grow(1).Center().Gap(12).Children(func() {
					ui.Text(c, "Create a thread to start an agent.").FontSize(18)
					if ui.PrimaryButton(c.Key("empty-new-thread"), "New thread").Clicked() {
						_, _ = a.NewThread(context.Background())
					}
				})
				return
			}
			thread := selected.Projection.Thread
			ui.Row(c).Gap(8).Children(func() {
				ui.Text(c, thread.Title).FontSize(20).Bold().Grow(1)
				ui.Text(c, threadStatus(selected.Projection)).TextColor(theme.TextMuted)
			})
			provider := string(a.Provider)
			ui.Row(c).Gap(8).Children(func() {
				if ui.Combobox(c.Key("provider"), &provider, []string{"codex", "claude", "pi", "agy", "fx"}).Label("Provider").Changed() {
					_ = a.SetProvider(context.Background(), orchestration.ProviderInstanceID(provider))
				}
				mode := string(a.Mode)
				if ui.Combobox(c.Key("runtime-mode"), &mode, []string{"approval-required", "auto-accept-edits", "auto", "full-access"}).Label("Runtime").Changed() {
					_ = a.SetMode(context.Background(), orchestration.RuntimeMode(mode))
				}
			})
			if errorMessage != "" {
				ui.Text(c, errorMessage).TextColor(theme.Danger)
			}
			ui.Scroll(c).Grow(1).Children(func() {
				ui.Column(c).FillWidth().Gap(10).Children(func() {
					for _, item := range selected.Projection.TurnItems {
						viewTurnItem(c, item)
					}
				})
			})
			pending := pendingRequests(selected.Projection)
			for _, child := range children {
				pending = append(pending, pendingRequests(child.Projection)...)
			}
			if len(pending) > 0 {
				ui.Column(c).FillWidth().Padding(10).Gap(8).Children(func() {
					ui.Text(c, "Pending requests").Bold()
					for _, request := range pending {
						ui.Row(c.Key(request.ID)).Gap(8).Children(func() {
							ui.Text(c, string(request.Kind)+" · "+string(request.ID)).Grow(1)
							if ui.PrimaryButton(c.Key("allow"), "Allow").Clicked() {
								_ = a.RespondOnThread(context.Background(), request.ThreadID, request.ID, "accept")
							}
							if ui.Button(c.Key("deny"), "Deny").Clicked() {
								_ = a.RespondOnThread(context.Background(), request.ThreadID, request.ID, "decline")
							}
						})
					}
				})
			}
			ui.TextArea(c.Key("composer"), &a.Draft).Lines(3, 8).Placeholder("Ask the agent, or describe a child task")
			ui.Row(c).Gap(8).Children(func() {
				if ui.PrimaryButton(c.Key("send"), "Send").Disabled(strings.TrimSpace(a.Draft) == "").Clicked() {
					_ = a.Send(context.Background())
				}
				_, active := selected.Projection.ActiveRun()
				if ui.Button(c.Key("delegate"), "Delegate").Disabled(!active || strings.TrimSpace(a.Draft) == "").Clicked() {
					_ = a.Delegate(context.Background())
				}
				if ui.Button(c.Key("stop"), "Stop").Disabled(!active).Clicked() {
					_ = a.Interrupt(context.Background())
				}
			})
		})
	})
}

func threadStatus(projection orchestration.Projection) string {
	if len(projection.Runs) == 0 {
		return "idle"
	}
	return string(projection.Runs[len(projection.Runs)-1].Status)
}

type pendingRequest struct {
	ThreadID orchestration.ThreadID
	orchestration.RuntimeRequest
}

func pendingRequests(projection orchestration.Projection) []pendingRequest {
	result := make([]pendingRequest, 0)
	for _, request := range projection.RuntimeRequests {
		if request.Status == orchestration.RequestPending {
			result = append(result, pendingRequest{ThreadID: projection.Thread.ID, RuntimeRequest: request})
		}
	}
	return result
}

func viewTurnItem(c *ui.Context, item orchestration.TurnItem) {
	theme := c.Theme()
	title := item.Title
	if title == "" {
		title = item.Type
	}
	text := item.Text
	if text == "" && item.Data != nil {
		if encoded, err := json.Marshal(item.Data); err == nil {
			text = string(encoded)
		}
	}
	if len(text) > 4000 {
		text = text[:4000] + "…"
	}
	ui.Column(c.Key(item.ID)).FillWidth().Padding(10).Gap(4).Children(func() {
		ui.Row(c).Gap(8).Children(func() {
			ui.Text(c, title).Bold().Grow(1)
			ui.Text(c, string(item.Status)).FontSize(11).TextColor(theme.TextMuted)
		})
		if text != "" {
			ui.Text(c, text)
		}
		if item.Type == "subagent" {
			ui.Text(c, fmt.Sprintf("child thread: %v", item.Data["childThreadId"])).FontSize(11).TextColor(theme.TextMuted)
		}
	})
}
