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
	selectedItemID := a.SelectedItemID
	rightOpen := a.RightOpen
	a.mu.RUnlock()

	ui.Row(c).Fill().Background(c.Theme().Background).Children(func() {
		a.viewSidebar(c, threads, selectedID)
		ui.Divider(c)
		center := func() { a.viewConversation(c, selected, children, selectedID, errorMessage, selectedItemID) }
		if rightOpen {
			ui.SplitPercent(c, &a.RightSplit, center, func() { a.viewDetails(c, selected, selectedItemID) }).Grow(1)
		} else {
			center()
		}
	})
}

func (a *App) viewSidebar(c *ui.Context, threads []ThreadSummary, selectedID orchestration.ThreadID) {
	theme := c.Theme()
	ui.Column(c).Width(310).FillHeight().Padding(12).Gap(10).Children(func() {
		ui.Row(c).Gap(8).Children(func() {
			ui.Text(c, "Local").FontSize(20).Bold().Grow(1)
			ui.Text(c, "g0code").FontSize(12).TextColor(theme.TextMuted)
		})
		ui.SearchField(c, &a.Search).Placeholder("Search threads")
		if ui.Button(c.Key("new-thread"), "＋  New thread").FillWidth().Clicked() {
			_, _ = a.NewThread(context.Background())
		}
		ui.Divider(c)
		query := strings.ToLower(strings.TrimSpace(a.Search))
		ui.Scroll(c).Grow(1).Children(func() {
			ui.Column(c).FillWidth().Gap(6).Children(func() {
				for _, thread := range threads {
					if query != "" && !strings.Contains(strings.ToLower(thread.Title), query) {
						continue
					}
					surface := theme.Background
					if thread.ID == selectedID {
						surface = theme.Surface
					}
					button := ui.ButtonBase(c.Key(thread.ID)).FillWidth().Background(surface).Radius(10).Padding(10).Children(func() {
						ui.Column(c).FillWidth().Gap(5).Children(func() {
							ui.Text(c, thread.Title).Bold()
							ui.Row(c).Gap(6).Children(func() {
								ui.Text(c, "local workspace").FontSize(11).TextColor(theme.TextMuted).Grow(1)
								ui.Text(c, thread.Status).FontSize(11).TextColor(statusColor(theme, thread.Status))
							})
							ui.Text(c, string(thread.Provider)).FontSize(11).TextColor(theme.TextMuted)
						})
					})
					if button.Clicked() {
						_ = a.SelectThread(context.Background(), thread.ID)
					}
				}
			})
		})
		ui.Text(c, fmt.Sprintf("%d threads", len(threads))).FontSize(11).TextColor(theme.TextMuted)
	})
}

func (a *App) viewConversation(c *ui.Context, selected selectedSnapshot, children []orchestration.ThreadSnapshot,
	selectedID orchestration.ThreadID, errorMessage string, selectedItemID orchestration.TurnItemID) {
	theme := c.Theme()
	ui.Column(c).Grow(1).FillHeight().Children(func() {
		if selectedID == "" {
			ui.Column(c).Grow(1).Center().Gap(14).Children(func() {
				ui.Text(c, "Work with your agents").FontSize(24).Bold()
				ui.Text(c, "Create a local thread, choose a provider, and send a task.").TextColor(theme.TextMuted)
				if ui.PrimaryButton(c.Key("empty-new-thread"), "New thread").Clicked() {
					_, _ = a.NewThread(context.Background())
				}
			})
			return
		}
		thread := selected.Projection.Thread
		ui.Row(c).Padding(14, 18).Gap(8).Children(func() {
			ui.Text(c, "local").TextColor(theme.TextMuted)
			ui.Text(c, "/").TextColor(theme.TextMuted)
			ui.Text(c, thread.Title).Bold().Grow(1)
			ui.Text(c, threadStatus(selected.Projection)).TextColor(statusColor(theme, threadStatus(selected.Projection)))
		})
		ui.Divider(c)
		ui.Scroll(c).Grow(1).Children(func() {
			ui.Column(c).FillWidth().Padding(18, 22).Gap(12).Children(func() {
				if summary, ok := changeSummary(selected.Projection); ok {
					ui.Row(c).FillWidth().Background(theme.Surface).Radius(12).Padding(12).Gap(10).Children(func() {
						ui.Text(c, fmt.Sprintf("%d changed %s", summary.files, plural(summary.files, "file", "files"))).Bold().Grow(1)
						ui.Text(c, fmt.Sprintf("+%d", summary.additions)).TextColor(theme.Success)
						ui.Text(c, fmt.Sprintf("−%d", summary.deletions)).TextColor(theme.Danger)
						if ui.Button(c.Key("open-latest-diff"), "Open diff").Clicked() {
							a.SelectItem(summary.itemID)
						}
					})
				}
				for _, item := range selected.Projection.TurnItems {
					a.viewTurnItem(c, item, item.ID == selectedItemID)
				}
			})
		})
		if errorMessage != "" {
			ui.Text(c, errorMessage).Padding(0, 18, 8).TextColor(theme.Danger)
		}
		pending := pendingRequests(selected.Projection)
		for _, child := range children {
			pending = append(pending, pendingRequests(child.Projection)...)
		}
		if len(pending) > 0 {
			ui.Column(c).FillWidth().Margin(0, 18, 8).Background(theme.Surface).Border(1, theme.Border).Radius(12).Padding(12).Gap(8).Children(func() {
				ui.Text(c, "Approval needed").Bold()
				for _, request := range pending {
					ui.Row(c.Key(request.ID)).Gap(8).Children(func() {
						ui.Text(c, string(request.Kind)+" · "+string(request.ID)).FontSize(12).Grow(1)
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
		a.viewComposer(c, selected)
	})
}

func (a *App) viewComposer(c *ui.Context, selected selectedSnapshot) {
	theme := c.Theme()
	ui.Column(c).FillWidth().MaxWidth(900).Margin(0, ui.Auto, 16).Background(theme.Surface).Border(1, theme.Border).Radius(16).Padding(14).Gap(10).Children(func() {
		ui.TextAreaBase(c.Key("composer"), &a.Draft).Lines(3, 8).Placeholder("Ask anything. @ files, $ skills, / commands")
		ui.Row(c).Gap(8).Wrap().Children(func() {
			provider := string(a.Provider)
			if ui.Combobox(c.Key("provider"), &provider, []string{"codex", "claude", "pi", "agy", "fx"}).Label("Provider").Changed() {
				_ = a.SetProvider(context.Background(), orchestration.ProviderInstanceID(provider))
			}
			mode := string(a.Mode)
			if ui.Combobox(c.Key("runtime-mode"), &mode, []string{"approval-required", "auto-accept-edits", "auto", "full-access"}).Label("Runtime").Changed() {
				_ = a.SetMode(context.Background(), orchestration.RuntimeMode(mode))
			}
			ui.Spacer(c).Grow(1)
			_, active := selected.Projection.ActiveRun()
			if ui.Button(c.Key("delegate"), "Delegate").Disabled(!active || strings.TrimSpace(a.Draft) == "").Clicked() {
				_ = a.Delegate(context.Background())
			}
			if ui.Button(c.Key("stop"), "Stop").Disabled(!active).Clicked() {
				_ = a.Interrupt(context.Background())
			}
			if ui.PrimaryButton(c.Key("send"), "↑  Send").Disabled(strings.TrimSpace(a.Draft) == "").Clicked() {
				_ = a.Send(context.Background())
			}
		})
	})
}

func (a *App) viewTurnItem(c *ui.Context, item orchestration.TurnItem, selected bool) {
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
	border := theme.Border
	if selected {
		border = theme.Accent
	}
	ui.Column(c.Key(item.ID)).FillWidth().Background(theme.Surface).Border(1, border).Radius(12).Padding(12).Gap(6).Children(func() {
		ui.Row(c).Gap(8).Children(func() {
			ui.Text(c, title).Bold().Grow(1)
			ui.Text(c, string(item.Status)).FontSize(11).TextColor(statusColor(theme, string(item.Status)))
		})
		if text != "" {
			ui.Text(c, text)
		}
		if item.Type == "subagent" {
			ui.Text(c, fmt.Sprintf("child thread: %v", item.Data["childThreadId"])).FontSize(11).TextColor(theme.TextMuted)
		}
		if item.Type == "command_execution" || item.Type == "file_change" || item.Type == "dynamic_tool" {
			if ui.Button(c.Key("open-item"), "Open details").Clicked() {
				a.SelectItem(item.ID)
			}
		}
	})
}

func (a *App) viewDetails(c *ui.Context, selected selectedSnapshot, selectedItemID orchestration.TurnItemID) {
	theme := c.Theme()
	item, ok := selectedTurnItem(selected.Projection, selectedItemID)
	if !ok {
		item, ok = latestToolItem(selected.Projection)
	}
	ui.Column(c).FillHeight().Children(func() {
		ui.Row(c).Padding(12, 16).Gap(8).Children(func() {
			title := "Run details"
			if ok && item.Data["fileName"] != nil {
				title = fmt.Sprint(item.Data["fileName"])
			} else if ok && item.Title != "" {
				title = item.Title
			}
			ui.Text(c, title).Bold().Grow(1)
			if ui.Button(c.Key("close-details"), "×").Clicked() {
				a.CloseDetails()
			}
		})
		ui.Divider(c)
		if !ok {
			ui.Column(c).Grow(1).Center().Children(func() {
				ui.Text(c, "Select a tool call or file change").TextColor(theme.TextMuted)
			})
			return
		}
		ui.Scroll(c).Grow(1).Children(func() {
			ui.Column(c).FillWidth().Padding(14, 16).Gap(12).Children(func() {
				ui.Text(c, item.Type+" · "+string(item.Status)).TextColor(statusColor(theme, string(item.Status)))
				for _, entry := range []struct {
					label string
					value any
				}{{"Diff", item.Data["diffStr"]}, {"Input", item.Data["input"]}, {"Output", item.Data["output"]}} {
					if entry.value == nil || fmt.Sprint(entry.value) == "" {
						continue
					}
					ui.Text(c, entry.label).Bold()
					ui.Text(c, formatDetail(entry.value)).Font("Menlo")
				}
			})
		})
	})
}

func selectedTurnItem(projection orchestration.Projection, id orchestration.TurnItemID) (orchestration.TurnItem, bool) {
	for _, item := range projection.TurnItems {
		if item.ID == id {
			return item, true
		}
	}
	return orchestration.TurnItem{}, false
}

func latestToolItem(projection orchestration.Projection) (orchestration.TurnItem, bool) {
	for index := len(projection.TurnItems) - 1; index >= 0; index-- {
		item := projection.TurnItems[index]
		if item.Type == "command_execution" || item.Type == "file_change" || item.Type == "dynamic_tool" {
			return item, true
		}
	}
	return orchestration.TurnItem{}, false
}

func formatDetail(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		encoded, _ := json.MarshalIndent(value, "", "  ")
		return string(encoded)
	}
}

type changeTotals struct {
	files     int
	additions int
	deletions int
	itemID    orchestration.TurnItemID
}

func changeSummary(projection orchestration.Projection) (changeTotals, bool) {
	files := make(map[string]bool)
	var totals changeTotals
	for _, item := range projection.TurnItems {
		if item.Type != "file_change" {
			continue
		}
		fileName := fmt.Sprint(item.Data["fileName"])
		if fileName != "" && fileName != "<nil>" {
			files[fileName] = true
		}
		totals.itemID = item.ID
		if diff, ok := item.Data["diffStr"].(string); ok {
			for _, line := range strings.Split(diff, "\n") {
				if strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++") {
					totals.additions++
				} else if strings.HasPrefix(line, "-") && !strings.HasPrefix(line, "---") {
					totals.deletions++
				}
			}
		}
	}
	totals.files = len(files)
	return totals, totals.itemID != ""
}

func plural(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return plural
}

func threadStatus(projection orchestration.Projection) string {
	if len(projection.Runs) == 0 {
		return "idle"
	}
	return string(projection.Runs[len(projection.Runs)-1].Status)
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

type pendingRequest struct {
	ThreadID orchestration.ThreadID
	orchestration.RuntimeRequest
}

func statusColor(theme *ui.Theme, status string) ui.Color {
	switch status {
	case "failed", "cancelled", "declined":
		return theme.Danger
	case "completed":
		return theme.Success
	case "running", "starting", "waiting", "pending", "queued", "interrupted":
		return theme.Warning
	default:
		return theme.TextMuted
	}
}
