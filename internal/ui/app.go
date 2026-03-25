package ui

import (
	"context"
	"fmt"
	"strings"

	"fyne.io/fyne/v2"
	fyneapp "fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

// Launch starts the Fyne desktop shell and blocks until the window closes.
func Launch(ctx context.Context, deps *Dependencies) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if deps == nil {
		return fmt.Errorf("dependencies are nil")
	}
	if strings.TrimSpace(deps.Config.AppID) == "" {
		return fmt.Errorf("app id is required")
	}

	app := fyneapp.NewWithID(deps.Config.AppID)
	window := app.NewWindow(deps.Config.WindowTitle)
	window.Resize(fyne.NewSize(1360, 880))

	runtime := &Runtime{
		Dependencies: deps,
		Window:       window,
	}
	runtime.SetStatus("Ready")
	runtime.SetActivity("Bootstrap completed")

	content := buildShell(runtime)
	window.SetContent(content)
	window.ShowAndRun()
	return nil
}

func buildShell(runtime *Runtime) fyne.CanvasObject {
	registry := snapshotScreenRegistry()
	tabs := make([]*container.TabItem, 0, len(registry))
	for _, entry := range registry {
		build := entry.Build
		if build == nil {
			continue
		}
		tabs = append(tabs, container.NewTabItem(entry.Name, build(runtime)))
	}
	if len(tabs) == 0 {
		tabs = append(tabs, container.NewTabItem("Dashboard", buildDashboard(runtime)))
	}

	headerTitle := widget.NewLabelWithStyle(
		runtime.Config.WindowTitle,
		fyne.TextAlignLeading,
		fyne.TextStyle{Bold: true},
	)
	dbPath := widget.NewLabel(fmt.Sprintf("DB: %s", runtime.DBPath))
	psgcSummary := widget.NewLabel(psgcStatusSummary(runtime.PSGCReport))
	statusLabel := widget.NewLabelWithData(runtime.StatusMessage)
	activityLabel := widget.NewLabelWithData(runtime.Activity)

	refreshTabs := container.NewAppTabs(tabs...)
	refreshTabs.SetTabLocation(container.TabLocationTop)
	refreshBtn := widget.NewButton("Refresh tabs", func() {
		updated := snapshotScreenRegistry()
		nextTabs := make([]*container.TabItem, 0, len(updated))
		for _, entry := range updated {
			if entry.Build == nil {
				continue
			}
			nextTabs = append(nextTabs, container.NewTabItem(entry.Name, entry.Build(runtime)))
		}
		if len(nextTabs) == 0 {
			nextTabs = append(nextTabs, container.NewTabItem("Dashboard", buildDashboard(runtime)))
		}
		refreshTabs.Items = nextTabs
		refreshTabs.Refresh()
		runtime.SetActivity("Tabs refreshed")
	})

	header := container.NewVBox(
		headerTitle,
		dbPath,
		psgcSummary,
		container.NewHBox(layout.NewSpacer(), refreshBtn),
		widget.NewSeparator(),
		statusLabel,
		activityLabel,
	)

	return container.NewBorder(header, buildFooter(runtime), nil, nil, refreshTabs)
}

func buildFooter(runtime *Runtime) fyne.CanvasObject {
	return container.NewHBox(
		widget.NewLabelWithStyle("Status:", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithData(runtime.StatusMessage),
		layout.NewSpacer(),
		widget.NewLabelWithStyle("Activity:", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithData(runtime.Activity),
	)
}
