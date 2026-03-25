package ui

import (
	"context"
	"fmt"
	"strings"

	"fyne.io/fyne/v2"
	fyneapp "fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
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
	app.Settings().SetTheme(&CivicLedgerTheme{})

	window := app.NewWindow(deps.Config.WindowTitle)
	window.Resize(fyne.NewSize(1360, 880))
	window.SetMaster()

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
	if len(registry) == 0 {
		return widget.NewLabel("No screens registered")
	}

	// Build initial screen content
	activeIdx := 0
	contentArea := container.NewStack()
	if registry[0].Build != nil {
		contentArea.Objects = []fyne.CanvasObject{registry[0].Build(runtime)}
	}

	// Nav items from registry
	items := make([]navItem, len(registry))
	for i, e := range registry {
		items[i] = navItem{
			Order:  e.Order,
			Label:  e.Name,
			Icon:   iconForScreen(e.Name),
			Screen: e.Build,
		}
	}

	// Sidebar: rebuild nav buttons on every selection to update highlight
	navContainer := container.NewVBox()
	var rebuildNav func()

	rebuildNav = func() {
		navContainer.Objects = nil
		for i := range items {
			idx := i
			item := items[i]
			active := idx == activeIdx

			icon := theme.DefaultTheme().Icon(item.Icon)
			btn := buildNavButton(item.Label, icon, active, func() {
				activeIdx = idx
				rebuildNav()
				if item.Screen != nil {
					contentArea.Objects = []fyne.CanvasObject{item.Screen(runtime)}
					contentArea.Refresh()
				}
				runtime.SetActivity(fmt.Sprintf("Navigated to %s", item.Label))
			})
			navContainer.Add(btn)
		}
		navContainer.Refresh()
	}
	rebuildNav()

	// Status footer inside sidebar
	statusLabel := widget.NewLabelWithData(runtime.StatusMessage)

	dbText := canvas.NewText(fmt.Sprintf("DB: %s", truncatePath(runtime.DBPath, 26)), ColorOnSurfaceVariant)
	dbText.TextSize = 10

	psgcText := canvas.NewText(psgcStatusSummary(runtime.PSGCReport), ColorOnSurfaceVariant)
	psgcText.TextSize = 10

	sidebarFooter := container.NewVBox(
		widget.NewSeparator(),
		dbText,
		psgcText,
		statusLabel,
	)

	// Brand header
	brandTitle := canvas.NewText(runtime.Config.WindowTitle, ColorPrimary)
	brandTitle.TextSize = 14
	brandTitle.TextStyle = fyne.TextStyle{Bold: true}

	brandSub := canvas.NewText("LGU Administrative Portal", ColorOnSurfaceVariant)
	brandSub.TextSize = 9

	brandIconBg := canvas.NewRectangle(ColorPrimary)
	brandIconBg.CornerRadius = 8
	brandIconBg.SetMinSize(fyne.NewSize(36, 36))
	brandIconLabel := canvas.NewText("⬡", ColorOnPrimary)
	brandIconLabel.TextSize = 20
	brandIconLabel.TextStyle = fyne.TextStyle{Bold: true}
	brandIconBox := container.NewStack(brandIconBg, container.NewCenter(brandIconLabel))

	brandTextBox := container.NewVBox(brandTitle, brandSub)
	brandRow := container.NewPadded(container.NewHBox(brandIconBox, brandTextBox))

	sidebarBg := canvas.NewRectangle(ColorSurfaceContainerLow)
	sidebarContent := container.NewBorder(
		container.NewVBox(brandRow, widget.NewSeparator()),
		sidebarFooter,
		nil, nil,
		container.NewVScroll(navContainer),
	)
	sidebar := container.NewStack(sidebarBg, sidebarContent)

	// Top bar
	topBar := buildTopBar(runtime)

	// Main area = top bar + content
	mainBg := canvas.NewRectangle(ColorSurface)
	mainContent := container.NewStack(
		mainBg,
		container.NewBorder(topBar, nil, nil, nil, container.NewPadded(contentArea)),
	)

	// Split sidebar (fixed 240px) + main content
	split := container.NewHSplit(sidebar, mainContent)
	split.SetOffset(0.18) // ~240px of a 1360 wide window

	return split
}

func buildFooter(_ *Runtime) fyne.CanvasObject {
	// Footer is now embedded in the sidebar — kept for compatibility only.
	return widget.NewLabel("")
}
