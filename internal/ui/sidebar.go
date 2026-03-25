package ui

import (
	"fmt"
	"image/color"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// navItem describes a single sidebar navigation entry.
type navItem struct {
	Order  int
	Label  string
	Icon   fyne.ThemeIconName
	Screen func(*Runtime) fyne.CanvasObject
}

// sidebarNavItems returns the ordered navigation entries matching the Stitch design.
// Icons map to the closest available Fyne built-in theme icons.
func sidebarNavItems(runtime *Runtime) []navItem {
	entries := snapshotScreenRegistry()
	items := make([]navItem, 0, len(entries))
	for _, e := range entries {
		icon := iconForScreen(e.Name)
		items = append(items, navItem{
			Order:  e.Order,
			Label:  e.Name,
			Icon:   icon,
			Screen: e.Build,
		})
	}
	return items
}

// iconForScreen maps screen names to Fyne theme icon names.
func iconForScreen(name string) fyne.ThemeIconName {
	switch strings.ToLower(name) {
	case "dashboard":
		return theme.IconNameHome
	case "encoding", "beneficiaries":
		return theme.IconNameList
	case "deduplication", "dedup review":
		return theme.IconNameSearch
	case "import":
		return theme.IconNameUpload
	case "export":
		return theme.IconNameDownload
	case "settings":
		return theme.IconNameSettings
	case "audit logs", "audit trail":
		return theme.IconNameHistory
	default:
		return theme.IconNameDocument
	}
}

// buildSidebar constructs the left sidebar navigation panel.
// onSelect is called with the index of the selected nav item when clicked.
func buildSidebar(runtime *Runtime, items []navItem, activeIdx *int, onSelect func(int)) fyne.CanvasObject {
	// Brand header
	brandIcon := canvas.NewText("⬡", ColorOnPrimary)
	brandIcon.TextSize = 18

	brandIconBg := canvas.NewRectangle(ColorPrimary)
	brandIconBg.CornerRadius = 8
	brandIconBg.SetMinSize(fyne.NewSize(36, 36))

	brandIconBox := container.NewStack(brandIconBg, container.NewCenter(brandIcon))

	appTitle := canvas.NewText("Beneficiary Tool", ColorPrimary)
	appTitle.TextSize = 12
	appTitle.TextStyle = fyne.TextStyle{Bold: true}

	appSubtitle := canvas.NewText("LGU Portal", ColorOnSurfaceVariant)
	appSubtitle.TextSize = 9

	brandText := container.NewVBox(appTitle, appSubtitle)
	brandRow := container.NewHBox(brandIconBox, brandText)

	// Nav items
	navButtons := make([]fyne.CanvasObject, len(items))
	for i := range items {
		idx := i
		item := items[i]

		icon := widget.NewIcon(theme.DefaultTheme().Icon(item.Icon))
		label := widget.NewLabel(item.Label)
		label.TextStyle = fyne.TextStyle{Bold: false}

		var btn *widget.Button
		btn = widget.NewButton("", func() {
			*activeIdx = idx
			onSelect(idx)
		})
		btn.Importance = widget.LowImportance

		row := container.NewHBox(icon, label)
		navButtons[i] = container.NewStack(btn, container.NewPadded(row))
	}

	navList := container.NewVBox(navButtons...)

	// Status footer
	statusLabel := widget.NewLabelWithData(runtime.StatusMessage)
	statusLabel.TextStyle = fyne.TextStyle{}

	dbLabel := canvas.NewText(fmt.Sprintf("DB: %s", truncatePath(runtime.DBPath, 18)), ColorOnSurfaceVariant)
	dbLabel.TextSize = 10

	footer := container.NewVBox(
		widget.NewSeparator(),
		dbLabel,
		statusLabel,
	)

	// Assemble sidebar
	sidebarContent := container.NewBorder(
		container.NewVBox(brandRow, widget.NewSeparator()),
		footer,
		nil, nil,
		container.NewVScroll(navList),
	)

	// Sidebar background
	bg := canvas.NewRectangle(ColorSurfaceContainerLow)
	sidebar := container.NewStack(bg, container.NewPadded(sidebarContent))
	sidebar.Resize(fyne.NewSize(240, 0))
	return sidebar
}

// buildTopBar constructs the slim top header bar.
func buildTopBar(runtime *Runtime) fyne.CanvasObject {
	title := canvas.NewText(runtime.Config.WindowTitle, ColorOnSurface)
	title.TextSize = 16
	title.TextStyle = fyne.TextStyle{Bold: true}

	versionBadge := canvas.NewText("v1.0 Offline", ColorOnTertiaryContainer)
	versionBadge.TextSize = 9
	versionBadge.TextStyle = fyne.TextStyle{Bold: true}

	versionBg := canvas.NewRectangle(ColorTertiaryContainer)
	versionBg.CornerRadius = 4
	versionBadgeBox := container.NewStack(versionBg, container.NewPadded(versionBadge))

	activityLabel := widget.NewLabelWithData(runtime.Activity)

	topBar := container.NewBorder(
		nil, nil,
		container.NewHBox(title, versionBadgeBox),
		activityLabel,
	)

	topBg := canvas.NewRectangle(withAlpha(ColorSurfaceContainerLowest, 0xee))
	return container.NewStack(topBg, container.NewPadded(topBar))
}

// truncatePath shortens a file path for display in the sidebar.
func truncatePath(path string, maxLen int) string {
	if len(path) <= maxLen {
		return path
	}
	return "…" + path[len(path)-maxLen:]
}

// highlightNavItem visually updates the active nav item.
// This is called after selection to redraw nav buttons.
func buildNavButton(label string, icon fyne.Resource, active bool, onTap func()) fyne.CanvasObject {
	var textColor color.Color = ColorOnSurface
	var bgColor color.Color = ColorTransparent

	if active {
		textColor = ColorPrimary
		bgColor = ColorPrimaryContainer
	}

	_ = textColor

	iconWidget := widget.NewIcon(icon)

	lbl := canvas.NewText(label, ColorOnSurface)
	lbl.TextSize = 13
	if active {
		lbl.Color = ColorPrimary
		lbl.TextStyle = fyne.TextStyle{Bold: true}
	}

	// Left accent bar for active item (4px wide, primary color)
	var leftAccent fyne.CanvasObject
	if active {
		accent := canvas.NewRectangle(ColorPrimary)
		accent.SetMinSize(fyne.NewSize(4, 0))
		leftAccent = accent
	} else {
		spacer := canvas.NewRectangle(ColorTransparent)
		spacer.SetMinSize(fyne.NewSize(4, 0))
		leftAccent = spacer
	}

	row := container.NewHBox(leftAccent, iconWidget, lbl, layout.NewSpacer())
	bg := canvas.NewRectangle(bgColor)
	bg.CornerRadius = 4

	btn := widget.NewButton("", onTap)
	btn.Importance = widget.LowImportance

	return container.NewStack(bg, btn, container.NewPadded(row))
}
