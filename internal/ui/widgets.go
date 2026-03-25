package ui

import (
	"image/color"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

// ─────────────────────────────────────────────
// MetricCard
// ─────────────────────────────────────────────

// MetricCard renders a dashboard KPI card with a title, value, and a colored
// 3-px bottom accent bar matching the Civic Ledger design.
func MetricCard(title string, value fyne.CanvasObject, accentColor color.Color) fyne.CanvasObject {
	titleLabel := canvas.NewText(title, ColorOnSurfaceVariant)
	titleLabel.TextSize = 10
	titleLabel.TextStyle = fyne.TextStyle{Bold: true}

	accent := canvas.NewRectangle(accentColor)
	accent.SetMinSize(fyne.NewSize(0, 3))

	inner := container.NewVBox(
		titleLabel,
		value,
		layout.NewSpacer(),
		accent,
	)

	bg := canvas.NewRectangle(ColorSurfaceContainerLowest)
	bg.CornerRadius = 8

	card := container.NewStack(bg, container.NewPadded(inner))
	card.Resize(fyne.NewSize(0, 100))
	return card
}

// MetricValueLabel returns a label styled as the large metric number.
func MetricValueLabel(text string) *canvas.Text {
	t := canvas.NewText(text, ColorOnSurface)
	t.TextSize = 28
	t.TextStyle = fyne.TextStyle{Bold: true}
	return t
}

// ─────────────────────────────────────────────
// StatusBadge
// ─────────────────────────────────────────────

type StatusKind int

const (
	StatusVerified StatusKind = iota
	StatusActive
	StatusFlagged
	StatusDuplicate
	StatusPending
	StatusDeleted
	StatusRetained
	StatusInfo
)

// StatusBadge renders a pill-shaped colored label chip.
func StatusBadge(label string, kind StatusKind) fyne.CanvasObject {
	var fg, bg color.Color
	switch kind {
	case StatusVerified, StatusActive, StatusRetained:
		fg = ColorTertiary
		bg = withAlpha(ColorTertiary, 0x1a) // 10% tint
	case StatusFlagged, StatusDuplicate:
		fg = ColorError
		bg = withAlpha(ColorError, 0x1a)
	case StatusPending:
		fg = ColorAmber
		bg = withAlpha(ColorAmber, 0x1a)
	case StatusDeleted:
		fg = ColorOnSurfaceVariant
		bg = withAlpha(ColorOnSurfaceVariant, 0x18)
	default:
		fg = ColorPrimary
		bg = withAlpha(ColorPrimary, 0x1a)
	}

	text := canvas.NewText(label, fg)
	text.TextSize = 10
	text.TextStyle = fyne.TextStyle{Bold: true}

	bgRect := canvas.NewRectangle(bg)
	bgRect.CornerRadius = 10

	return container.NewStack(bgRect, container.NewPadded(text))
}

// StatusBadgeForString returns a StatusBadge for common status strings.
func StatusBadgeForString(status string) fyne.CanvasObject {
	switch status {
	case "ACTIVE":
		return StatusBadge("ACTIVE", StatusActive)
	case "RETAINED":
		return StatusBadge("RETAINED", StatusRetained)
	case "DELETED":
		return StatusBadge("DELETED", StatusDeleted)
	case "DUPLICATE":
		return StatusBadge("DUPLICATE", StatusDuplicate)
	case "PENDING":
		return StatusBadge("PENDING", StatusPending)
	default:
		return StatusBadge(status, StatusInfo)
	}
}

// ─────────────────────────────────────────────
// SectionHeader
// ─────────────────────────────────────────────

// SectionHeader renders a page title and optional subtitle matching the
// "font-headline font-extrabold text-3xl" style from the design.
func SectionHeader(title, subtitle string) fyne.CanvasObject {
	titleText := canvas.NewText(title, ColorOnSurface)
	titleText.TextSize = 22
	titleText.TextStyle = fyne.TextStyle{Bold: true}

	if subtitle == "" {
		return container.NewVBox(titleText)
	}

	subText := canvas.NewText(subtitle, ColorOnSurfaceVariant)
	subText.TextSize = 12

	return container.NewVBox(titleText, subText)
}

// ─────────────────────────────────────────────
// DataTable
// ─────────────────────────────────────────────

// DataTable renders a styled table with alternating row backgrounds.
// Headers use uppercase small caps style on a surface-container background.
func DataTable(headers []string, rows [][]string) fyne.CanvasObject {
	if len(headers) == 0 {
		return widget.NewLabel("No data")
	}

	// Header row
	headerCells := make([]fyne.CanvasObject, len(headers))
	for i, h := range headers {
		t := canvas.NewText(h, ColorOnSurfaceVariant)
		t.TextSize = 10
		t.TextStyle = fyne.TextStyle{Bold: true}
		headerCells[i] = container.NewPadded(t)
	}
	headerBg := canvas.NewRectangle(ColorSurfaceContainer)
	headerRow := container.NewStack(headerBg, container.NewGridWithColumns(len(headers), headerCells...))

	// Data rows
	rowContainers := make([]fyne.CanvasObject, 0, len(rows)+1)
	rowContainers = append(rowContainers, headerRow)

	for i, row := range rows {
		cells := make([]fyne.CanvasObject, len(headers))
		for j, cell := range row {
			if j >= len(headers) {
				break
			}
			t := widget.NewLabel(cell)
			t.Wrapping = fyne.TextTruncate
			cells[j] = t
		}
		// fill missing cells
		for j := len(row); j < len(headers); j++ {
			cells[j] = widget.NewLabel("")
		}

		var rowBg color.Color
		if i%2 == 0 {
			rowBg = ColorSurfaceContainerLowest
		} else {
			rowBg = ColorSurfaceContainerLow
		}
		bg := canvas.NewRectangle(rowBg)
		gridRow := container.NewStack(bg, container.NewGridWithColumns(len(headers), cells...))
		rowContainers = append(rowContainers, gridRow)
	}

	return container.NewVBox(rowContainers...)
}

// ─────────────────────────────────────────────
// Card
// ─────────────────────────────────────────────

// Card wraps content in a white rounded card container.
func Card(content fyne.CanvasObject) fyne.CanvasObject {
	bg := canvas.NewRectangle(ColorSurfaceContainerLowest)
	bg.CornerRadius = 8
	return container.NewStack(bg, container.NewPadded(content))
}

// ─────────────────────────────────────────────
// PrimaryButton / SecondaryButton
// ─────────────────────────────────────────────

// PrimaryButton returns a styled primary action button.
func PrimaryButton(label string, tapped func()) *widget.Button {
	btn := widget.NewButton(label, tapped)
	btn.Importance = widget.HighImportance
	return btn
}

// DangerButton returns a button with error styling for destructive actions.
func DangerButton(label string, tapped func()) *widget.Button {
	btn := widget.NewButton(label, tapped)
	btn.Importance = widget.DangerImportance
	return btn
}

// ─────────────────────────────────────────────
// InitialsAvatar
// ─────────────────────────────────────────────

// InitialsAvatar renders a circular avatar with two-letter initials.
func InitialsAvatar(initials string, bg color.Color) fyne.CanvasObject {
	circle := canvas.NewCircle(bg)
	circle.Resize(fyne.NewSize(32, 32))

	text := canvas.NewText(initials, ColorOnSurface)
	text.TextSize = 11
	text.TextStyle = fyne.TextStyle{Bold: true}
	text.Alignment = fyne.TextAlignCenter

	return container.NewStack(circle, container.NewCenter(text))
}
