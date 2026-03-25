package ui

import (
	"image/color"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
)

// CivicLedgerTheme implements fyne.Theme with the Civic Ledger design tokens.
type CivicLedgerTheme struct{}

var _ fyne.Theme = (*CivicLedgerTheme)(nil)

func (t *CivicLedgerTheme) Color(name fyne.ThemeColorName, variant fyne.ThemeVariant) color.Color {
	switch name {
	case theme.ColorNameBackground:
		return ColorSurface
	case theme.ColorNameButton:
		return ColorPrimary
	case theme.ColorNameDisabledButton:
		return ColorSurfaceContainerHigh
	case theme.ColorNameDisabled:
		return ColorOnSurfaceVariant
	case theme.ColorNameError:
		return ColorError
	case theme.ColorNameFocus:
		return withAlpha(ColorPrimary, 0x66) // 40% opacity ghost border
	case theme.ColorNameForeground:
		return ColorOnSurface
	case theme.ColorNameHover:
		return ColorSurfaceContainerHigh
	case theme.ColorNameInputBackground:
		return ColorSurfaceContainerHighest
	case theme.ColorNameInputBorder:
		return withAlpha(ColorOutlineVariant, 0x26) // 15% opacity ghost border
	case theme.ColorNameMenuBackground:
		return ColorSurfaceContainerLowest
	case theme.ColorNameOverlayBackground:
		return ColorSurfaceContainerLowest
	case theme.ColorNamePlaceHolder:
		return ColorOnSurfaceVariant
	case theme.ColorNamePressed:
		return withAlpha(ColorPrimary, 0x33)
	case theme.ColorNamePrimary:
		return ColorPrimary
	case theme.ColorNameScrollBar:
		return withAlpha(ColorOnSurfaceVariant, 0x40)
	case theme.ColorNameSeparator:
		return withAlpha(ColorOutlineVariant, 0x40)
	case theme.ColorNameShadow:
		return withAlpha(ColorOnSurface, 0x10) // very diffused — 6% opacity
	case theme.ColorNameHeaderBackground:
		return ColorSurfaceContainerLow
	case theme.ColorNameSelection:
		return ColorPrimaryContainer
	case theme.ColorNameSuccess:
		return ColorTertiary
	case theme.ColorNameWarning:
		return ColorAmber
	}
	return theme.DefaultTheme().Color(name, variant)
}

func (t *CivicLedgerTheme) Font(style fyne.TextStyle) fyne.Resource {
	// Fall back to the default theme fonts.
	// If Public Sans / Inter TTF files are bundled in assets/fonts/,
	// they can be wired here via fyne.NewStaticResource.
	return theme.DefaultTheme().Font(style)
}

func (t *CivicLedgerTheme) Icon(name fyne.ThemeIconName) fyne.Resource {
	return theme.DefaultTheme().Icon(name)
}

func (t *CivicLedgerTheme) Size(name fyne.ThemeSizeName) float32 {
	switch name {
	case theme.SizeNamePadding:
		return 8
	case theme.SizeNameInlineIcon:
		return 18
	case theme.SizeNameScrollBar:
		return 6
	case theme.SizeNameScrollBarSmall:
		return 3
	case theme.SizeNameSeparatorThickness:
		return 1
	case theme.SizeNameText:
		return 13
	case theme.SizeNameHeadingText:
		return 22
	case theme.SizeNameSubHeadingText:
		return 16
	case theme.SizeNameCaptionText:
		return 11
	case theme.SizeNameInputBorder:
		return 2
	}
	return theme.DefaultTheme().Size(name)
}
