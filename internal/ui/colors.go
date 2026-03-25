package ui

import "image/color"

// Civic Ledger design token colors extracted from the Stitch-generated design system.
// Source: D:\GitProjects\dedup\design\civic_ledger\DESIGN.md and code.html Tailwind config.
var (
	// Surface hierarchy
	ColorSurface                 = color.NRGBA{R: 0xf7, G: 0xf9, B: 0xfb, A: 0xff} // #f7f9fb — window background
	ColorSurfaceContainerLow     = color.NRGBA{R: 0xf0, G: 0xf4, B: 0xf7, A: 0xff} // #f0f4f7 — sidebar
	ColorSurfaceContainerLowest  = color.NRGBA{R: 0xff, G: 0xff, B: 0xff, A: 0xff} // #ffffff — cards
	ColorSurfaceContainer        = color.NRGBA{R: 0xe8, G: 0xef, B: 0xf3, A: 0xff} // #e8eff3 — table header rows
	ColorSurfaceContainerHigh    = color.NRGBA{R: 0xe1, G: 0xe9, B: 0xee, A: 0xff} // #e1e9ee — hover states
	ColorSurfaceContainerHighest = color.NRGBA{R: 0xd9, G: 0xe4, B: 0xea, A: 0xff} // #d9e4ea — input fields

	// Primary (institutional blue)
	ColorPrimary            = color.NRGBA{R: 0x3a, G: 0x5f, B: 0x94, A: 0xff} // #3a5f94
	ColorPrimaryDim         = color.NRGBA{R: 0x2d, G: 0x53, B: 0x87, A: 0xff} // #2d5387
	ColorPrimaryContainer   = color.NRGBA{R: 0xd5, G: 0xe3, B: 0xff, A: 0xff} // #d5e3ff — active nav bg
	ColorOnPrimary          = color.NRGBA{R: 0xf6, G: 0xf7, B: 0xff, A: 0xff} // #f6f7ff — text on primary
	ColorOnPrimaryContainer = color.NRGBA{R: 0x2c, G: 0x52, B: 0x87, A: 0xff} // #2c5287

	// Typography
	ColorOnSurface        = color.NRGBA{R: 0x2a, G: 0x34, B: 0x39, A: 0xff} // #2a3439 — primary text
	ColorOnSurfaceVariant = color.NRGBA{R: 0x56, G: 0x61, B: 0x66, A: 0xff} // #566166 — labels, helper text
	ColorOnBackground     = color.NRGBA{R: 0x2a, G: 0x34, B: 0x39, A: 0xff} // #2a3439

	// Tertiary (success / active / verified)
	ColorTertiary            = color.NRGBA{R: 0x00, G: 0x6d, B: 0x4a, A: 0xff} // #006d4a
	ColorTertiaryContainer   = color.NRGBA{R: 0x69, G: 0xf6, B: 0xb8, A: 0xff} // #69f6b8
	ColorOnTertiaryContainer = color.NRGBA{R: 0x00, G: 0x5a, B: 0x3c, A: 0xff} // #005a3c

	// Error (duplicates / flagged)
	ColorError            = color.NRGBA{R: 0x9f, G: 0x40, B: 0x3d, A: 0xff} // #9f403d
	ColorErrorContainer   = color.NRGBA{R: 0xfe, G: 0x89, B: 0x83, A: 0xff} // #fe8983
	ColorOnErrorContainer = color.NRGBA{R: 0x75, G: 0x21, B: 0x21, A: 0xff} // #752121

	// Secondary
	ColorSecondary          = color.NRGBA{R: 0x52, G: 0x60, B: 0x74, A: 0xff} // #526074
	ColorSecondaryContainer = color.NRGBA{R: 0xd5, G: 0xe3, B: 0xfc, A: 0xff} // #d5e3fc

	// Outline
	ColorOutlineVariant = color.NRGBA{R: 0xa9, G: 0xb4, B: 0xb9, A: 0xff} // #a9b4b9

	// Amber / warning (possible duplicates)
	ColorAmber   = color.NRGBA{R: 0xb4, G: 0x5f, B: 0x09, A: 0xff} // amber-700 approx
	ColorAmberBg = color.NRGBA{R: 0xff, G: 0xf7, B: 0xe6, A: 0xff} // amber-100 approx

	// Transparent helpers
	ColorTransparent = color.NRGBA{A: 0x00}
)

// withAlpha returns a copy of c with the specified alpha (0–255).
func withAlpha(c color.NRGBA, alpha uint8) color.NRGBA {
	c.A = alpha
	return c
}
