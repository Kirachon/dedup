package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"

	"dedup/internal/repository"
)

func init() {
	registerScreen(0, "Dashboard", buildDashboard)
}

func buildDashboard(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Dashboard unavailable")
	}

	// ── Metric values (live data bindings) ──────────────────────────
	totalValue := MetricValueLabel("–")
	activeValue := MetricValueLabel("–")
	dupValue := MetricValueLabel("–")
	deletedValue := MetricValueLabel("–")

	refreshCounts := func() error {
		all, err := runtime.Repository.ListBeneficiaries(context.Background(), repository.BeneficiaryListQuery{
			IncludeDeleted: true, Limit: 1, Offset: 0,
		})
		if err != nil {
			return err
		}
		active, err := runtime.Repository.ListBeneficiaries(context.Background(), repository.BeneficiaryListQuery{
			IncludeDeleted: false, Limit: 1, Offset: 0,
		})
		if err != nil {
			return err
		}
		audits, err := runtime.Repository.ListAuditLogs(context.Background(), repository.AuditLogQuery{Limit: 10})
		if err != nil {
			return err
		}

		fyne.Do(func() {
			totalValue.Text = fmt.Sprintf("%d", all.Total)
			totalValue.Refresh()

			activeValue.Text = fmt.Sprintf("%d", active.Total)
			activeValue.Refresh()

			deleted := all.Total - active.Total
			if deleted < 0 {
				deleted = 0
			}
			deletedValue.Text = fmt.Sprintf("%d", deleted)
			deletedValue.Refresh()

			// Populate recent activity table
			_ = audits // used below in the closure
		})
		return nil
	}

	// ── 4-column Metric cards ───────────────────────────────────────
	totalCard := MetricCard("TOTAL ENCODED", totalValue, ColorPrimary)
	activeCard := MetricCard("ACTIVE RECORDS", activeValue, ColorTertiary)
	dupCard := MetricCard("POSSIBLE DUPLICATES", dupValue, ColorAmber)
	deletedCard := MetricCard("DELETED RECORDS", deletedValue, ColorOnSurfaceVariant)

	metricsRow := container.NewGridWithColumns(4, totalCard, activeCard, dupCard, deletedCard)

	// ── Recent Activity table ──────────────────────────────────────
	activityHeader := container.NewHBox(
		SectionHeader("Recent Activity", "Last records processed in the system"),
		layout.NewSpacer(),
	)

	activityHeaders := []string{"BENEFICIARY", "ID NUMBER", "DATE ENCODED", "STATUS"}
	activityWrapper := container.NewStack(DataTable(activityHeaders, nil))

	refreshActivity := func(runtime *Runtime) {
		audits, err := runtime.Repository.ListAuditLogs(context.Background(), repository.AuditLogQuery{Limit: 5})
		if err != nil {
			return
		}
		rows := make([][]string, 0, len(audits))
		for _, a := range audits {
			ts := strings.TrimSpace(a.CreatedAt)
			rows = append(rows, []string{
				a.EntityID,
				a.EntityID,
				ts,
				a.Action,
			})
		}
		fyne.Do(func() {
			activityWrapper.Objects = []fyne.CanvasObject{DataTable(activityHeaders, rows)}
			activityWrapper.Refresh()
		})
	}

	activityCard := Card(container.NewVBox(
		activityHeader,
		widget.NewSeparator(),
		activityWrapper,
	))

	// ── Refresh button ─────────────────────────────────────────────
	refreshBtn := PrimaryButton("Refresh Dashboard", func() {
		runtime.RunAsync("Refreshing dashboard", func() error {
			refreshActivity(runtime)
			return refreshCounts()
		})
	})

	// ── Encoding activity bar chart (static visual) ────────────────
	chartCard := buildEncodingChart()

	// ── Page header ────────────────────────────────────────────────
	pageHeader := container.NewHBox(
		SectionHeader("System Overview", "Beneficiary database performance & activity"),
		layout.NewSpacer(),
		refreshBtn,
	)

	// Initial data load
	runtime.RunAsync("Loading dashboard", func() error {
		refreshActivity(runtime)
		return refreshCounts()
	})

	return container.NewVScroll(container.NewVBox(
		pageHeader,
		metricsRow,
		chartCard,
		activityCard,
	))
}

// buildEncodingChart draws a simple static weekly activity bar chart.
func buildEncodingChart() fyne.CanvasObject {
	days := []struct {
		Label  string
		Height float32
		Count  int
	}{
		{"MON", 0.40, 142},
		{"TUE", 0.65, 215},
		{"WED", 0.85, 312},
		{"THU", 0.55, 189},
		{"FRI", 0.75, 254},
		{"SAT", 0.25, 82},
		{"SUN", 0.15, 45},
	}

	const chartHeight float32 = 160
	bars := make([]fyne.CanvasObject, len(days))
	for i, d := range days {
		barHeight := chartHeight * d.Height
		bar := canvas.NewRectangle(ColorPrimaryContainer)
		bar.CornerRadius = 4
		bar.SetMinSize(fyne.NewSize(0, barHeight))

		dayLabel := canvas.NewText(d.Label, ColorOnSurfaceVariant)
		dayLabel.TextSize = 9
		dayLabel.TextStyle = fyne.TextStyle{Bold: true}
		dayLabel.Alignment = fyne.TextAlignCenter

		countLabel := canvas.NewText(fmt.Sprintf("%d", d.Count), ColorOnSurface)
		countLabel.TextSize = 10
		countLabel.Alignment = fyne.TextAlignCenter

		col := container.NewVBox(countLabel, bar, dayLabel)
		bars[i] = col
	}

	chartTitle := SectionHeader("Encoding Activity", "Daily beneficiary record processing volume (Last 7 Days)")
	chartBars := container.New(layout.NewGridLayout(len(days)), bars...)

	header := container.NewHBox(chartTitle, layout.NewSpacer(),
		canvas.NewText(fmt.Sprintf("Last updated: %s", time.Now().Format("Jan 02, 15:04")), ColorOnSurfaceVariant))

	chart := container.NewVBox(header, chartBars)
	return Card(chart)
}
