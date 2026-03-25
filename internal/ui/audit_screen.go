package ui

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"dedup/internal/model"
	"dedup/internal/repository"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

func init() {
	registerScreen(70, "Audit Logs", buildAuditScreen)
}

const auditPageSize = 20

func buildAuditScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Audit Logs unavailable")
	}

	var (
		auditMu  sync.RWMutex
		allLogs  []model.AuditLog
		filtered []model.AuditLog
		page     = 0
	)

	// ── Table content area ──────────────────────────────────────────
	headers := []string{"TIMESTAMP", "OPERATOR", "ACTION", "ENTITY ID", "DETAILS"}
	tableArea := container.NewStack(DataTable(headers, nil))

	// ── Pagination labels ───────────────────────────────────────────
	pageLabel := canvas.NewText("Page 1 of 1", ColorOnSurfaceVariant)
	pageLabel.TextSize = 12

	totalLabel := canvas.NewText("0 records", ColorOnSurfaceVariant)
	totalLabel.TextSize = 11

	// ── Render current page into the table area ─────────────────────
	renderPage := func() {
		auditMu.RLock()
		defer auditMu.RUnlock()

		start := page * auditPageSize
		end := start + auditPageSize
		if end > len(filtered) {
			end = len(filtered)
		}
		if start > len(filtered) {
			start = len(filtered)
		}
		pageSlice := filtered[start:end]

		rows := make([][]string, 0, len(pageSlice))
		for _, a := range pageSlice {
			ts := strings.TrimSpace(a.CreatedAt)
			operator := strings.TrimSpace(a.PerformedBy)
			action := strings.TrimSpace(a.Action)
			entityID := strings.TrimSpace(a.EntityID)
			details := ""
			if a.DetailsJSON != nil {
				details = strings.TrimSpace(*a.DetailsJSON)
				if len(details) > 60 {
					details = details[:60] + "…"
				}
			}
			rows = append(rows, []string{ts, operator, action, entityID, details})
		}

		totalPages := max(1, (len(filtered)+auditPageSize-1)/auditPageSize)

		fyne.Do(func() {
			tableArea.Objects = []fyne.CanvasObject{DataTable(headers, rows)}
			tableArea.Refresh()
			pageLabel.Text = fmt.Sprintf("Page %d of %d", page+1, totalPages)
			pageLabel.Refresh()
			totalLabel.Text = fmt.Sprintf("%d records", len(filtered))
			totalLabel.Refresh()
		})
	}

	// ── Filter logic ────────────────────────────────────────────────
	filterEntry := widget.NewEntry()
	filterEntry.SetPlaceHolder("Filter by operator, action, or entity ID")
	filterEntry.OnChanged = func(q string) {
		q = strings.ToLower(strings.TrimSpace(q))
		auditMu.Lock()
		if q == "" {
			filtered = allLogs
		} else {
			result := make([]model.AuditLog, 0)
			for _, a := range allLogs {
				if strings.Contains(strings.ToLower(a.PerformedBy), q) ||
					strings.Contains(strings.ToLower(a.Action), q) ||
					strings.Contains(strings.ToLower(a.EntityID), q) {
					result = append(result, a)
				}
			}
			filtered = result
		}
		page = 0
		auditMu.Unlock()
		renderPage()
	}

	// ── Load data ───────────────────────────────────────────────────
	loadAuditLogs := func() error {
		logs, err := runtime.Repository.ListAuditLogs(context.Background(), repository.AuditLogQuery{
			Limit: 500, Offset: 0,
		})
		if err != nil {
			return err
		}
		auditMu.Lock()
		allLogs = logs
		filtered = logs
		page = 0
		auditMu.Unlock()
		renderPage()
		return nil
	}

	// ── Pagination buttons ──────────────────────────────────────────
	prevBtn := widget.NewButton("← Prev", func() {
		if page > 0 {
			page--
			renderPage()
		}
	})
	nextBtn := widget.NewButton("Next →", func() {
		auditMu.RLock()
		maxPage := max(0, (len(filtered)+auditPageSize-1)/auditPageSize-1)
		auditMu.RUnlock()
		if page < maxPage {
			page++
			renderPage()
		}
	})

	refreshBtn := PrimaryButton("Refresh", func() {
		runtime.RunAsync("Refreshing audit logs", loadAuditLogs)
	})

	pagination := container.NewHBox(
		prevBtn,
		pageLabel,
		nextBtn,
		layout.NewSpacer(),
		totalLabel,
		refreshBtn,
	)

	// ── Filter bar ──────────────────────────────────────────────────
	filterBar := container.NewHBox(filterEntry)
	filterEntry.Resize(fyne.NewSize(400, 0))

	// ── Main card ───────────────────────────────────────────────────
	auditCard := Card(container.NewBorder(
		container.NewVBox(
			SectionHeader("Audit Logs", "System action history — all creates, updates, decisions, and imports"),
			widget.NewSeparator(),
			filterBar,
		),
		container.NewVBox(widget.NewSeparator(), pagination),
		nil, nil,
		container.NewVScroll(tableArea),
	))

	pageHeader := SectionHeader("Audit Logs", "Complete record of system and operator actions")

	// Initial load
	runtime.RunAsync("Loading audit logs", loadAuditLogs)

	return container.NewVBox(pageHeader, auditCard)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
