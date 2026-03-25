package ui

import (
	"context"
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
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

	beneficiaryCountValue := widget.NewLabel("unknown")
	importCountValue := widget.NewLabel("unknown")
	exportCountValue := widget.NewLabel("unknown")
	auditCountValue := widget.NewLabel("unknown")
	psgcChecksumValue := widget.NewLabel(psgcChecksum(runtime.PSGCReport))

	refreshCounts := func() error {
		beneficiaries, err := runtime.Repository.ListBeneficiaries(context.Background(), repository.BeneficiaryListQuery{
			IncludeDeleted: true,
			Limit:          1,
			Offset:         0,
		})
		if err != nil {
			return err
		}
		imports, err := runtime.Repository.ListImportLogs(context.Background(), repository.ImportLogListQuery{Limit: 1})
		if err != nil {
			return err
		}
		exports, err := runtime.Repository.ListExportLogs(context.Background(), repository.ExportLogListQuery{Limit: 1})
		if err != nil {
			return err
		}
		audits, err := runtime.Repository.ListAuditLogs(context.Background(), repository.AuditLogQuery{Limit: 1})
		if err != nil {
			return err
		}

		fyne.Do(func() {
			beneficiaryCountValue.SetText(fmt.Sprintf("%d", beneficiaries.Total))
			importCountValue.SetText(fmt.Sprintf("%d", len(imports)))
			exportCountValue.SetText(fmt.Sprintf("%d", len(exports)))
			auditCountValue.SetText(fmt.Sprintf("%d", len(audits)))
			psgcChecksumValue.SetText(psgcChecksum(runtime.PSGCReport))
		})
		return nil
	}
	_ = refreshCounts()

	metrics := container.NewGridWithColumns(2,
		infoCard("Beneficiaries", beneficiaryCountValue),
		infoCard("Import logs", importCountValue),
		infoCard("Export logs", exportCountValue),
		infoCard("Audit logs", auditCountValue),
		infoCard("PSGC checksum", psgcChecksumValue),
	)

	refreshBtn := widget.NewButton("Refresh summary", func() {
		runtime.RunAsync("Refreshing dashboard", func() error {
			return refreshCounts()
		})
	})

	return container.NewVBox(
		widget.NewLabel("Dashboard"),
		widget.NewLabel("Quick operational snapshot for the offline beneficiary workflow."),
		refreshBtn,
		metrics,
	)
}

func infoCard(title string, value fyne.CanvasObject) fyne.CanvasObject {
	return container.NewVBox(
		widget.NewLabelWithStyle(title, fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		value,
	)
}
