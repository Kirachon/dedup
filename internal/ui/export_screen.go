package ui

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dedup/internal/exporter"
	"dedup/internal/model"
	"dedup/internal/repository"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

func init() {
	registerScreen(40, "Export", buildExportScreen)
}

func buildExportScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Export screen unavailable")
	}

	var (
		exportLogsMu sync.RWMutex
		exportLogs   []model.ExportLog
	)

	outputPath := widget.NewEntry()
	outputPath.SetText(exportScreenDefaultOutputPath(runtime.DBPath, time.Now().UTC()))
	outputPath.SetPlaceHolder(`e.g. D:\Exports\beneficiaries.csv`)

	operatorName := widget.NewEntry()
	operatorName.SetPlaceHolder("Enter operator name")

	includeUnresolved := widget.NewCheck("Include unresolved duplicates", nil)

	resultView := widget.NewMultiLineEntry()
	resultView.Disable()
	resultView.Wrapping = fyne.TextWrapWord
	resultView.SetPlaceHolder("Export results will appear here")

	setResult := func(message string) {
		fyne.Do(func() {
			resultView.SetText(strings.TrimSpace(message))
		})
	}

	refreshExportLogs := func() error {
		logs, err := runtime.Repository.ListExportLogs(context.Background(), repository.ExportLogListQuery{
			Limit:  100,
			Offset: 0,
		})
		if err != nil {
			return err
		}

		exportLogsMu.Lock()
		exportLogs = logs
		exportLogsMu.Unlock()
		return nil
	}

	exportLogList := widget.NewList(
		func() int {
			exportLogsMu.RLock()
			defer exportLogsMu.RUnlock()
			return len(exportLogs)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(id widget.ListItemID, item fyne.CanvasObject) {
			label, ok := item.(*widget.Label)
			if !ok {
				return
			}
			exportLogsMu.RLock()
			defer exportLogsMu.RUnlock()
			if id < 0 || id >= len(exportLogs) {
				label.SetText("")
				return
			}
			label.SetText(exportScreenFormatExportLog(exportLogs[id]))
		},
	)

	loadAndRefreshLogs := func() {
		runtime.SetBusy(true)
		go func() {
			err := refreshExportLogs()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("Export log refresh failed")
					runtime.SetActivity(err.Error())
					return
				}
				exportLogList.Refresh()
				runtime.SetActivity("Export history refreshed")
			})
		}()
	}

	exportLogList.OnSelected = func(id widget.ListItemID) {
		exportLogsMu.RLock()
		if id < 0 || id >= len(exportLogs) {
			exportLogsMu.RUnlock()
			return
		}
		exportID := exportLogs[id].ExportID
		exportLogsMu.RUnlock()

		runtime.SetBusy(true)
		runtime.SetStatus("Loading export log details")
		go func() {
			details, err := runtime.Repository.GetExportLog(context.Background(), exportID)
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("Failed to load export details")
					runtime.SetActivity(err.Error())
					return
				}
				if details == nil {
					setResult("No export details found")
					return
				}
				setResult(exportScreenDescribeLog(details))
			})
		}()
	}

	runAsyncExport := func(status string, fn func() error) {
		runtime.SetBusy(true)
		runtime.SetStatus(status)
		go func() {
			err := fn()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("Export operation failed")
					runtime.SetActivity(err.Error())
					setResult("Error: " + err.Error())
					return
				}
				exportLogList.Refresh()
			})
		}()
	}

	exportBtn := widget.NewButton("Export CSV", func() {
		runAsyncExport("Running export", func() error {
			req := exporter.Request{
				OutputPath:                  strings.TrimSpace(outputPath.Text),
				OperatorName:                strings.TrimSpace(operatorName.Text),
				IncludeUnresolvedDuplicates: includeUnresolved.Checked,
			}
			result, err := runtime.Exporter.ExportCSV(context.Background(), req)
			if err != nil {
				return err
			}
			if err := refreshExportLogs(); err != nil {
				return err
			}

			fyne.Do(func() {
				setResult(exportScreenDescribeResult(result))
				runtime.SetStatus("Export completed")
				runtime.SetActivity(fmt.Sprintf("Rows exported: %d", result.RowsExported))
			})
			return nil
		})
	})

	resetPathBtn := widget.NewButton("Reset path", func() {
		outputPath.SetText(exportScreenDefaultOutputPath(runtime.DBPath, time.Now().UTC()))
	})
	refreshBtn := widget.NewButton("Refresh history", loadAndRefreshLogs)

	loadAndRefreshLogs()

	// ── Export Options Card ──────────────────────────────────────
	exportBtn.Importance = widget.HighImportance

	optionsForm := widget.NewForm(
		widget.NewFormItem("Output path", outputPath),
		widget.NewFormItem("Operator", operatorName),
		widget.NewFormItem("", includeUnresolved),
	)
	optionsCard := Card(container.NewVBox(
		SectionHeader("Export Options", "Configure and run a CSV export"),
		widget.NewSeparator(),
		optionsForm,
		container.NewHBox(exportBtn, resetPathBtn, layout.NewSpacer(), refreshBtn),
		resultView,
	))

	// ── Export History Table ─────────────────────────────────────
	historyCard := Card(container.NewBorder(
		container.NewVBox(
			SectionHeader("Export History", "Previous export operations"),
			widget.NewSeparator(),
		),
		nil, nil, nil,
		exportLogList,
	))

	// ── Page header ──────────────────────────────────────────────
	pageHeader := SectionHeader("Export Data", "Export beneficiary records to CSV")

	return container.NewVScroll(container.NewVBox(
		pageHeader,
		optionsCard,
		historyCard,
	))
}

func exportScreenDefaultOutputPath(dbPath string, now time.Time) string {
	baseDir := filepath.Dir(strings.TrimSpace(dbPath))
	if baseDir == "" || baseDir == "." {
		baseDir = "."
	}
	exportsDir := filepath.Join(baseDir, "exports")
	name := fmt.Sprintf("beneficiaries-%s.csv", now.UTC().Format("20060102-150405"))
	return filepath.Join(exportsDir, name)
}

func exportScreenDescribeResult(result *exporter.Result) string {
	if result == nil {
		return "export result unavailable"
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Export completed\nExport ID: %s\nPath: %s\nRows considered: %d\nRows exported: %d\nCreated at: %s",
		result.ExportID,
		result.OutputPath,
		result.RowsConsidered,
		result.RowsExported,
		result.CreatedAtUTC,
	))
}

func exportScreenDescribeLog(log *model.ExportLog) string {
	if log == nil {
		return "export log unavailable"
	}
	performedBy := "system"
	if log.PerformedBy != nil && strings.TrimSpace(*log.PerformedBy) != "" {
		performedBy = strings.TrimSpace(*log.PerformedBy)
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Export ID: %s\nFile: %s\nType: %s\nRows exported: %d\nCreated at: %s\nPerformed by: %s",
		log.ExportID,
		log.FileName,
		log.ExportType,
		log.RowsExported,
		log.CreatedAt,
		performedBy,
	))
}

func exportScreenFormatExportLog(log model.ExportLog) string {
	performedBy := "system"
	if log.PerformedBy != nil && strings.TrimSpace(*log.PerformedBy) != "" {
		performedBy = strings.TrimSpace(*log.PerformedBy)
	}
	return fmt.Sprintf(
		"%s | %s | rows=%d | by=%s",
		strings.TrimSpace(log.CreatedAt),
		strings.TrimSpace(log.ExportID),
		log.RowsExported,
		performedBy,
	)
}
