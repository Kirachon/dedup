package ui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"dedup/internal/importer"
	"dedup/internal/model"
	"dedup/internal/repository"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

func init() {
	registerScreen(30, "Import", buildImportScreen)
}

func buildImportScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Import screen unavailable")
	}

	var (
		importLogsMu sync.RWMutex
		importLogs   []model.ImportLog
	)

	sourceType := widget.NewSelect([]string{
		string(model.ImportSourceCSV),
		string(model.ImportSourceExchangePackage),
	}, nil)
	sourceType.SetSelected(string(model.ImportSourceCSV))

	sourcePath := widget.NewEntry()
	sourcePath.SetPlaceHolder(`Use beneficiary_import_template.csv or exchange.zip`)

	operatorName := widget.NewEntry()
	operatorName.SetPlaceHolder("Enter operator name")

	sourceReference := widget.NewEntry()
	sourceReference.SetPlaceHolder("Optional batch reference")

	idempotencyKey := widget.NewEntry()
	idempotencyKey.SetPlaceHolder("Leave blank to auto-generate")

	previewToken := widget.NewMultiLineEntry()
	previewToken.SetPlaceHolder("Generated after preview")
	previewToken.Wrapping = fyne.TextWrapWord

	checkpointToken := widget.NewMultiLineEntry()
	checkpointToken.SetPlaceHolder("Generated if resume is available")
	checkpointToken.Wrapping = fyne.TextWrapWord

	resultView := widget.NewMultiLineEntry()
	resultView.SetPlaceHolder("Import results and validation details will appear here. The bundled CSV template uses the matching workflow layout.")
	resultView.Wrapping = fyne.TextWrapWord
	resultView.Disable()

	setResult := func(message string) {
		fyne.Do(func() {
			resultView.SetText(strings.TrimSpace(message))
		})
	}

	refreshImportLogs := func() error {
		logs, err := runtime.Repository.ListImportLogs(context.Background(), repository.ImportLogListQuery{
			Limit:  100,
			Offset: 0,
		})
		if err != nil {
			return err
		}

		importLogsMu.Lock()
		importLogs = logs
		importLogsMu.Unlock()
		return nil
	}

	importLogList := widget.NewList(
		func() int {
			importLogsMu.RLock()
			defer importLogsMu.RUnlock()
			return len(importLogs)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(id widget.ListItemID, item fyne.CanvasObject) {
			label, ok := item.(*widget.Label)
			if !ok {
				return
			}
			importLogsMu.RLock()
			defer importLogsMu.RUnlock()
			if id < 0 || id >= len(importLogs) {
				label.SetText("")
				return
			}
			label.SetText(importScreenFormatImportLog(importLogs[id]))
		},
	)

	loadAndRefreshLogs := func() {
		runtime.SetBusy(true)
		go func() {
			err := refreshImportLogs()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("Import log refresh failed")
					runtime.SetActivity(err.Error())
					return
				}
				importLogList.Refresh()
				runtime.SetActivity("Import history refreshed")
			})
		}()
	}

	importLogList.OnSelected = func(id widget.ListItemID) {
		importLogsMu.RLock()
		if id < 0 || id >= len(importLogs) {
			importLogsMu.RUnlock()
			return
		}
		importID := importLogs[id].ImportID
		importLogsMu.RUnlock()

		runtime.SetBusy(true)
		runtime.SetStatus("Loading import log details")
		go func() {
			details, err := runtime.Repository.GetImportLog(context.Background(), importID)
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("Failed to load import log details")
					runtime.SetActivity(err.Error())
					return
				}
				if details == nil {
					setResult("No import details found")
					return
				}
				setResult(importScreenDescribeImportResult(&importer.ImportResult{
					ImportID:        details.ImportID,
					Status:          details.Status,
					RowsRead:        details.RowsRead,
					RowsInserted:    details.RowsInserted,
					RowsSkipped:     details.RowsSkipped,
					RowsFailed:      details.RowsFailed,
					CheckpointToken: details.CheckpointToken,
					CompletedAtUTC:  details.CompletedAt,
				}))
			})
		}()
	}

	runAsyncImport := func(status string, fn func() error) {
		runtime.SetBusy(true)
		runtime.SetStatus(status)
		go func() {
			err := fn()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("Import operation failed")
					runtime.SetActivity(err.Error())
					setResult("Error: " + err.Error())
					return
				}
				importLogList.Refresh()
			})
		}()
	}

	previewBtn := widget.NewButton("Preview", func() {
		source := importer.Source{
			Type:            importScreenNormalizeSourceType(sourceType.Selected),
			Path:            strings.TrimSpace(sourcePath.Text),
			OperatorName:    strings.TrimSpace(operatorName.Text),
			SourceReference: strings.TrimSpace(sourceReference.Text),
		}

		runAsyncImport("Previewing import source", func() error {
			report, err := runtime.Importer.Preview(context.Background(), source)
			if err != nil {
				return err
			}
			if err := refreshImportLogs(); err != nil {
				return err
			}

			fyne.Do(func() {
				previewToken.SetText(report.PreviewToken)
				if strings.TrimSpace(sourceReference.Text) == "" {
					sourceReference.SetText(report.DetectedSourceReference)
				}
				setResult(importScreenDescribePreview(report))
				runtime.SetStatus("Import preview completed")
				runtime.SetActivity(fmt.Sprintf("Rows valid %d / total %d", report.RowCountValid, report.RowCountTotal))
			})
			return nil
		})
	})

	commitBtn := widget.NewButton("Commit", func() {
		runAsyncImport("Committing import", func() error {
			token := strings.TrimSpace(previewToken.Text)
			if token == "" {
				return fmt.Errorf("preview token is required before commit")
			}

			key := strings.TrimSpace(idempotencyKey.Text)
			if key == "" {
				key = fmt.Sprintf("import-%d", time.Now().UTC().UnixNano())
				fyne.Do(func() {
					idempotencyKey.SetText(key)
				})
			}

			result, err := runtime.Importer.Commit(context.Background(), token, key)
			if err != nil {
				return err
			}
			if err := refreshImportLogs(); err != nil {
				return err
			}

			fyne.Do(func() {
				if result.CheckpointToken != nil {
					checkpointToken.SetText(*result.CheckpointToken)
				}
				setResult(importScreenDescribeImportResult(result))
				runtime.SetStatus("Import commit completed")
				runtime.SetActivity("Commit status: " + strings.TrimSpace(result.Status))
			})
			return nil
		})
	})

	resumeBtn := widget.NewButton("Resume", func() {
		runAsyncImport("Resuming import", func() error {
			token := strings.TrimSpace(checkpointToken.Text)
			if token == "" {
				return fmt.Errorf("checkpoint token is required for resume")
			}

			result, err := runtime.Importer.Resume(context.Background(), token)
			if err != nil {
				return err
			}
			if err := refreshImportLogs(); err != nil {
				return err
			}

			fyne.Do(func() {
				if result.CheckpointToken != nil {
					checkpointToken.SetText(*result.CheckpointToken)
				}
				setResult(importScreenDescribeImportResult(result))
				runtime.SetStatus("Import resume completed")
				runtime.SetActivity("Resume status: " + strings.TrimSpace(result.Status))
			})
			return nil
		})
	})

	refreshBtn := widget.NewButton("Refresh history", loadAndRefreshLogs)
	loadAndRefreshLogs()

	// ── Import Options Card ──────────────────────────────────────
	previewBtn.Importance = widget.HighImportance

	optionsForm := widget.NewForm(
		widget.NewFormItem("Source type", sourceType),
		widget.NewFormItem("Source path", sourcePath),
		widget.NewFormItem("Operator", operatorName),
		widget.NewFormItem("Source ref", sourceReference),
		widget.NewFormItem("Idempotency key", idempotencyKey),
	)

	tokenSection := widget.NewForm(
		widget.NewFormItem("Preview token", previewToken),
		widget.NewFormItem("Checkpoint token", checkpointToken),
	)

	optionsCard := Card(container.NewVBox(
		SectionHeader("Import Data", "Select CSV or ZIP exchange package to import"),
		widget.NewSeparator(),
		optionsForm,
		container.NewHBox(previewBtn, commitBtn, resumeBtn, layout.NewSpacer(), refreshBtn),
		tokenSection,
		resultView,
	))

	// ── Import History Table ─────────────────────────────────────
	historyCard := Card(container.NewBorder(
		container.NewVBox(
			SectionHeader("Import History", "Previous import operations"),
			widget.NewSeparator(),
		),
		nil, nil, nil,
		importLogList,
	))

	pageHeader := SectionHeader("Import Data", "Import beneficiary records from CSV or exchange package")
	templateNote := widget.NewLabel("CSV template bundled with the app: beneficiary_import_template.csv")

	return container.NewVScroll(container.NewVBox(
		pageHeader,
		templateNote,
		optionsCard,
		historyCard,
	))
}

func importScreenNormalizeSourceType(value string) model.ImportSource {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch model.ImportSource(normalized) {
	case model.ImportSourceExchangePackage:
		return model.ImportSourceExchangePackage
	default:
		return model.ImportSourceCSV
	}
}

func importScreenDescribePreview(report *importer.PreviewReport) string {
	if report == nil {
		return "preview report unavailable"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Preview completed\n")
	fmt.Fprintf(&b, "Source type: %s\n", report.SourceType)
	fmt.Fprintf(&b, "Source hash: %s\n", report.SourceHash)
	fmt.Fprintf(&b, "Rows: total=%d valid=%d invalid=%d\n", report.RowCountTotal, report.RowCountValid, report.RowCountInvalid)
	fmt.Fprintf(&b, "Header valid: %t\n", report.HeaderValidationPassed)
	if len(report.SampleErrors) > 0 {
		b.WriteString("Sample errors:\n")
		for _, sample := range report.SampleErrors {
			fmt.Fprintf(&b, "- %s\n", strings.TrimSpace(sample))
		}
	}
	return strings.TrimSpace(b.String())
}

func importScreenDescribeImportResult(result *importer.ImportResult) string {
	if result == nil {
		return "import result unavailable"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Import ID: %s\n", result.ImportID)
	fmt.Fprintf(&b, "Status: %s\n", result.Status)
	fmt.Fprintf(&b, "Rows read=%d inserted=%d skipped=%d failed=%d\n", result.RowsRead, result.RowsInserted, result.RowsSkipped, result.RowsFailed)
	if result.CheckpointToken != nil && strings.TrimSpace(*result.CheckpointToken) != "" {
		b.WriteString("Checkpoint token generated for resume\n")
	}
	if result.CompletedAtUTC != nil {
		fmt.Fprintf(&b, "Completed at: %s\n", strings.TrimSpace(*result.CompletedAtUTC))
	}
	return strings.TrimSpace(b.String())
}

func importScreenFormatImportLog(log model.ImportLog) string {
	return fmt.Sprintf(
		"%s | %s | %s | read=%d ins=%d skip=%d fail=%d",
		strings.TrimSpace(log.StartedAt),
		strings.TrimSpace(log.ImportID),
		strings.TrimSpace(log.Status),
		log.RowsRead,
		log.RowsInserted,
		log.RowsSkipped,
		log.RowsFailed,
	)
}
