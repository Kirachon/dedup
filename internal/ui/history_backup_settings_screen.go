package ui

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

func init() {
	registerScreen(50, "History", buildHistoryScreen)
	registerScreen(60, "Settings", buildSettingsScreen)
	registerScreen(70, "Backup", buildBackupScreen)
}

func buildHistoryScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("History screen unavailable")
	}

	var (
		importLogsMu sync.RWMutex
		importLogs   []model.ImportLog

		exportLogsMu sync.RWMutex
		exportLogs   []model.ExportLog

		auditLogsMu sync.RWMutex
		auditLogs   []model.AuditLog
	)

	detailView := widget.NewMultiLineEntry()
	detailView.Wrapping = fyne.TextWrapWord
	detailView.Disable()
	detailView.SetPlaceHolder("Select a history item to view details")

	setDetails := func(text string) {
		fyne.Do(func() {
			detailView.SetText(strings.TrimSpace(text))
		})
	}

	importList := widget.NewList(
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
			label.SetText(historyScreenFormatImportLog(importLogs[id]))
		},
	)

	exportList := widget.NewList(
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
			label.SetText(historyScreenFormatExportLog(exportLogs[id]))
		},
	)

	auditList := widget.NewList(
		func() int {
			auditLogsMu.RLock()
			defer auditLogsMu.RUnlock()
			return len(auditLogs)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(id widget.ListItemID, item fyne.CanvasObject) {
			label, ok := item.(*widget.Label)
			if !ok {
				return
			}
			auditLogsMu.RLock()
			defer auditLogsMu.RUnlock()
			if id < 0 || id >= len(auditLogs) {
				label.SetText("")
				return
			}
			label.SetText(historyScreenFormatAuditLog(auditLogs[id]))
		},
	)

	loadHistory := func() error {
		importItems, err := runtime.Repository.ListImportLogs(context.Background(), repository.ImportLogListQuery{
			Limit:  100,
			Offset: 0,
		})
		if err != nil {
			return err
		}
		exportItems, err := runtime.Repository.ListExportLogs(context.Background(), repository.ExportLogListQuery{
			Limit:  100,
			Offset: 0,
		})
		if err != nil {
			return err
		}
		auditItems, err := runtime.Repository.ListAuditLogs(context.Background(), repository.AuditLogQuery{
			Limit:  120,
			Offset: 0,
		})
		if err != nil {
			return err
		}

		importLogsMu.Lock()
		importLogs = importItems
		importLogsMu.Unlock()

		exportLogsMu.Lock()
		exportLogs = exportItems
		exportLogsMu.Unlock()

		auditLogsMu.Lock()
		auditLogs = auditItems
		auditLogsMu.Unlock()
		return nil
	}

	refreshHistory := func() {
		runtime.SetBusy(true)
		runtime.SetStatus("Refreshing history")
		go func() {
			err := loadHistory()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("History refresh failed")
					runtime.SetActivity(err.Error())
					setDetails("Error: " + err.Error())
					return
				}
				importList.Refresh()
				exportList.Refresh()
				auditList.Refresh()
				runtime.SetStatus("History refreshed")
				runtime.SetActivity("Import/export/audit history is up to date")
			})
		}()
	}

	importList.OnSelected = func(id widget.ListItemID) {
		importLogsMu.RLock()
		if id < 0 || id >= len(importLogs) {
			importLogsMu.RUnlock()
			return
		}
		importID := importLogs[id].ImportID
		importLogsMu.RUnlock()

		runtime.SetBusy(true)
		runtime.SetStatus("Loading import details")
		go func() {
			log, err := runtime.Repository.GetImportLog(context.Background(), importID)
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					setDetails("Error: " + err.Error())
					return
				}
				setDetails(historyScreenDescribeImportLog(log))
			})
		}()
	}

	exportList.OnSelected = func(id widget.ListItemID) {
		exportLogsMu.RLock()
		if id < 0 || id >= len(exportLogs) {
			exportLogsMu.RUnlock()
			return
		}
		exportID := exportLogs[id].ExportID
		exportLogsMu.RUnlock()

		runtime.SetBusy(true)
		runtime.SetStatus("Loading export details")
		go func() {
			log, err := runtime.Repository.GetExportLog(context.Background(), exportID)
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					setDetails("Error: " + err.Error())
					return
				}
				setDetails(historyScreenDescribeExportLog(log))
			})
		}()
	}

	auditList.OnSelected = func(id widget.ListItemID) {
		auditLogsMu.RLock()
		defer auditLogsMu.RUnlock()
		if id < 0 || id >= len(auditLogs) {
			return
		}
		setDetails(historyScreenDescribeAuditLog(&auditLogs[id]))
	}

	refreshBtn := widget.NewButton("Refresh history", refreshHistory)
	refreshHistory()

	tabs := container.NewAppTabs(
		container.NewTabItem("Imports", importList),
		container.NewTabItem("Exports", exportList),
		container.NewTabItem("Audit", auditList),
	)
	tabs.SetTabLocation(container.TabLocationTop)

	return container.NewBorder(
		container.NewHBox(widget.NewLabelWithStyle("System History", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}), layout.NewSpacer(), refreshBtn),
		container.NewVBox(widget.NewLabelWithStyle("Details", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}), detailView),
		nil,
		nil,
		tabs,
	)
}

func buildSettingsScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Settings screen unavailable")
	}

	var (
		settingsMu sync.RWMutex
		settings   []model.AppSetting
	)

	runtimeInfo := widget.NewMultiLineEntry()
	runtimeInfo.Wrapping = fyne.TextWrapWord
	runtimeInfo.Disable()
	runtimeInfo.SetText(settingsScreenRuntimeInfo(runtime))

	metadataView := widget.NewMultiLineEntry()
	metadataView.Wrapping = fyne.TextWrapWord
	metadataView.Disable()
	metadataView.SetPlaceHolder("PSGC ingest metadata will appear here")

	settingsList := widget.NewList(
		func() int {
			settingsMu.RLock()
			defer settingsMu.RUnlock()
			return len(settings)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("setting")
		},
		func(id widget.ListItemID, item fyne.CanvasObject) {
			label, ok := item.(*widget.Label)
			if !ok {
				return
			}
			settingsMu.RLock()
			defer settingsMu.RUnlock()
			if id < 0 || id >= len(settings) {
				label.SetText("")
				return
			}
			label.SetText(settingsScreenFormatSetting(settings[id]))
		},
	)

	settingKey := widget.NewEntry()
	settingKey.SetPlaceHolder("Setting key")
	settingValue := widget.NewEntry()
	settingValue.SetPlaceHolder("Setting value")

	settingsResult := widget.NewLabel("")

	setResult := func(msg string) {
		fyne.Do(func() {
			settingsResult.SetText(strings.TrimSpace(msg))
		})
	}

	loadSettingsAndMetadata := func() error {
		items, err := runtime.Repository.ListSettings(context.Background())
		if err != nil {
			return err
		}
		meta, err := runtime.Repository.GetIngestMetadata(context.Background())
		metaText := ""
		if err != nil {
			metaText = "PSGC metadata unavailable: " + err.Error()
		} else {
			metaText = settingsScreenFormatIngestMetadata(meta)
		}

		settingsMu.Lock()
		settings = items
		settingsMu.Unlock()

		fyne.Do(func() {
			metadataView.SetText(metaText)
		})
		return nil
	}

	refreshSettings := func() {
		runtime.SetBusy(true)
		runtime.SetStatus("Refreshing settings")
		go func() {
			err := loadSettingsAndMetadata()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					runtime.SetStatus("Settings refresh failed")
					runtime.SetActivity(err.Error())
					setResult("Error: " + err.Error())
					return
				}
				settingsList.Refresh()
				setResult("Settings and PSGC metadata refreshed")
				runtime.SetStatus("Settings refreshed")
			})
		}()
	}

	saveBtn := widget.NewButton("Save setting", func() {
		key := strings.TrimSpace(settingKey.Text)
		value := strings.TrimSpace(settingValue.Text)
		if key == "" {
			setResult("Setting key is required")
			return
		}

		runtime.SetBusy(true)
		runtime.SetStatus("Saving setting")
		go func() {
			err := runtime.Repository.UpsertSetting(context.Background(), &model.AppSetting{
				SettingKey:   key,
				SettingValue: value,
				UpdatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
			})
			if err != nil {
				fyne.Do(func() {
					runtime.SetBusy(false)
					setResult("Error: " + err.Error())
					runtime.SetActivity(err.Error())
				})
				return
			}
			err = loadSettingsAndMetadata()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					setResult("Setting saved, but refresh failed: " + err.Error())
					return
				}
				settingsList.Refresh()
				setResult("Setting saved")
				runtime.SetActivity("Saved setting key: " + key)
			})
		}()
	})

	settingsList.OnSelected = func(id widget.ListItemID) {
		settingsMu.RLock()
		defer settingsMu.RUnlock()
		if id < 0 || id >= len(settings) {
			return
		}
		settingKey.SetText(settings[id].SettingKey)
		settingValue.SetText(settings[id].SettingValue)
	}

	refreshBtn := widget.NewButton("Refresh", refreshSettings)
	refreshSettings()

	runtimeCard := Card(container.NewVBox(
		SectionHeader("LGU Profile & Runtime Information", ""),
		widget.NewSeparator(),
		runtimeInfo,
		widget.NewSeparator(),
		SectionHeader("PSGC Ingest Metadata", ""),
		metadataView,
	))

	settingsCard := Card(container.NewVBox(
		SectionHeader("Application Settings", "Configure application-level key-value settings"),
		widget.NewSeparator(),
		widget.NewForm(
			widget.NewFormItem("Key", settingKey),
			widget.NewFormItem("Value", settingValue),
		),
		container.NewHBox(saveBtn, refreshBtn, layout.NewSpacer(), settingsResult),
		widget.NewSeparator(),
		settingsList,
	))

	pageHeader := SectionHeader("System Settings", "Manage LGU profile and application configuration")

	return container.NewVScroll(container.NewVBox(
		pageHeader,
		runtimeCard,
		settingsCard,
	))
}

func buildBackupScreen(runtime *Runtime) fyne.CanvasObject {
	if runtime == nil || runtime.Dependencies == nil {
		return widget.NewLabel("Backup screen unavailable")
	}

	snapshotDir := widget.NewEntry()
	snapshotDir.SetText(filepath.Join(filepath.Dir(strings.TrimSpace(runtime.DBPath)), "backups"))
	snapshotDir.SetPlaceHolder(`e.g. D:\Backups`)

	backupOperator := widget.NewEntry()
	backupOperator.SetPlaceHolder("Enter operator name")

	snapshotPath := widget.NewEntry()
	snapshotPath.SetPlaceHolder(`e.g. D:\Backups\snapshot.db`)

	manifestPath := widget.NewEntry()
	manifestPath.SetPlaceHolder(`e.g. D:\Backups\snapshot.db.manifest.json`)

	restoreConfirmation := widget.NewEntry()
	restoreOperator := widget.NewEntry()
	restoreOperator.SetPlaceHolder("Enter operator name")

	expectedConfirmation := widget.NewLabel(runtime.BackupService.ExpectedRestoreConfirmation())
	restoreConfirmation.SetText(runtime.BackupService.ExpectedRestoreConfirmation())

	backupResult := widget.NewMultiLineEntry()
	backupResult.Wrapping = fyne.TextWrapWord
	backupResult.Disable()
	backupResult.SetPlaceHolder("Backup and restore results will appear here")

	setResult := func(text string) {
		fyne.Do(func() {
			backupResult.SetText(strings.TrimSpace(text))
		})
	}

	runBackupAsync := func(status string, fn func() error) {
		runtime.SetBusy(true)
		runtime.SetStatus(status)
		go func() {
			err := fn()
			fyne.Do(func() {
				runtime.SetBusy(false)
				if err != nil {
					setResult("Error: " + err.Error())
					runtime.SetActivity(err.Error())
					return
				}
			})
		}()
	}

	createSnapshotBtn := widget.NewButton("Create snapshot", func() {
		runBackupAsync("Creating backup snapshot", func() error {
			req := service.SnapshotRequest{
				OutputDir:   strings.TrimSpace(snapshotDir.Text),
				PerformedBy: strings.TrimSpace(backupOperator.Text),
			}
			result, err := runtime.BackupService.CreateSnapshot(context.Background(), req)
			if err != nil {
				return err
			}
			fyne.Do(func() {
				snapshotPath.SetText(result.SnapshotPath)
				manifestPath.SetText(result.ManifestPath)
				restoreConfirmation.SetText(runtime.BackupService.ExpectedRestoreConfirmation())
				expectedConfirmation.SetText(runtime.BackupService.ExpectedRestoreConfirmation())
				setResult(backupScreenDescribeSnapshotResult(result))
				runtime.SetActivity("Snapshot created")
			})
			return nil
		})
	})

	validateRestoreBtn := widget.NewButton("Validate restore", func() {
		runBackupAsync("Validating restore package", func() error {
			req := service.RestoreValidationRequest{
				SnapshotPath: strings.TrimSpace(snapshotPath.Text),
				ManifestPath: strings.TrimSpace(manifestPath.Text),
			}
			result, err := runtime.BackupService.ValidateRestore(context.Background(), req)
			if err != nil {
				return err
			}
			fyne.Do(func() {
				expectedConfirmation.SetText(result.ExpectedConfirmation)
				if strings.TrimSpace(restoreConfirmation.Text) == "" {
					restoreConfirmation.SetText(result.ExpectedConfirmation)
				}
				setResult(backupScreenDescribeValidationResult(result))
				runtime.SetActivity("Restore validation completed")
			})
			return nil
		})
	})

	applyRestoreBtn := widget.NewButton("Apply restore", func() {
		runBackupAsync("Applying restore", func() error {
			req := service.RestoreApplyRequest{
				SnapshotPath: strings.TrimSpace(snapshotPath.Text),
				ManifestPath: strings.TrimSpace(manifestPath.Text),
				Confirmation: strings.TrimSpace(restoreConfirmation.Text),
				PerformedBy:  strings.TrimSpace(restoreOperator.Text),
			}
			result, err := runtime.BackupService.ApplyRestore(context.Background(), req)
			if err != nil {
				return err
			}
			fyne.Do(func() {
				setResult(backupScreenDescribeApplyResult(result))
				runtime.SetStatus("Restore applied")
				runtime.SetActivity("Restore completed; restart app workflows if other panels fail due to stale handles")
			})
			return nil
		})
	})

	useExpectedBtn := widget.NewButton("Use expected text", func() {
		restoreConfirmation.SetText(runtime.BackupService.ExpectedRestoreConfirmation())
		expectedConfirmation.SetText(runtime.BackupService.ExpectedRestoreConfirmation())
	})

	return container.NewVBox(
		widget.NewLabelWithStyle("Backup and Restore", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewForm(
			widget.NewFormItem("Snapshot output dir", snapshotDir),
			widget.NewFormItem("Backup operator", backupOperator),
		),
		container.NewHBox(createSnapshotBtn),
		widget.NewSeparator(),
		widget.NewForm(
			widget.NewFormItem("Snapshot path", snapshotPath),
			widget.NewFormItem("Manifest path", manifestPath),
			widget.NewFormItem("Expected confirmation", expectedConfirmation),
			widget.NewFormItem("Confirmation input", restoreConfirmation),
			widget.NewFormItem("Restore operator", restoreOperator),
		),
		container.NewHBox(validateRestoreBtn, applyRestoreBtn, useExpectedBtn),
		widget.NewLabel("Result"),
		backupResult,
	)
}

func historyScreenFormatImportLog(log model.ImportLog) string {
	return fmt.Sprintf("%s | %s | %s", strings.TrimSpace(log.StartedAt), strings.TrimSpace(log.ImportID), strings.TrimSpace(log.Status))
}

func historyScreenFormatExportLog(log model.ExportLog) string {
	return fmt.Sprintf("%s | %s | rows=%d", strings.TrimSpace(log.CreatedAt), strings.TrimSpace(log.ExportID), log.RowsExported)
}

func historyScreenFormatAuditLog(log model.AuditLog) string {
	return fmt.Sprintf("%s | %s | %s", strings.TrimSpace(log.CreatedAt), strings.TrimSpace(log.Action), strings.TrimSpace(log.EntityType))
}

func historyScreenDescribeImportLog(log *model.ImportLog) string {
	if log == nil {
		return "import log not found"
	}
	completed := ""
	if log.CompletedAt != nil {
		completed = strings.TrimSpace(*log.CompletedAt)
	}
	checkpoint := ""
	if log.CheckpointToken != nil {
		checkpoint = strings.TrimSpace(*log.CheckpointToken)
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Import ID: %s\nStatus: %s\nSource type: %s\nSource: %s\nRows read=%d inserted=%d skipped=%d failed=%d\nStarted: %s\nCompleted: %s\nCheckpoint: %s",
		log.ImportID,
		log.Status,
		log.SourceType,
		log.SourceReference,
		log.RowsRead,
		log.RowsInserted,
		log.RowsSkipped,
		log.RowsFailed,
		log.StartedAt,
		completed,
		checkpoint,
	))
}

func historyScreenDescribeExportLog(log *model.ExportLog) string {
	if log == nil {
		return "export log not found"
	}
	performedBy := "system"
	if log.PerformedBy != nil && strings.TrimSpace(*log.PerformedBy) != "" {
		performedBy = strings.TrimSpace(*log.PerformedBy)
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Export ID: %s\nType: %s\nFile: %s\nRows exported: %d\nCreated: %s\nPerformed by: %s",
		log.ExportID,
		log.ExportType,
		log.FileName,
		log.RowsExported,
		log.CreatedAt,
		performedBy,
	))
}

func historyScreenDescribeAuditLog(log *model.AuditLog) string {
	if log == nil {
		return "audit log not found"
	}
	details := ""
	if log.DetailsJSON != nil {
		details = strings.TrimSpace(*log.DetailsJSON)
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Audit ID: %s\nAction: %s\nEntity: %s/%s\nPerformed by: %s\nCreated: %s\nDetails: %s",
		log.AuditID,
		log.Action,
		log.EntityType,
		log.EntityID,
		log.PerformedBy,
		log.CreatedAt,
		details,
	))
}

func settingsScreenRuntimeInfo(runtime *Runtime) string {
	if runtime == nil || runtime.Dependencies == nil {
		return "runtime unavailable"
	}
	return strings.TrimSpace(fmt.Sprintf(
		"App ID: %s\nWindow title: %s\nDB path: %s\nPSGC checksum: %s",
		runtime.Config.AppID,
		runtime.Config.WindowTitle,
		runtime.DBPath,
		psgcChecksum(runtime.PSGCReport),
	))
}

func settingsScreenFormatSetting(item model.AppSetting) string {
	return fmt.Sprintf("%s = %s (updated %s)", item.SettingKey, item.SettingValue, item.UpdatedAt)
}

func settingsScreenFormatIngestMetadata(meta *model.PSGCIngestMetadata) string {
	if meta == nil {
		return "PSGC metadata not available"
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Source file: %s\nChecksum: %s\nRows read: %d\nRegions: %d Provinces: %d Cities: %d Barangays: %d\nIngested at: %s",
		meta.SourceFileName,
		meta.SourceChecksum,
		meta.RowsRead,
		meta.RowsRegions,
		meta.RowsProvinces,
		meta.RowsCities,
		meta.RowsBarangays,
		meta.IngestedAt,
	))
}

func backupScreenDescribeSnapshotResult(result *service.SnapshotResult) string {
	if result == nil {
		return "snapshot result unavailable"
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Snapshot created\nBackup ID: %s\nSnapshot: %s\nManifest: %s\nChecksum: %s\nSize: %d bytes\nCreated at: %s",
		result.BackupID,
		result.SnapshotPath,
		result.ManifestPath,
		result.SnapshotSHA256,
		result.SizeBytes,
		result.CreatedAtUTC,
	))
}

func backupScreenDescribeValidationResult(result *service.RestoreValidationResult) string {
	if result == nil {
		return "restore validation result unavailable"
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Restore validation\nValid: %t Apply ready: %t\nSnapshot: %s\nManifest: %s\nManifest present: %t matches: %t\nChecksum: %s\nSize: %d bytes\nBlocking jobs: %d\nExpected confirmation: %s",
		result.Valid,
		result.ApplyReady,
		result.SnapshotPath,
		result.ManifestPath,
		result.ManifestPresent,
		result.ManifestMatches,
		result.SnapshotSHA256,
		result.SnapshotSizeBytes,
		len(result.BlockingJobs),
		result.ExpectedConfirmation,
	))
}

func backupScreenDescribeApplyResult(result *service.RestoreApplyResult) string {
	if result == nil {
		return "restore apply result unavailable"
	}
	return strings.TrimSpace(fmt.Sprintf(
		"Restore applied\nDB path: %s\nSnapshot: %s\nManifest: %s\nPre-restore copy: %s\nApplied at: %s\nChecksum: %s",
		result.DBPath,
		result.SnapshotPath,
		result.ManifestPath,
		result.PreRestorePath,
		result.AppliedAtUTC,
		result.SnapshotSHA256,
	))
}
