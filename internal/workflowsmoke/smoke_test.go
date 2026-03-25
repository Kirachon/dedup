package workflowsmoke

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appcore "dedup/internal/app"
	"dedup/internal/config"
	"dedup/internal/db"
	"dedup/internal/dedup"
	"dedup/internal/exporter"
	"dedup/internal/importer"
	"dedup/internal/jobs"
	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"
	appui "dedup/internal/ui"
)

const smokePSGCCSVFileName = "lib_geo_map_2025_202603251312.csv"
const smokeSettingKey = "workflow.smoke"

func TestWorkflowSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := mustRepoRoot(t)
	psgcPath := filepath.Join(repoRoot, smokePSGCCSVFileName)
	if _, err := os.Stat(psgcPath); err != nil {
		t.Fatalf("psgc csv missing at %s: %v", psgcPath, err)
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "workflow-smoke.db")

	bootstrap, err := appcore.BootstrapDatabase(ctx, config.Config{
		AppID:       "ph.lgu.beneficiary",
		WindowTitle: "Offline Beneficiary Tool (Workflow Smoke)",
		DBPath:      dbPath,
		PSGCCSVPath: psgcPath,
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	defer func() { _ = bootstrap.DB.Close() }()

	repo, err := repository.New(bootstrap.DB, bootstrap.Writer)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}

	beneficiaryClock := time.Date(2026, time.March, 25, 17, 0, 0, 0, time.UTC)
	importClock := beneficiaryClock.Add(45 * time.Minute)
	packageClock := beneficiaryClock.Add(75 * time.Minute)
	backupClock := beneficiaryClock.Add(2 * time.Hour)
	decisionClock := beneficiaryClock.Add(30 * time.Minute)

	beneficiarySvc, err := service.NewBeneficiaryService(
		bootstrap.DB,
		bootstrap.Writer,
		repo,
		service.WithClock(func() time.Time { return beneficiaryClock }),
	)
	if err != nil {
		t.Fatalf("beneficiary service: %v", err)
	}

	csvImporter, err := importer.NewImporter(
		repo,
		beneficiarySvc,
		importer.WithClock(func() time.Time { return importClock }),
		importer.WithCommitRowBudget(1),
	)
	if err != nil {
		t.Fatalf("csv importer: %v", err)
	}

	exporterSvc, err := exporter.NewExporter(repo)
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}

	dedupEngine := dedup.NewEngine()

	dedupDecisionSvc, err := service.NewDedupDecisionService(
		repo,
		service.WithDedupDecisionClock(func() time.Time { return decisionClock }),
	)
	if err != nil {
		t.Fatalf("dedup decision service: %v", err)
	}

	backupSvc, err := service.NewBackupService(
		bootstrap.DB,
		bootstrap.DBPath,
		bootstrap.Writer,
		service.WithBackupClock(func() time.Time { return backupClock }),
	)
	if err != nil {
		t.Fatalf("backup service: %v", err)
	}
	defer func() { _ = backupSvc.Close() }()

	jobManager := jobs.NewManager(bootstrap.DB, bootstrap.Writer)

	deps, err := appui.NewDependencies(
		config.Config{
			AppID:       "ph.lgu.beneficiary",
			WindowTitle: "Offline Beneficiary Tool (Workflow Smoke)",
			DBPath:      dbPath,
			PSGCCSVPath: psgcPath,
		},
		bootstrap.DBPath,
		bootstrap.PSGCReport,
		repo,
		beneficiarySvc,
		csvImporter,
		exporterSvc,
		dedupEngine,
		dedupDecisionSvc,
		backupSvc,
		jobManager,
	)
	if err != nil {
		t.Fatalf("ui dependencies: %v", err)
	}

	if shell := appui.BuildShell(&appui.Runtime{Dependencies: deps}); shell == nil {
		t.Fatalf("expected ui shell to build")
	}
	t.Log("ui shell built")

	location := mustSmokeLocation(t, ctx, repo)

	draft := smokeDraft{
		LastName:      "Reyes",
		FirstName:     "Ana",
		MiddleName:    "S",
		ExtensionName: "",
		Sex:           "F",
		BirthdateISO:  "1980-01-02",
		RegionCode:    location.RegionCode,
		RegionName:    location.RegionName,
		ProvinceCode:  location.ProvinceCode,
		ProvinceName:  location.ProvinceName,
		CityCode:      location.CityCode,
		CityName:      location.CityName,
		BarangayCode:  location.BarangayCode,
		BarangayName:  location.BarangayName,
		ContactNo:     "09170000001",
		BirthMonth:    int64Ptr(1),
		BirthDay:      int64Ptr(2),
		BirthYear:     int64Ptr(1980),
	}

	beforePrompt, err := beneficiarySvc.BuildDuplicatePrecheckPrompt(ctx, draft.toServiceDraft(), "")
	if err != nil {
		t.Fatalf("duplicate precheck before create: %v", err)
	}
	if beforePrompt.HasExactDuplicate {
		t.Fatalf("expected no exact duplicate before seed create")
	}

	firstBeneficiary, err := beneficiarySvc.CreateBeneficiary(ctx, draft.toServiceDraft(), service.CreateOptions{
		SourceType:      model.BeneficiarySourceLocal,
		RecordStatus:    model.RecordStatusActive,
		DedupStatus:     model.DedupStatusClear,
		SourceReference: "workflow-smoke-manual-1",
	})
	if err != nil {
		t.Fatalf("create first beneficiary: %v", err)
	}
	secondBeneficiary, err := beneficiarySvc.CreateBeneficiary(ctx, draft.toServiceDraft(), service.CreateOptions{
		SourceType:      model.BeneficiarySourceLocal,
		RecordStatus:    model.RecordStatusActive,
		DedupStatus:     model.DedupStatusClear,
		SourceReference: "workflow-smoke-manual-2",
	})
	if err != nil {
		t.Fatalf("create duplicate beneficiary: %v", err)
	}

	afterPrompt, err := beneficiarySvc.BuildDuplicatePrecheckPrompt(ctx, draft.toServiceDraft(), "")
	if err != nil {
		t.Fatalf("duplicate precheck after create: %v", err)
	}
	if !afterPrompt.HasExactDuplicate {
		t.Fatalf("expected exact duplicate after duplicate beneficiary creation")
	}
	t.Logf("duplicate precheck: %s", afterPrompt.Message)

	beneficiaries, err := listAllBeneficiaries(ctx, repo, false)
	if err != nil {
		t.Fatalf("list beneficiaries: %v", err)
	}
	dedupRunID := smokeID("dedup")
	dedupResult, err := dedupEngine.Run(dedup.RunRequest{
		RunID:          dedupRunID,
		InitiatedBy:    "workflow-smoke",
		Threshold:      90,
		IncludeDeleted: false,
	}, beneficiaries)
	if err != nil {
		t.Fatalf("run dedup: %v", err)
	}
	if len(dedupResult.Matches) == 0 {
		t.Fatalf("expected at least one dedup match")
	}
	if err := persistDedupRun(ctx, repo, dedupResult, decisionClock); err != nil {
		t.Fatalf("persist dedup run: %v", err)
	}
	t.Logf("dedup run: %s candidates=%d matches=%d", dedupRunID, dedupResult.TotalCandidates, len(dedupResult.Matches))

	matchID := fmt.Sprintf("%s-%05d", dedupRunID, 1)
	applyResult, err := dedupDecisionSvc.ApplyDecision(ctx, service.ApplyDedupDecisionRequest{
		MatchID:    matchID,
		Decision:   model.DedupDecisionDeleteASoft,
		ResolvedBy: "workflow-smoke",
		Notes:      "workflow smoke delete/reset exercise",
	})
	if err != nil {
		t.Fatalf("apply dedup decision: %v", err)
	}
	if applyResult.SoftDeletedInternalUUID == nil || *applyResult.SoftDeletedInternalUUID == "" {
		t.Fatalf("expected soft-deleted beneficiary uuid from dedup apply")
	}
	if applyResult.Decision.Decision != model.DedupDecisionDeleteASoft {
		t.Fatalf("unexpected applied decision: %s", applyResult.Decision.Decision)
	}

	resetResult, err := dedupDecisionSvc.ResetDecision(ctx, service.ResetDedupDecisionRequest{
		MatchID: matchID,
		ResetBy: "workflow-smoke",
		Notes:   "workflow smoke reset exercise",
	})
	if err != nil {
		t.Fatalf("reset dedup decision: %v", err)
	}
	if resetResult.RestoredInternalUUID == nil || *resetResult.RestoredInternalUUID == "" {
		t.Fatalf("expected restored beneficiary uuid from dedup reset")
	}
	restoredA, err := repo.GetBeneficiary(ctx, firstBeneficiary.InternalUUID)
	if err != nil {
		t.Fatalf("read beneficiary after reset: %v", err)
	}
	restoredB, err := repo.GetBeneficiary(ctx, secondBeneficiary.InternalUUID)
	if err != nil {
		t.Fatalf("read second beneficiary after reset: %v", err)
	}
	if restoredA.DedupStatus != model.DedupStatusPossibleDuplicate || restoredB.DedupStatus != model.DedupStatusPossibleDuplicate {
		t.Fatalf("expected both duplicate beneficiaries to return to POSSIBLE_DUPLICATE after reset")
	}

	csvPath := filepath.Join(tempDir, "workflow-smoke-import.csv")
	if err := writeBeneficiaryCSV(csvPath, []smokeCSVRow{
		{
			GeneratedID:   "SMOKE-CSV-0001",
			LastName:      "Santos",
			FirstName:     "Ben",
			MiddleName:    "T",
			ExtensionName: "",
			Sex:           "M",
			BirthdateISO:  "1982-03-04",
			RegionCode:    location.RegionCode,
			ProvinceCode:  location.ProvinceCode,
			CityCode:      location.CityCode,
			BarangayCode:  location.BarangayCode,
			ContactNo:     "09170000002",
		},
		{
			GeneratedID:   "SMOKE-CSV-0002",
			LastName:      "Dela Cruz",
			FirstName:     "Carla",
			MiddleName:    "B",
			ExtensionName: "",
			Sex:           "F",
			BirthdateISO:  "1985-05-06",
			RegionCode:    location.RegionCode,
			ProvinceCode:  location.ProvinceCode,
			CityCode:      location.CityCode,
			BarangayCode:  location.BarangayCode,
			ContactNo:     "09179990001",
		},
	}); err != nil {
		t.Fatalf("write csv fixture: %v", err)
	}

	csvPreview, err := csvImporter.Preview(ctx, importer.Source{
		Type:            model.ImportSourceCSV,
		Path:            csvPath,
		OperatorName:    "workflow-smoke",
		SourceReference: "workflow-smoke-csv",
	})
	if err != nil {
		t.Fatalf("preview csv import: %v", err)
	}
	csvCommit, err := csvImporter.Commit(ctx, csvPreview.PreviewToken, "workflow-smoke-csv-idem")
	if err != nil {
		t.Fatalf("commit csv import: %v", err)
	}
	if csvCommit.CheckpointToken == nil {
		t.Fatalf("expected checkpoint token from partial csv commit")
	}
	if csvCommit.Status != "PARTIAL" {
		t.Fatalf("expected partial csv status, got %s", csvCommit.Status)
	}
	resumeImporter, err := importer.NewImporter(
		repo,
		beneficiarySvc,
		importer.WithClock(func() time.Time { return importClock }),
	)
	if err != nil {
		t.Fatalf("resume importer: %v", err)
	}
	csvResume, err := resumeImporter.Resume(ctx, *csvCommit.CheckpointToken)
	if err != nil {
		t.Fatalf("resume csv import: %v", err)
	}
	if csvResume.Status != "RESUMED" {
		t.Fatalf("expected resumed csv status, got %s", csvResume.Status)
	}
	repeatCommit, err := resumeImporter.Commit(ctx, csvPreview.PreviewToken, "workflow-smoke-csv-idem")
	if err != nil {
		t.Fatalf("repeat csv commit: %v", err)
	}
	if repeatCommit.ImportID != csvResume.ImportID {
		t.Fatalf("expected idempotent csv commit to reuse import id")
	}

	pkgPath := filepath.Join(tempDir, "workflow-smoke-package.zip")
	if err := writeExchangePackage(pkgPath, []smokeCSVRow{
		{
			GeneratedID:   "SMOKE-PKG-0001",
			LastName:      "Lopez",
			FirstName:     "Dina",
			MiddleName:    "C",
			ExtensionName: "",
			Sex:           "F",
			BirthdateISO:  "1987-07-08",
			RegionCode:    location.RegionCode,
			ProvinceCode:  location.ProvinceCode,
			CityCode:      location.CityCode,
			BarangayCode:  location.BarangayCode,
			ContactNo:     "09178880001",
		},
	}); err != nil {
		t.Fatalf("write package fixture: %v", err)
	}

	pkgImporter, err := importer.NewImporter(
		repo,
		beneficiarySvc,
		importer.WithClock(func() time.Time { return packageClock }),
	)
	if err != nil {
		t.Fatalf("package importer: %v", err)
	}
	pkgPreview, err := pkgImporter.Preview(ctx, importer.Source{
		Type:            model.ImportSourceExchangePackage,
		Path:            pkgPath,
		OperatorName:    "workflow-smoke",
		SourceReference: "workflow-smoke-package",
	})
	if err != nil {
		t.Fatalf("preview package import: %v", err)
	}
	pkgCommit, err := pkgImporter.Commit(ctx, pkgPreview.PreviewToken, "workflow-smoke-package-idem")
	if err != nil {
		t.Fatalf("commit package import: %v", err)
	}
	if pkgCommit.Status != "SUCCEEDED" {
		t.Fatalf("expected succeeded package status, got %s", pkgCommit.Status)
	}

	exportPath := filepath.Join(tempDir, "workflow-smoke-export.csv")
	exportResult, err := exporterSvc.ExportCSV(ctx, exporter.Request{
		OutputPath:                  exportPath,
		IncludeUnresolvedDuplicates: true,
		OperatorName:                "workflow-smoke",
	})
	if err != nil {
		t.Fatalf("export csv: %v", err)
	}
	if err := verifyExportCSV(exportPath, exportResult.RowsExported); err != nil {
		t.Fatalf("verify export csv: %v", err)
	}

	if err := repo.UpsertSetting(ctx, &model.AppSetting{
		SettingKey:   smokeSettingKey,
		SettingValue: "before",
		UpdatedAt:    backupClock.Add(-15 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed backup setting: %v", err)
	}

	snapshotDir := filepath.Join(tempDir, "snapshots")
	snapshotResult, err := backupSvc.CreateSnapshot(ctx, service.SnapshotRequest{
		OutputDir:   snapshotDir,
		PerformedBy: "workflow-smoke",
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if err := repo.UpsertSetting(ctx, &model.AppSetting{
		SettingKey:   smokeSettingKey,
		SettingValue: "after",
		UpdatedAt:    backupClock.Add(-10 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("mutate setting before restore: %v", err)
	}

	validation, err := backupSvc.ValidateRestore(ctx, service.RestoreValidationRequest{
		SnapshotPath: snapshotResult.SnapshotPath,
		ManifestPath: snapshotResult.ManifestPath,
	})
	if err != nil {
		t.Fatalf("validate restore: %v", err)
	}
	if !validation.ApplyReady {
		t.Fatalf("expected backup restore to be apply-ready")
	}

	restoreResult, err := backupSvc.ApplyRestore(ctx, service.RestoreApplyRequest{
		SnapshotPath: snapshotResult.SnapshotPath,
		ManifestPath: snapshotResult.ManifestPath,
		Confirmation: validation.ExpectedConfirmation,
		PerformedBy:  "workflow-smoke",
	})
	if err != nil {
		t.Fatalf("apply restore: %v", err)
	}
	if !restoreResult.Restored {
		t.Fatalf("expected restore to report success")
	}

	freshDB, err := db.OpenSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen restored db: %v", err)
	}
	defer func() { _ = freshDB.Close() }()

	freshRepo, err := repository.New(freshDB, bootstrap.Writer)
	if err != nil {
		t.Fatalf("fresh repository after restore: %v", err)
	}
	restoredSetting, err := freshRepo.GetSetting(ctx, smokeSettingKey)
	if err != nil {
		t.Fatalf("read restored setting: %v", err)
	}
	if restoredSetting.SettingValue != "before" {
		t.Fatalf("expected restore to bring back setting value 'before', got %q", restoredSetting.SettingValue)
	}

	t.Logf("workflow smoke complete: dedup matches=%d csv rows=%d package rows=%d export rows=%d", len(dedupResult.Matches), csvResume.RowsInserted, pkgCommit.RowsInserted, exportResult.RowsExported)
}

type smokeLocation struct {
	RegionCode   string
	RegionName   string
	ProvinceCode string
	ProvinceName string
	CityCode     string
	CityName     string
	BarangayCode string
	BarangayName string
}

type smokeCSVRow struct {
	GeneratedID   string
	LastName      string
	FirstName     string
	MiddleName    string
	ExtensionName string
	Sex           string
	BirthdateISO  string
	RegionCode    string
	ProvinceCode  string
	CityCode      string
	BarangayCode  string
	ContactNo     string
}

type smokeDraft struct {
	LastName      string
	FirstName     string
	MiddleName    string
	ExtensionName string
	Sex           string
	BirthdateISO  string
	RegionCode    string
	RegionName    string
	ProvinceCode  string
	ProvinceName  string
	CityCode      string
	CityName      string
	BarangayCode  string
	BarangayName  string
	ContactNo     string
	BirthMonth    *int64
	BirthDay      *int64
	BirthYear     *int64
}

func (d smokeDraft) toServiceDraft() service.BeneficiaryDraft {
	return service.BeneficiaryDraft{
		LastName:      d.LastName,
		FirstName:     d.FirstName,
		MiddleName:    d.MiddleName,
		ExtensionName: d.ExtensionName,
		RegionCode:    d.RegionCode,
		RegionName:    d.RegionName,
		ProvinceCode:  d.ProvinceCode,
		ProvinceName:  d.ProvinceName,
		CityCode:      d.CityCode,
		CityName:      d.CityName,
		BarangayCode:  d.BarangayCode,
		BarangayName:  d.BarangayName,
		ContactNo:     d.ContactNo,
		BirthMonth:    d.BirthMonth,
		BirthDay:      d.BirthDay,
		BirthYear:     d.BirthYear,
		BirthdateISO:  d.BirthdateISO,
		Sex:           d.Sex,
	}
}

func mustRepoRoot(t *testing.T) string {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	current := cwd
	for {
		if fileExists(filepath.Join(current, "go.mod")) && fileExists(filepath.Join(current, smokePSGCCSVFileName)) {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			t.Fatalf("could not locate repo root from %s", cwd)
		}
		current = parent
	}
}

func mustSmokeLocation(t *testing.T, ctx context.Context, repo *repository.Repository) smokeLocation {
	t.Helper()

	regions, err := repo.ListRegions(ctx)
	if err != nil {
		t.Fatalf("list regions: %v", err)
	}
	if len(regions) == 0 {
		t.Fatalf("expected at least one PSGC region")
	}
	provinces, err := repo.ListProvincesByRegion(ctx, regions[0].RegionCode)
	if err != nil {
		t.Fatalf("list provinces: %v", err)
	}
	if len(provinces) == 0 {
		t.Fatalf("expected at least one province")
	}
	cities, err := repo.ListCitiesByProvince(ctx, provinces[0].ProvinceCode)
	if err != nil {
		t.Fatalf("list cities: %v", err)
	}
	if len(cities) == 0 {
		t.Fatalf("expected at least one city")
	}
	barangays, err := repo.ListBarangaysByCity(ctx, cities[0].CityCode)
	if err != nil {
		t.Fatalf("list barangays: %v", err)
	}
	if len(barangays) == 0 {
		t.Fatalf("expected at least one barangay")
	}

	return smokeLocation{
		RegionCode:   regions[0].RegionCode,
		RegionName:   regions[0].RegionName,
		ProvinceCode: provinces[0].ProvinceCode,
		ProvinceName: provinces[0].ProvinceName,
		CityCode:     cities[0].CityCode,
		CityName:     cities[0].CityName,
		BarangayCode: barangays[0].BarangayCode,
		BarangayName: barangays[0].BarangayName,
	}
}

func writeBeneficiaryCSV(path string, rows []smokeCSVRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create csv dir: %w", err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv file: %w", err)
	}
	defer func() { _ = file.Close() }()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"generated_id", "last_name", "first_name", "middle_name", "extension_name", "sex", "birthdate_iso", "region_code", "province_code", "city_code", "barangay_code", "contact_no",
	}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	for _, row := range rows {
		if err := writer.Write([]string{
			row.GeneratedID,
			row.LastName,
			row.FirstName,
			row.MiddleName,
			row.ExtensionName,
			row.Sex,
			row.BirthdateISO,
			row.RegionCode,
			row.ProvinceCode,
			row.CityCode,
			row.BarangayCode,
			row.ContactNo,
		}); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}

func writeExchangePackage(path string, rows []smokeCSVRow) error {
	csvBody, manifestBody, checksumBody, exportMetaBody, err := buildPackageBodies(rows)
	if err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create package: %w", err)
	}
	defer func() { _ = file.Close() }()

	zw := zip.NewWriter(file)
	writeZipEntry := func(name string, body []byte) error {
		entry, err := zw.Create(name)
		if err != nil {
			return err
		}
		if _, err := entry.Write(body); err != nil {
			return err
		}
		return nil
	}

	if err := writeZipEntry("manifest.json", manifestBody); err != nil {
		_ = zw.Close()
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := writeZipEntry("beneficiaries.csv", csvBody); err != nil {
		_ = zw.Close()
		return fmt.Errorf("write beneficiaries: %w", err)
	}
	if err := writeZipEntry("checksums.txt", checksumBody); err != nil {
		_ = zw.Close()
		return fmt.Errorf("write checksums: %w", err)
	}
	if err := writeZipEntry("export_meta.json", exportMetaBody); err != nil {
		_ = zw.Close()
		return fmt.Errorf("write export meta: %w", err)
	}
	if err := writeZipEntry("attachments/notes.txt", []byte("workflow smoke attachment")); err != nil {
		_ = zw.Close()
		return fmt.Errorf("write attachment: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close package zip: %w", err)
	}
	return nil
}

func buildPackageBodies(rows []smokeCSVRow) ([]byte, []byte, []byte, []byte, error) {
	csvBody, err := buildBeneficiaryCSVBytes(rows)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	manifestBody, err := json.Marshal(map[string]any{
		"spec_version":       "v1",
		"package_id":         smokeID("pkg"),
		"created_at_utc":     time.Now().UTC().Format(time.RFC3339Nano),
		"source_lgu_name":    "Workflow Smoke LGU",
		"source_system_name": "Offline Beneficiary Tool",
		"rows_declared":      len(rows),
		"checksum_algorithm": "sha256",
	})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("marshal manifest: %w", err)
	}

	exportMetaBody := []byte(`{"exported_by":"workflow-smoke","exported_at_utc":"2026-03-25T17:00:00Z"}`)
	checksumBody := buildChecksumsBytes(map[string]string{
		"manifest.json":     sha256Hex(manifestBody),
		"beneficiaries.csv": sha256Hex(csvBody),
		"export_meta.json":  sha256Hex(exportMetaBody),
	})

	return csvBody, manifestBody, checksumBody, exportMetaBody, nil
}

func buildBeneficiaryCSVBytes(rows []smokeCSVRow) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write([]string{
		"generated_id", "last_name", "first_name", "middle_name", "extension_name", "sex", "birthdate_iso", "region_code", "province_code", "city_code", "barangay_code", "contact_no",
	}); err != nil {
		return nil, fmt.Errorf("write csv header: %w", err)
	}
	for _, row := range rows {
		if err := writer.Write([]string{
			row.GeneratedID,
			row.LastName,
			row.FirstName,
			row.MiddleName,
			row.ExtensionName,
			row.Sex,
			row.BirthdateISO,
			row.RegionCode,
			row.ProvinceCode,
			row.CityCode,
			row.BarangayCode,
			row.ContactNo,
		}); err != nil {
			return nil, fmt.Errorf("write csv row: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("flush csv: %w", err)
	}
	return buf.Bytes(), nil
}

func buildChecksumsBytes(checksums map[string]string) []byte {
	lines := []string{
		fmt.Sprintf("%s  manifest.json", checksums["manifest.json"]),
		fmt.Sprintf("%s  beneficiaries.csv", checksums["beneficiaries.csv"]),
		fmt.Sprintf("%s  export_meta.json", checksums["export_meta.json"]),
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func verifyExportCSV(path string, expectedRows int) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read export csv: %w", err)
	}
	if len(body) < 3 || !bytes.Equal(body[:3], []byte{0xEF, 0xBB, 0xBF}) {
		return fmt.Errorf("export csv missing utf-8 bom")
	}

	reader := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(body), "\ufeff")))
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("parse export csv: %w", err)
	}
	if len(rows)-1 != expectedRows {
		return fmt.Errorf("export row count mismatch: got %d want %d", len(rows)-1, expectedRows)
	}
	return nil
}

func persistDedupRun(ctx context.Context, repo *repository.Repository, result dedup.RunResult, clock time.Time) error {
	run := &model.DedupRun{
		RunID:           result.RunID,
		StartedAt:       clock.Format(time.RFC3339Nano),
		CompletedAt:     stringPtr(clock.Add(1 * time.Minute).Format(time.RFC3339Nano)),
		Status:          "succeeded",
		TotalCandidates: result.TotalCandidates,
		TotalMatches:    len(result.Matches),
		Notes:           stringPtr("workflow smoke dedup run"),
	}
	if err := repo.CreateDedupRun(ctx, run); err != nil {
		return fmt.Errorf("create dedup run: %w", err)
	}

	for index, match := range result.Matches {
		record := &model.DedupMatch{
			MatchID:            fmt.Sprintf("%s-%05d", result.RunID, index+1),
			RunID:              result.RunID,
			RecordAUUID:        match.RecordAUUID,
			RecordBUUID:        match.RecordBUUID,
			PairKey:            match.PairKey,
			FirstNameScore:     match.FirstNameScore,
			MiddleNameScore:    match.MiddleNameScore,
			LastNameScore:      match.LastNameScore,
			ExtensionNameScore: match.ExtensionNameScore,
			TotalScore:         match.TotalScore,
			BirthdateCompare:   match.BirthdateCompare.NullableInt64Ptr(),
			BarangayCompare:    match.BarangayCompare.NullableInt64Ptr(),
			DecisionStatus:     "PENDING",
			CreatedAt:          clock.Format(time.RFC3339Nano),
		}
		if err := repo.CreateDedupMatch(ctx, record); err != nil {
			return fmt.Errorf("create dedup match: %w", err)
		}
	}
	return nil
}

func listAllBeneficiaries(ctx context.Context, repo *repository.Repository, includeDeleted bool) ([]model.Beneficiary, error) {
	items := make([]model.Beneficiary, 0)
	offset := 0
	for {
		page, err := repo.ListBeneficiaries(ctx, repository.BeneficiaryListQuery{
			IncludeDeleted: includeDeleted,
			Limit:          500,
			Offset:         offset,
		})
		if err != nil {
			return nil, err
		}
		if len(page.Items) == 0 {
			break
		}
		items = append(items, page.Items...)
		offset += len(page.Items)
		if offset >= page.Total {
			break
		}
	}
	return items, nil
}

func smokeID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

func stringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
