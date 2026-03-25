package importer

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dedup/internal/db"
	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"
)

func TestImporterPreviewCommitResumeCSV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixed := time.Date(2026, time.March, 25, 17, 0, 0, 0, time.UTC)
	imp, repo, creator, handle, cleanup := newImporterTestFixture(t, WithClock(func() time.Time { return fixed }), WithCommitRowBudget(1))
	defer cleanup()

	seedImporterPSGC(t, handle.DB)
	csvPath := writeBeneficiaryCSVFixture(t, []beneficiaryCSVRow{
		{GeneratedID: "G-000001", LastName: "Reyes", FirstName: "Ana", MiddleName: "S", ExtensionName: "", Sex: "F", BirthdateISO: "1980-01-02", RegionCode: "01", ProvinceCode: "0101", CityCode: "010101", BarangayCode: "010101001", ContactNo: "09170000001"},
		{GeneratedID: "G-000002", LastName: "Santos", FirstName: "Ben", MiddleName: "T", ExtensionName: "", Sex: "M", BirthdateISO: "1979-03-04", RegionCode: "01", ProvinceCode: "0101", CityCode: "010101", BarangayCode: "010101001", ContactNo: "09170000002"},
	})

	preview, err := imp.Preview(ctx, Source{
		Type:            model.ImportSourceCSV,
		Path:            csvPath,
		OperatorName:    "operator-a",
		SourceReference: "csv-fixture",
	})
	if err != nil {
		t.Fatalf("preview csv: %v", err)
	}
	if preview.PreviewToken == "" {
		t.Fatalf("expected preview token")
	}
	if !preview.HeaderValidationPassed {
		t.Fatalf("expected header validation to pass")
	}
	if preview.RowCountTotal != 2 || preview.RowCountValid != 2 || preview.RowCountInvalid != 0 {
		t.Fatalf("unexpected preview counts: %+v", preview)
	}

	firstCommit, err := imp.Commit(ctx, preview.PreviewToken, "idem-csv-1")
	if err != nil {
		t.Fatalf("commit csv: %v", err)
	}
	if firstCommit.Status != importStatusPartial {
		t.Fatalf("expected partial status after budgeted commit, got %s", firstCommit.Status)
	}
	if firstCommit.CheckpointToken == nil || *firstCommit.CheckpointToken == "" {
		t.Fatalf("expected checkpoint token from partial commit")
	}
	if firstCommit.RowsInserted != 1 || firstCommit.RowsRead != 1 {
		t.Fatalf("unexpected partial commit counts: %+v", firstCommit)
	}

	resumer, err := NewImporter(repo, creator, WithClock(func() time.Time { return fixed.Add(5 * time.Minute) }))
	if err != nil {
		t.Fatalf("new resumer: %v", err)
	}

	resumed, err := resumer.Resume(ctx, *firstCommit.CheckpointToken)
	if err != nil {
		t.Fatalf("resume csv: %v", err)
	}
	if resumed.Status != importStatusResumed {
		t.Fatalf("expected resumed status, got %s", resumed.Status)
	}
	if resumed.RowsRead != 2 || resumed.RowsInserted != 2 || resumed.RowsSkipped != 0 || resumed.RowsFailed != 0 {
		t.Fatalf("unexpected resumed counts: %+v", resumed)
	}
	if resumed.CompletedAtUTC == nil || *resumed.CompletedAtUTC == "" {
		t.Fatalf("expected completed_at on resumed import")
	}

	repeat, err := resumer.Commit(ctx, preview.PreviewToken, "idem-csv-1")
	if err != nil {
		t.Fatalf("repeat commit csv: %v", err)
	}
	if repeat.ImportID != resumed.ImportID {
		t.Fatalf("expected idempotent commit to reuse import id: got=%s want=%s", repeat.ImportID, resumed.ImportID)
	}
	if repeat.RowsInserted != 2 || repeat.Status != importStatusResumed {
		t.Fatalf("unexpected repeat commit result: %+v", repeat)
	}

	page, err := repo.ListBeneficiaries(ctx, repository.BeneficiaryListQuery{Limit: 10})
	if err != nil {
		t.Fatalf("list beneficiaries: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("expected 2 beneficiaries, got %d", len(page.Items))
	}
}

func TestImporterPreviewAndCommitExchangePackage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixed := time.Date(2026, time.March, 25, 17, 30, 0, 0, time.UTC)
	imp, repo, _, handle, cleanup := newImporterTestFixture(t, WithClock(func() time.Time { return fixed }))
	defer cleanup()

	seedImporterPSGC(t, handle.DB)
	packagePath := writeExchangePackageFixture(t, []beneficiaryCSVRow{
		{GeneratedID: "G-000101", LastName: "Dela Cruz", FirstName: "Carla", MiddleName: "B", ExtensionName: "", Sex: "F", BirthdateISO: "1985-05-06", RegionCode: "01", ProvinceCode: "0101", CityCode: "010101", BarangayCode: "010101001", ContactNo: "09179990001"},
	})

	preview, err := imp.Preview(ctx, Source{
		Type:            model.ImportSourceExchangePackage,
		Path:            packagePath,
		OperatorName:    "operator-b",
		SourceReference: "package-fixture",
	})
	if err != nil {
		t.Fatalf("preview package: %v", err)
	}
	if preview.SourceType != model.ImportSourceExchangePackage {
		t.Fatalf("unexpected preview source type: %s", preview.SourceType)
	}
	if preview.RowCountTotal != 1 || preview.RowCountValid != 1 || preview.RowCountInvalid != 0 {
		t.Fatalf("unexpected package preview counts: %+v", preview)
	}
	if preview.SourceHash == "" {
		t.Fatalf("expected package hash")
	}

	result, err := imp.Commit(ctx, preview.PreviewToken, "idem-pkg-1")
	if err != nil {
		t.Fatalf("commit package: %v", err)
	}
	if result.Status != importStatusSucceeded {
		t.Fatalf("expected succeeded status, got %s", result.Status)
	}
	if result.RowsInserted != 1 || result.RowsFailed != 0 || result.RowsSkipped != 0 || result.RowsRead != 1 {
		t.Fatalf("unexpected package commit counts: %+v", result)
	}
	if result.CompletedAtUTC == nil || *result.CompletedAtUTC == "" {
		t.Fatalf("expected completed_at on package import")
	}

	page, err := repo.ListBeneficiaries(ctx, repository.BeneficiaryListQuery{Limit: 10})
	if err != nil {
		t.Fatalf("list beneficiaries after package commit: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("expected 1 beneficiary from package import, got %d", len(page.Items))
	}
}

func newImporterTestFixture(t *testing.T, opts ...Option) (*Importer, *repository.Repository, *service.BeneficiaryService, *db.Handle, func()) {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "importer.db")
	handle, err := db.OpenAndMigrate(ctx, dbPath)
	if err != nil {
		t.Fatalf("open and migrate importer db: %v", err)
	}

	repo, err := repository.New(handle.DB, handle.Writer)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new repository: %v", err)
	}

	creator, err := service.NewBeneficiaryService(handle.DB, handle.Writer, repo)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new beneficiary service: %v", err)
	}

	imp, err := NewImporter(repo, creator, opts...)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new importer: %v", err)
	}

	cleanup := func() {
		_ = handle.DB.Close()
	}

	return imp, repo, creator, handle, cleanup
}

type beneficiaryCSVRow struct {
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

func seedImporterPSGC(t *testing.T, database *sql.DB) {
	t.Helper()

	ctx := context.Background()
	statements := []string{
		`INSERT INTO psgc_regions (region_code, region_name) VALUES ('01', 'Region One');`,
		`INSERT INTO psgc_provinces (province_code, region_code, province_name) VALUES ('0101', '01', 'Province One');`,
		`INSERT INTO psgc_cities (city_code, region_code, province_code, city_name, city_type) VALUES ('010101', '01', '0101', 'City One', 'Component City');`,
		`INSERT INTO psgc_barangays (barangay_code, region_code, province_code, city_code, barangay_name) VALUES ('010101001', '01', '0101', '010101', 'Barangay One');`,
	}
	for _, statement := range statements {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed psgc statement failed: %v", err)
		}
	}
}

func writeBeneficiaryCSVFixture(t *testing.T, rows []beneficiaryCSVRow) string {
	t.Helper()

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(requiredBeneficiaryHeaders); err != nil {
		t.Fatalf("write header: %v", err)
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
			t.Fatalf("write csv row: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush csv writer: %v", err)
	}

	path := filepath.Join(t.TempDir(), "beneficiaries.csv")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write csv fixture: %v", err)
	}
	return path
}

func writeExchangePackageFixture(t *testing.T, rows []beneficiaryCSVRow) string {
	t.Helper()

	csvBody := buildBeneficiaryCSVBytes(t, rows)
	manifest := exchangePackageManifest{
		SpecVersion:       "v1",
		PackageID:         "pkg-" + strings.ReplaceAll(uuidLike(t), "-", ""),
		CreatedAtUTC:      time.Now().UTC().Format(time.RFC3339Nano),
		SourceLGUName:     "Test LGU",
		SourceSystemName:  "Offline Beneficiary Tool",
		RowsDeclared:      len(rows),
		ChecksumAlgorithm: "sha256",
	}
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	exportMetaBody := []byte(`{"exported_by":"test","exported_at_utc":"2026-03-25T17:00:00Z"}`)
	checksums := map[string]string{
		packageManifestFileName:   sha256Hex(manifestBody),
		packageBeneficiariesName:  sha256Hex(csvBody),
		packageExportMetaFileName: sha256Hex(exportMetaBody),
	}
	checksumBody := buildChecksumsBytes(checksums)

	path := filepath.Join(t.TempDir(), "beneficiaries-package.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create package fixture: %v", err)
	}
	zw := zip.NewWriter(file)
	writeZipEntry := func(name string, body []byte) {
		entry, createErr := zw.Create(name)
		if createErr != nil {
			t.Fatalf("create zip entry %s: %v", name, createErr)
		}
		if _, createErr := entry.Write(body); createErr != nil {
			t.Fatalf("write zip entry %s: %v", name, createErr)
		}
	}
	writeZipEntry(packageManifestFileName, manifestBody)
	writeZipEntry(packageBeneficiariesName, csvBody)
	writeZipEntry(packageChecksumsFileName, checksumBody)
	writeZipEntry(packageExportMetaFileName, exportMetaBody)
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close package file: %v", err)
	}
	return path
}

func buildBeneficiaryCSVBytes(t *testing.T, rows []beneficiaryCSVRow) []byte {
	t.Helper()

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(requiredBeneficiaryHeaders); err != nil {
		t.Fatalf("write header: %v", err)
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
			t.Fatalf("write csv row: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush csv writer: %v", err)
	}
	return buf.Bytes()
}

func buildChecksumsBytes(checksums map[string]string) []byte {
	names := []string{
		packageManifestFileName,
		packageBeneficiariesName,
		packageExportMetaFileName,
	}
	var lines []string
	for _, name := range names {
		lines = append(lines, fmt.Sprintf("%s  %s", checksums[name], name))
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func uuidLike(t *testing.T) string {
	t.Helper()
	return strings.ReplaceAll(t.Name()+"-"+time.Now().UTC().Format("150405.000000000"), " ", "-")
}
