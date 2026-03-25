package exporter

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dedup/internal/db"
	"dedup/internal/model"
	"dedup/internal/repository"
)

func TestExporterExportCSVDefaultPolicyAndSafety(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixed := time.Date(2026, time.March, 25, 18, 0, 0, 0, time.UTC)
	exp, repo, cleanup := newExporterTestFixture(t, WithClock(func() time.Time { return fixed }), WithPageSize(2))
	defer cleanup()

	if err := repo.CreateBeneficiary(ctx, beneficiaryFixture(beneficiaryParams{
		InternalUUID: "uuid-active-clear",
		GeneratedID:  "G-000001",
		LastName:     "Alpha",
		FirstName:    "Anne",
		RecordStatus: model.RecordStatusActive,
		DedupStatus:  model.DedupStatusClear,
	})); err != nil {
		t.Fatalf("seed active clear: %v", err)
	}
	if err := repo.CreateBeneficiary(ctx, beneficiaryFixture(beneficiaryParams{
		InternalUUID: "uuid-retained-resolved",
		GeneratedID:  "G-000002",
		LastName:     "Bravo",
		FirstName:    "Ben",
		RecordStatus: model.RecordStatusRetained,
		DedupStatus:  model.DedupStatusResolved,
	})); err != nil {
		t.Fatalf("seed retained resolved: %v", err)
	}
	if err := repo.CreateBeneficiary(ctx, beneficiaryFixture(beneficiaryParams{
		InternalUUID: "uuid-active-unresolved",
		GeneratedID:  "G-000003",
		LastName:     "Charlie",
		FirstName:    "Cal",
		RecordStatus: model.RecordStatusActive,
		DedupStatus:  model.DedupStatusPossibleDuplicate,
	})); err != nil {
		t.Fatalf("seed active unresolved duplicate: %v", err)
	}
	if err := repo.CreateBeneficiary(ctx, beneficiaryFixture(beneficiaryParams{
		InternalUUID: "uuid-deleted",
		GeneratedID:  "G-000004",
		LastName:     "Delta",
		FirstName:    "Dan",
		RecordStatus: model.RecordStatusDeleted,
		DedupStatus:  model.DedupStatusClear,
		DeletedAt:    strPtr("2026-03-25T18:00:00Z"),
	})); err != nil {
		t.Fatalf("seed deleted beneficiary: %v", err)
	}
	if err := repo.CreateBeneficiary(ctx, beneficiaryFixture(beneficiaryParams{
		InternalUUID:  "uuid-injection",
		GeneratedID:   "G-000005",
		LastName:      "=HYPERLINK(\"http://malicious\")",
		FirstName:     "+SUM(1,2)",
		MiddleName:    strPtr("-10+20"),
		ExtensionName: strPtr("@risk"),
		BarangayName:  "Barangay \"Quoted\", Zone 1",
		RecordStatus:  model.RecordStatusActive,
		DedupStatus:   model.DedupStatusClear,
	})); err != nil {
		t.Fatalf("seed injection beneficiary: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "exports", "beneficiaries.csv")
	result, err := exp.ExportCSV(ctx, Request{
		OutputPath:   outputPath,
		OperatorName: "operator-a",
	})
	if err != nil {
		t.Fatalf("export csv: %v", err)
	}
	if result.RowsExported != 3 {
		t.Fatalf("expected 3 exported rows, got %d", result.RowsExported)
	}

	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read exported csv: %v", err)
	}
	if !bytes.HasPrefix(raw, utf8BOM) {
		t.Fatalf("expected exported csv to start with UTF-8 BOM")
	}
	if !strings.Contains(string(raw), "\"Barangay \"\"Quoted\"\", Zone 1\"") {
		t.Fatalf("expected quoted barangay value in raw csv output")
	}

	rows, err := parseCSVAfterBOM(raw)
	if err != nil {
		t.Fatalf("parse exported csv: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 total csv rows (header + 3 items), got %d", len(rows))
	}
	if got, want := rows[0], beneficiaryExportHeaders; !equalStringSlices(got, want) {
		t.Fatalf("unexpected export header: got=%v want=%v", got, want)
	}

	exportedByID := mapRowsByID(rows[1:])
	if _, ok := exportedByID["G-000001"]; !ok {
		t.Fatalf("expected ACTIVE final record in export")
	}
	if _, ok := exportedByID["G-000002"]; !ok {
		t.Fatalf("expected RETAINED final record in export")
	}
	if _, ok := exportedByID["G-000003"]; ok {
		t.Fatalf("did not expect unresolved duplicate to be exported by default")
	}
	if _, ok := exportedByID["G-000004"]; ok {
		t.Fatalf("did not expect DELETED record in export")
	}

	injectionRow, ok := exportedByID["G-000005"]
	if !ok {
		t.Fatalf("expected injection fixture row in export")
	}
	if injectionRow[1] != "'=HYPERLINK(\"http://malicious\")" {
		t.Fatalf("expected formula-safe last_name, got %q", injectionRow[1])
	}
	if injectionRow[2] != "'+SUM(1,2)" {
		t.Fatalf("expected formula-safe first_name, got %q", injectionRow[2])
	}
	if injectionRow[3] != "'-10+20" {
		t.Fatalf("expected formula-safe middle_name, got %q", injectionRow[3])
	}
	if injectionRow[4] != "'@risk" {
		t.Fatalf("expected formula-safe extension_name, got %q", injectionRow[4])
	}
	if injectionRow[8] != "Barangay \"Quoted\", Zone 1" {
		t.Fatalf("unexpected barangay cell after parsing: %q", injectionRow[8])
	}

	logs, err := repo.ListExportLogs(ctx, repository.ExportLogListQuery{
		ExportType: exportTypeBeneficiaries,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("list export logs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected one export log entry, got %d", len(logs))
	}
	if logs[0].RowsExported != 3 {
		t.Fatalf("unexpected rows_exported in log: %+v", logs[0])
	}
	if logs[0].PerformedBy == nil || *logs[0].PerformedBy != "operator-a" {
		t.Fatalf("unexpected performed_by in log: %+v", logs[0])
	}
	if logs[0].FileName != "beneficiaries.csv" {
		t.Fatalf("unexpected file name in export log: %s", logs[0].FileName)
	}
}

func TestExporterExportCSVAllowsUnresolvedDuplicatesWhenRequested(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exp, repo, cleanup := newExporterTestFixture(t)
	defer cleanup()

	if err := repo.CreateBeneficiary(ctx, beneficiaryFixture(beneficiaryParams{
		InternalUUID: "uuid-unresolved",
		GeneratedID:  "G-100001",
		LastName:     "Echo",
		FirstName:    "Elle",
		RecordStatus: model.RecordStatusActive,
		DedupStatus:  model.DedupStatusPossibleDuplicate,
	})); err != nil {
		t.Fatalf("seed unresolved duplicate: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "beneficiaries.csv")
	result, err := exp.ExportCSV(ctx, Request{
		OutputPath:                  outputPath,
		IncludeUnresolvedDuplicates: true,
	})
	if err != nil {
		t.Fatalf("export csv with unresolved duplicates enabled: %v", err)
	}
	if result.RowsExported != 1 {
		t.Fatalf("expected unresolved duplicate to be exported when enabled, got %d rows", result.RowsExported)
	}

	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read exported csv: %v", err)
	}
	rows, err := parseCSVAfterBOM(raw)
	if err != nil {
		t.Fatalf("parse exported csv: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected header + one exported row, got %d", len(rows))
	}
	if rows[1][0] != "G-100001" {
		t.Fatalf("unexpected exported generated_id: %q", rows[1][0])
	}
}

func TestSanitizeCSVCellNeutralizesFormulaPrefixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "equals", input: "=1+1", want: "'=1+1"},
		{name: "plus", input: "+cmd", want: "'+cmd"},
		{name: "minus", input: "-10", want: "'-10"},
		{name: "at", input: "@calc", want: "'@calc"},
		{name: "leading whitespace", input: " \t=SUM(A1:A2)", want: "' \t=SUM(A1:A2)"},
		{name: "safe plain text", input: "normal", want: "normal"},
		{name: "already escaped", input: "'=text", want: "'=text"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeCSVCell(tc.input); got != tc.want {
				t.Fatalf("sanitizeCSVCell(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func newExporterTestFixture(t *testing.T, opts ...Option) (*Exporter, *repository.Repository, func()) {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exporter.db")
	handle, err := db.OpenAndMigrate(ctx, dbPath)
	if err != nil {
		t.Fatalf("open and migrate exporter db: %v", err)
	}

	repo, err := repository.New(handle.DB, handle.Writer)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new repository: %v", err)
	}

	exp, err := NewExporter(repo, opts...)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new exporter: %v", err)
	}

	return exp, repo, func() {
		_ = handle.DB.Close()
	}
}

type beneficiaryParams struct {
	InternalUUID  string
	GeneratedID   string
	LastName      string
	FirstName     string
	MiddleName    *string
	ExtensionName *string
	BarangayName  string
	RecordStatus  model.RecordStatus
	DedupStatus   model.DedupStatus
	DeletedAt     *string
}

func beneficiaryFixture(p beneficiaryParams) *model.Beneficiary {
	middle := p.MiddleName
	if middle == nil {
		middle = strPtr("M")
	}
	extension := p.ExtensionName
	if extension == nil {
		extension = strPtr("")
	}
	barangayName := p.BarangayName
	if strings.TrimSpace(barangayName) == "" {
		barangayName = "Barangay One"
	}

	createdAt := "2026-03-25T17:00:00Z"
	updatedAt := createdAt
	return &model.Beneficiary{
		InternalUUID:      p.InternalUUID,
		GeneratedID:       p.GeneratedID,
		LastName:          p.LastName,
		FirstName:         p.FirstName,
		MiddleName:        middle,
		ExtensionName:     extension,
		NormLastName:      strings.ToUpper(strings.TrimSpace(p.LastName)),
		NormFirstName:     strings.ToUpper(strings.TrimSpace(p.FirstName)),
		NormMiddleName:    strPtr(strings.ToUpper(strings.TrimSpace(*middle))),
		NormExtensionName: strPtr(strings.ToUpper(strings.TrimSpace(*extension))),
		RegionCode:        "01",
		RegionName:        "Region One",
		ProvinceCode:      "0101",
		ProvinceName:      "Province One",
		CityCode:          "010101",
		CityName:          "City One",
		BarangayCode:      "010101001",
		BarangayName:      barangayName,
		ContactNo:         strPtr("09171234567"),
		ContactNoNorm:     strPtr("09171234567"),
		BirthMonth:        int64Ptr(3),
		BirthDay:          int64Ptr(25),
		BirthYear:         int64Ptr(1990),
		BirthdateISO:      strPtr("1990-03-25"),
		Sex:               "F",
		RecordStatus:      p.RecordStatus,
		DedupStatus:       p.DedupStatus,
		SourceType:        model.BeneficiarySourceLocal,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
		DeletedAt:         p.DeletedAt,
	}
}

func parseCSVAfterBOM(content []byte) ([][]string, error) {
	withoutBOM := bytes.TrimPrefix(content, utf8BOM)
	reader := csv.NewReader(bytes.NewReader(withoutBOM))
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func mapRowsByID(rows [][]string) map[string][]string {
	out := make(map[string][]string, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		out[row[0]] = row
	}
	return out
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func strPtr(value string) *string {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}
