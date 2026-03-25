package ui

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dedup/internal/model"
	"dedup/internal/service"
)

func TestImportScreenNormalizeSourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  model.ImportSource
	}{
		{name: "csv default", input: "csv", want: model.ImportSourceCSV},
		{name: "exchange package", input: "EXCHANGE_PACKAGE", want: model.ImportSourceExchangePackage},
		{name: "unknown falls back to csv", input: "other", want: model.ImportSourceCSV},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := importScreenNormalizeSourceType(tc.input)
			if got != tc.want {
				t.Fatalf("importScreenNormalizeSourceType(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestExportScreenDefaultOutputPath(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 25, 12, 34, 56, 0, time.UTC)
	dbPath := filepath.Join("D:", "data", "beneficiary.db")

	got := exportScreenDefaultOutputPath(dbPath, now)
	wantSuffix := filepath.Join("exports", "beneficiaries-20260325-123456.csv")
	if !strings.HasSuffix(got, wantSuffix) {
		t.Fatalf("unexpected export path suffix: got %q want suffix %q", got, wantSuffix)
	}
}

func TestBackupScreenDescribeValidationResult(t *testing.T) {
	t.Parallel()

	result := &service.RestoreValidationResult{
		Valid:                true,
		ApplyReady:           false,
		SnapshotPath:         `D:\backups\snapshot.db`,
		ManifestPath:         `D:\backups\snapshot.db.manifest.json`,
		ManifestPresent:      true,
		ManifestMatches:      true,
		SnapshotSHA256:       "abc123",
		SnapshotSizeBytes:    2048,
		ExpectedConfirmation: "RESTORE beneficiary.db",
		BlockingJobs: []service.BlockingJob{
			{JobID: "job-1"},
		},
	}

	text := backupScreenDescribeValidationResult(result)
	for _, token := range []string{"Apply ready: false", "Blocking jobs: 1", "RESTORE beneficiary.db"} {
		if !strings.Contains(text, token) {
			t.Fatalf("expected %q in validation description, got %q", token, text)
		}
	}
}
