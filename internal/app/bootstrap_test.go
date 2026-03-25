package appcore

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"dedup/internal/config"
)

func TestBootstrapDatabaseAutoIngestsPSGC(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "beneficiary.db")
	csvPath := writeBootstrapPSGCTestCSV(t, tempDir)

	bootstrap, err := BootstrapDatabase(ctx, config.Config{
		DBPath:      dbPath,
		PSGCCSVPath: csvPath,
	})
	if err != nil {
		t.Fatalf("bootstrap database: %v", err)
	}
	defer bootstrap.DB.Close()

	if bootstrap.PSGCReport == nil {
		t.Fatalf("expected PSGC report to be populated")
	}
	if bootstrap.PSGCReport.Skipped {
		t.Fatalf("expected PSGC ingest to run on first bootstrap: %+v", bootstrap.PSGCReport)
	}
	if bootstrap.PSGCReport.RowsRead != 1 {
		t.Fatalf("expected 1 PSGC row read, got %d", bootstrap.PSGCReport.RowsRead)
	}

	var barangayCount int
	if err := bootstrap.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM psgc_barangays;").Scan(&barangayCount); err != nil {
		t.Fatalf("count barangays: %v", err)
	}
	if barangayCount != 1 {
		t.Fatalf("expected 1 barangay row after bootstrap, got %d", barangayCount)
	}

	var metadataCount int
	if err := bootstrap.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM psgc_ingest_metadata;").Scan(&metadataCount); err != nil {
		t.Fatalf("count ingest metadata: %v", err)
	}
	if metadataCount != 1 {
		t.Fatalf("expected one ingest metadata row after bootstrap, got %d", metadataCount)
	}
}

func writeBootstrapPSGCTestCSV(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, "bootstrap-psgc.csv")
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write([]string{
		"barangay_code",
		"barangay_name",
		"urb_rur",
		"barangay_code2023",
		"barangay_name2023",
		"correspondence_code23",
		"citymun_code2023",
		"citymun_name2023",
		"prov_code2023",
		"prov_name2023",
		"region_code2023",
		"region_name2023",
		"barangay_code2024",
		"barangay_name2024",
		"correspondence_code24",
		"citymun_code2024",
		"citymun_name2024",
		"prov_code2024",
		"prov_name2024",
		"region_code2024",
		"region_name2024",
		"barangay_code2025",
		"correspondence_code25",
		"barangay_name2025",
		"citymun_code2025",
		"citymun_name2025",
		"prov_code2025",
		"prov_name2025",
		"region_code2025",
		"region_name2025",
		"region",
		"pop",
		"pop_incidence",
		"pop_int",
		"congressional_district",
	}); err != nil {
		t.Fatalf("write header: %v", err)
	}
	row := make([]string, 35)
	row[21] = "0105510016"
	row[23] = "PRIMICIAS"
	row[24] = "0105510000"
	row[25] = "BAUTISTA"
	row[26] = "0105500000"
	row[27] = "PANGASINAN"
	row[28] = "0100000000"
	row[29] = "REGION I (ILOCOS REGION)"
	if err := writer.Write(row); err != nil {
		t.Fatalf("write row: %v", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush csv: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	return path
}
