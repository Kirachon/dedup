package psgc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"dedup/internal/db"
)

func TestIngestCSVRejectsMalformedHeader(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	defer database.Close()

	csvPath := writeCSVWithHeader(t, []string{
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
		"congressional_district_x",
	}, nil)

	if _, err := IngestCSV(ctx, database, csvPath); err == nil {
		t.Fatalf("expected header validation error")
	}

	assertPSGCTablesEmpty(t, ctx, database)
}

func TestIngestCSVDeduplicatesAndPersistsPath(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	defer database.Close()

	rows := [][]string{
		fixtureRow("0105510016", "PRIMICIAS", "0105510000", "BAUTISTA", "0105500000", "PANGASINAN", "0100000000", "REGION I (ILOCOS REGION)", "137501001"),
		fixtureRow("0105510016", "PRIMICIAS", "0105510000", "BAUTISTA", "0105500000", "PANGASINAN", "0100000000", "REGION I (ILOCOS REGION)", "137501001"),
		fixtureRow("0105510017", "KETEGAN", "0105510000", "BAUTISTA", "0105500000", "PANGASINAN", "0100000000", "REGION I (ILOCOS REGION)", "137501002"),
	}
	csvPath := writeCSVWithHeader(t, expectedHeaders, rows)

	report, err := IngestCSV(ctx, database, csvPath)
	if err != nil {
		t.Fatalf("ingest csv: %v", err)
	}

	if report.RowsRead != 3 {
		t.Fatalf("expected 3 rows read, got %d", report.RowsRead)
	}
	if report.RegionsInserted != 1 || report.ProvincesInserted != 1 || report.CitiesInserted != 1 || report.BarangaysInserted != 2 {
		t.Fatalf("unexpected counts: %+v", report)
	}
	if report.RowsSkipped != 1 {
		t.Fatalf("expected 1 skipped duplicate row, got %d", report.RowsSkipped)
	}

	checksum := checksumOfFile(t, csvPath)
	if report.SourceChecksum != checksum {
		t.Fatalf("expected checksum %s, got %s", checksum, report.SourceChecksum)
	}

	var metadataCount int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM psgc_ingest_metadata;").Scan(&metadataCount); err != nil {
		t.Fatalf("count ingest metadata: %v", err)
	}
	if metadataCount != 1 {
		t.Fatalf("expected one ingest metadata row, got %d", metadataCount)
	}

	var sourceFileName, sourceChecksum string
	var rowsRead, regions, provinces, cities, barangays int
	if err := database.QueryRowContext(ctx, `
SELECT source_file_name, source_checksum, rows_read, rows_regions, rows_provinces, rows_cities, rows_barangays
FROM psgc_ingest_metadata
WHERE id = 1;
`).Scan(&sourceFileName, &sourceChecksum, &rowsRead, &regions, &provinces, &cities, &barangays); err != nil {
		t.Fatalf("read ingest metadata: %v", err)
	}
	if sourceFileName != filepath.Base(csvPath) {
		t.Fatalf("expected source file name %q, got %q", filepath.Base(csvPath), sourceFileName)
	}
	if sourceChecksum != checksum {
		t.Fatalf("expected metadata checksum %s, got %s", checksum, sourceChecksum)
	}
	if rowsRead != 3 || regions != 1 || provinces != 1 || cities != 1 || barangays != 2 {
		t.Fatalf("unexpected metadata counts: rows=%d regions=%d provinces=%d cities=%d barangays=%d", rowsRead, regions, provinces, cities, barangays)
	}

	barangayPath, err := GetBarangayPath(ctx, database, "0105510017")
	if err != nil {
		t.Fatalf("get barangay path: %v", err)
	}
	if barangayPath.Region.Code != "0100000000" || barangayPath.Region.Name != "REGION I (ILOCOS REGION)" {
		t.Fatalf("unexpected region path: %+v", barangayPath.Region)
	}
	if barangayPath.Province.Code != "0105500000" || barangayPath.Province.Name != "PANGASINAN" {
		t.Fatalf("unexpected province path: %+v", barangayPath.Province)
	}
	if barangayPath.City.Code != "0105510000" || barangayPath.City.Name != "BAUTISTA" {
		t.Fatalf("unexpected city path: %+v", barangayPath.City)
	}
	if barangayPath.Barangay.Code != "0105510017" || barangayPath.Barangay.Name != "KETEGAN" {
		t.Fatalf("unexpected barangay path: %+v", barangayPath.Barangay)
	}

	barangayList, err := ListBarangaysByCity(ctx, database, "0105510000")
	if err != nil {
		t.Fatalf("list barangays by city: %v", err)
	}
	if len(barangayList) != 2 {
		t.Fatalf("expected 2 barangays for city, got %d", len(barangayList))
	}
}

func TestIngestCSVSkipsIdenticalChecksum(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	defer database.Close()

	rows := [][]string{
		fixtureRow("0105510016", "PRIMICIAS", "0105510000", "BAUTISTA", "0105500000", "PANGASINAN", "0100000000", "REGION I (ILOCOS REGION)", "137501001"),
	}
	csvPath := writeCSVWithHeader(t, expectedHeaders, rows)

	first, err := IngestCSV(ctx, database, csvPath)
	if err != nil {
		t.Fatalf("first ingest csv: %v", err)
	}
	if first.Skipped {
		t.Fatalf("first ingest should not be skipped: %+v", first)
	}

	second, err := IngestCSV(ctx, database, csvPath)
	if err != nil {
		t.Fatalf("second ingest csv: %v", err)
	}
	if !second.Skipped {
		t.Fatalf("expected second ingest to be skipped, got %+v", second)
	}
	if second.RowsRead != first.RowsRead {
		t.Fatalf("expected skipped ingest to report %d rows read, got %d", first.RowsRead, second.RowsRead)
	}
	if second.RowsSkipped != first.RowsRead {
		t.Fatalf("expected skipped ingest to report %d skipped rows, got %d", first.RowsRead, second.RowsSkipped)
	}

	var metadataCount int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM psgc_ingest_metadata;").Scan(&metadataCount); err != nil {
		t.Fatalf("count ingest metadata: %v", err)
	}
	if metadataCount != 1 {
		t.Fatalf("expected one ingest metadata row, got %d", metadataCount)
	}
}

func TestIngestCSVRejectsParentMismatch(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	defer database.Close()

	rows := [][]string{
		fixtureRow("1000000001", "BARANGAY A", "1000100000", "CITY A", "1000200000", "PROVINCE A", "1000000000", "REGION A", "111111001"),
		fixtureRow("1000000002", "BARANGAY B", "1000100000", "CITY A", "9999999999", "PROVINCE B", "1000000000", "REGION A", "111111002"),
	}
	csvPath := writeCSVWithHeader(t, expectedHeaders, rows)

	if _, err := IngestCSV(ctx, database, csvPath); err == nil {
		t.Fatalf("expected parent-child inconsistency error")
	}

	assertPSGCTablesEmpty(t, ctx, database)
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	databasePath := filepath.Join(t.TempDir(), "psgc-test.db")
	database, err := db.OpenSQLite(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.MigrateUp(context.Background(), database); err != nil {
		database.Close()
		t.Fatalf("migrate up: %v", err)
	}

	return database
}

func writeCSVWithHeader(t *testing.T, header []string, rows [][]string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "fixture.csv")
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("flush csv: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	return path
}

func fixtureRow(barangayCode, barangayName, cityCode, cityName, provinceCode, provinceName, regionCode, regionName, correspondenceCode string) []string {
	row := make([]string, len(expectedHeaders))
	row[psgcColBarangayCode2025] = barangayCode
	row[psgcColBarangayName2025] = barangayName
	row[psgcColCityCode2025] = cityCode
	row[psgcColCityName2025] = cityName
	row[psgcColProvinceCode2025] = provinceCode
	row[psgcColProvinceName2025] = provinceName
	row[psgcColRegionCode2025] = regionCode
	row[psgcColRegionName2025] = regionName
	row[psgcColCorrespondence25] = correspondenceCode
	row[30] = "NCR"
	row[31] = "1"
	row[32] = "1.00%"
	row[33] = ""
	row[34] = "1st"
	return row
}

func checksumOfFile(t *testing.T, path string) string {
	t.Helper()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read csv fixture: %v", err)
	}
	sum := sha256.Sum256(contents)
	return hex.EncodeToString(sum[:])
}

func assertPSGCTablesEmpty(t *testing.T, ctx context.Context, database *sql.DB) {
	t.Helper()

	for _, query := range []string{
		"SELECT COUNT(*) FROM psgc_regions;",
		"SELECT COUNT(*) FROM psgc_provinces;",
		"SELECT COUNT(*) FROM psgc_cities;",
		"SELECT COUNT(*) FROM psgc_barangays;",
		"SELECT COUNT(*) FROM psgc_ingest_metadata;",
	} {
		var count int
		if err := database.QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("count query %q: %v", query, err)
		}
		if count != 0 {
			t.Fatalf("expected zero rows for %q, got %d", query, count)
		}
	}
}
