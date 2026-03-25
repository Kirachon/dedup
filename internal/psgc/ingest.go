package psgc

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"database/sql"
)

const (
	psgcHeaderColumns = 35

	psgcColBarangayCode2025 = 21
	psgcColCorrespondence25 = 22
	psgcColBarangayName2025 = 23
	psgcColCityCode2025     = 24
	psgcColCityName2025     = 25
	psgcColProvinceCode2025 = 26
	psgcColProvinceName2025 = 27
	psgcColRegionCode2025   = 28
	psgcColRegionName2025   = 29
)

var expectedHeaders = []string{
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
}

// Report summarizes a PSGC ingest run.
type Report struct {
	SourceFileName    string
	SourceChecksum    string
	RowsRead          int
	RegionsInserted   int
	ProvincesInserted int
	CitiesInserted    int
	BarangaysInserted int
	RowsSkipped       int
	Skipped           bool
	CompletedAtUTC    time.Time
}

type sourceRow struct {
	RegionCode   string
	RegionName   string
	ProvinceCode string
	ProvinceName string
	CityCode     string
	CityName     string
	BarangayCode string
	BarangayName string
}

type regionEntry struct {
	Code string
	Name string
}

type provinceEntry struct {
	Code       string
	RegionCode string
	Name       string
}

type cityEntry struct {
	Code         string
	RegionCode   string
	ProvinceCode string
	Name         string
	Type         string
}

type barangayEntry struct {
	Code         string
	RegionCode   string
	ProvinceCode string
	CityCode     string
	Name         string
}

type ingestCounts struct {
	regions   int
	provinces int
	cities    int
	barangays int
}

// IngestCSV streams the PSGC CSV into the canonical SQLite tables.
func IngestCSV(ctx context.Context, database *sql.DB, csvPath string) (Report, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if database == nil {
		return Report{}, fmt.Errorf("db is nil")
	}
	if strings.TrimSpace(csvPath) == "" {
		return Report{}, fmt.Errorf("csv path is required")
	}

	file, err := os.Open(csvPath)
	if err != nil {
		return Report{}, fmt.Errorf("open csv: %w", err)
	}
	defer file.Close()

	checksum, err := checksumFile(file)
	if err != nil {
		return Report{}, fmt.Errorf("checksum csv: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Report{}, fmt.Errorf("rewind csv: %w", err)
	}

	existing, err := loadIngestMetadata(ctx, database)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Report{}, err
	}
	if err == nil && existing.SourceChecksum == checksum {
		return Report{
			SourceFileName: filepath.Base(csvPath),
			SourceChecksum: checksum,
			RowsRead:       existing.RowsRead,
			RowsSkipped:    existing.RowsRead,
			Skipped:        true,
			CompletedAtUTC: time.Now().UTC(),
		}, nil
	}
	if err == nil && existing.SourceChecksum != checksum {
		return Report{}, fmt.Errorf(
			"psgc source drift detected: stored checksum %s does not match incoming %s",
			existing.SourceChecksum,
			checksum,
		)
	}

	reader := csv.NewReader(file)
	reader.ReuseRecord = false
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return Report{}, fmt.Errorf("read csv header: %w", err)
	}
	if err := validateHeader(header); err != nil {
		return Report{}, err
	}
	reader.FieldsPerRecord = len(expectedHeaders)

	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return Report{}, fmt.Errorf("begin ingest tx: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	regions := map[string]regionEntry{}
	provinces := map[string]provinceEntry{}
	cities := map[string]cityEntry{}
	barangays := map[string]barangayEntry{}
	counts := ingestCounts{}
	rowsRead := 0

	for {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Report{}, fmt.Errorf("read csv row %d: %w", rowsRead+2, err)
		}
		rowsRead++

		row, err := parseSourceRow(record)
		if err != nil {
			return Report{}, fmt.Errorf("parse csv row %d: %w", rowsRead+1, err)
		}

		if inserted, err := ensureRegion(ctx, tx, regions, regionEntry{
			Code: row.RegionCode,
			Name: row.RegionName,
		}); err != nil {
			return Report{}, fmt.Errorf("ingest region at row %d: %w", rowsRead+1, err)
		} else if inserted {
			counts.regions++
		}

		if inserted, err := ensureProvince(ctx, tx, regions, provinces, provinceEntry{
			Code:       row.ProvinceCode,
			RegionCode: row.RegionCode,
			Name:       row.ProvinceName,
		}); err != nil {
			return Report{}, fmt.Errorf("ingest province at row %d: %w", rowsRead+1, err)
		} else if inserted {
			counts.provinces++
		}

		if inserted, err := ensureCity(ctx, tx, regions, provinces, cities, cityEntry{
			Code:         row.CityCode,
			RegionCode:   row.RegionCode,
			ProvinceCode: row.ProvinceCode,
			Name:         row.CityName,
			Type:         "",
		}); err != nil {
			return Report{}, fmt.Errorf("ingest city at row %d: %w", rowsRead+1, err)
		} else if inserted {
			counts.cities++
		}

		if inserted, err := ensureBarangay(ctx, tx, regions, provinces, cities, barangays, barangayEntry{
			Code:         row.BarangayCode,
			RegionCode:   row.RegionCode,
			ProvinceCode: row.ProvinceCode,
			CityCode:     row.CityCode,
			Name:         row.BarangayName,
		}); err != nil {
			return Report{}, fmt.Errorf("ingest barangay at row %d: %w", rowsRead+1, err)
		} else if inserted {
			counts.barangays++
		}
	}

	completedAt := time.Now().UTC()
	if err := persistIngestMetadata(ctx, tx, filepath.Base(csvPath), checksum, rowsRead, counts, completedAt); err != nil {
		return Report{}, err
	}

	if err := tx.Commit(); err != nil {
		return Report{}, fmt.Errorf("commit ingest tx: %w", err)
	}
	committed = true

	return Report{
		SourceFileName:    filepath.Base(csvPath),
		SourceChecksum:    checksum,
		RowsRead:          rowsRead,
		RegionsInserted:   counts.regions,
		ProvincesInserted: counts.provinces,
		CitiesInserted:    counts.cities,
		BarangaysInserted: counts.barangays,
		RowsSkipped:       rowsRead - counts.barangays,
		Skipped:           false,
		CompletedAtUTC:    completedAt,
	}, nil
}

func checksumFile(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek csv start: %w", err)
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("hash csv: %w", err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

type ingestMetadata struct {
	SourceFileName string
	SourceChecksum string
	RowsRead       int
	RowsRegions    int
	RowsProvinces  int
	RowsCities     int
	RowsBarangays  int
	IngestedAtUTC  string
}

func loadIngestMetadata(ctx context.Context, database *sql.DB) (*ingestMetadata, error) {
	if database == nil {
		return nil, fmt.Errorf("db is nil")
	}

	row := database.QueryRowContext(ctx, `
SELECT source_file_name, source_checksum, rows_read, rows_regions, rows_provinces, rows_cities, rows_barangays, ingested_at
FROM psgc_ingest_metadata
WHERE id = 1;
`)

	var metadata ingestMetadata
	if err := row.Scan(
		&metadata.SourceFileName,
		&metadata.SourceChecksum,
		&metadata.RowsRead,
		&metadata.RowsRegions,
		&metadata.RowsProvinces,
		&metadata.RowsCities,
		&metadata.RowsBarangays,
		&metadata.IngestedAtUTC,
	); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func validateHeader(header []string) error {
	if len(header) != len(expectedHeaders) {
		return fmt.Errorf("unexpected PSGC header width: got %d want %d", len(header), len(expectedHeaders))
	}

	for i, expected := range expectedHeaders {
		if header[i] != expected {
			return fmt.Errorf("unexpected PSGC header at position %d: got %q want %q", i+1, header[i], expected)
		}
	}

	return nil
}

func parseSourceRow(record []string) (sourceRow, error) {
	if len(record) != psgcHeaderColumns {
		return sourceRow{}, fmt.Errorf("unexpected column count: got %d want %d", len(record), psgcHeaderColumns)
	}

	regionCode, err := requiredField(record, psgcColRegionCode2025, "region_code2025")
	if err != nil {
		return sourceRow{}, err
	}
	regionName, err := requiredField(record, psgcColRegionName2025, "region_name2025")
	if err != nil {
		return sourceRow{}, err
	}
	provinceCode, err := requiredField(record, psgcColProvinceCode2025, "prov_code2025")
	if err != nil {
		return sourceRow{}, err
	}
	provinceName, err := requiredField(record, psgcColProvinceName2025, "prov_name2025")
	if err != nil {
		return sourceRow{}, err
	}
	cityCode, err := requiredField(record, psgcColCityCode2025, "citymun_code2025")
	if err != nil {
		return sourceRow{}, err
	}
	cityName, err := requiredField(record, psgcColCityName2025, "citymun_name2025")
	if err != nil {
		return sourceRow{}, err
	}
	barangayCode, err := requiredField(record, psgcColBarangayCode2025, "barangay_code2025")
	if err != nil {
		return sourceRow{}, err
	}
	barangayName, err := requiredField(record, psgcColBarangayName2025, "barangay_name2025")
	if err != nil {
		return sourceRow{}, err
	}

	return sourceRow{
		RegionCode:   regionCode,
		RegionName:   regionName,
		ProvinceCode: provinceCode,
		ProvinceName: provinceName,
		CityCode:     cityCode,
		CityName:     cityName,
		BarangayCode: barangayCode,
		BarangayName: barangayName,
	}, nil
}

func requiredField(record []string, index int, fieldName string) (string, error) {
	if index >= len(record) {
		return "", fmt.Errorf("missing %s", fieldName)
	}

	value := strings.TrimSpace(record[index])
	if value == "" {
		return "", fmt.Errorf("missing %s", fieldName)
	}

	return value, nil
}

func ensureRegion(ctx context.Context, tx *sql.Tx, seen map[string]regionEntry, entry regionEntry) (bool, error) {
	if existing, ok := seen[entry.Code]; ok {
		if existing != entry {
			return false, fmt.Errorf("region code %s has conflicting values", entry.Code)
		}
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO psgc_regions (region_code, region_name) VALUES (?, ?);`, entry.Code, entry.Name); err != nil {
		return false, fmt.Errorf("insert region %s: %w", entry.Code, err)
	}

	seen[entry.Code] = entry
	return true, nil
}

func ensureProvince(ctx context.Context, tx *sql.Tx, regions map[string]regionEntry, seen map[string]provinceEntry, entry provinceEntry) (bool, error) {
	if _, ok := regions[entry.RegionCode]; !ok {
		return false, fmt.Errorf("province %s references unknown region %s", entry.Code, entry.RegionCode)
	}

	if existing, ok := seen[entry.Code]; ok {
		if existing != entry {
			return false, fmt.Errorf("province code %s has conflicting values", entry.Code)
		}
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO psgc_provinces (province_code, region_code, province_name) VALUES (?, ?, ?);`, entry.Code, entry.RegionCode, entry.Name); err != nil {
		return false, fmt.Errorf("insert province %s: %w", entry.Code, err)
	}

	seen[entry.Code] = entry
	return true, nil
}

func ensureCity(ctx context.Context, tx *sql.Tx, regions map[string]regionEntry, provinces map[string]provinceEntry, seen map[string]cityEntry, entry cityEntry) (bool, error) {
	if _, ok := regions[entry.RegionCode]; !ok {
		return false, fmt.Errorf("city %s references unknown region %s", entry.Code, entry.RegionCode)
	}

	province, ok := provinces[entry.ProvinceCode]
	if !ok {
		return false, fmt.Errorf("city %s references unknown province %s", entry.Code, entry.ProvinceCode)
	}
	if province.RegionCode != entry.RegionCode {
		return false, fmt.Errorf("city %s has province/region mismatch: province %s belongs to %s, not %s", entry.Code, entry.ProvinceCode, province.RegionCode, entry.RegionCode)
	}

	if existing, ok := seen[entry.Code]; ok {
		if existing != entry {
			return false, fmt.Errorf("city code %s has conflicting values", entry.Code)
		}
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO psgc_cities (city_code, region_code, province_code, city_name, city_type) VALUES (?, ?, ?, ?, ?);`, entry.Code, entry.RegionCode, entry.ProvinceCode, entry.Name, nullString(entry.Type)); err != nil {
		return false, fmt.Errorf("insert city %s: %w", entry.Code, err)
	}

	seen[entry.Code] = entry
	return true, nil
}

func ensureBarangay(ctx context.Context, tx *sql.Tx, regions map[string]regionEntry, provinces map[string]provinceEntry, cities map[string]cityEntry, seen map[string]barangayEntry, entry barangayEntry) (bool, error) {
	if _, ok := regions[entry.RegionCode]; !ok {
		return false, fmt.Errorf("barangay %s references unknown region %s", entry.Code, entry.RegionCode)
	}

	province, ok := provinces[entry.ProvinceCode]
	if !ok {
		return false, fmt.Errorf("barangay %s references unknown province %s", entry.Code, entry.ProvinceCode)
	}
	if province.RegionCode != entry.RegionCode {
		return false, fmt.Errorf("barangay %s has province/region mismatch: province %s belongs to %s, not %s", entry.Code, entry.ProvinceCode, province.RegionCode, entry.RegionCode)
	}

	city, ok := cities[entry.CityCode]
	if !ok {
		return false, fmt.Errorf("barangay %s references unknown city %s", entry.Code, entry.CityCode)
	}
	if city.RegionCode != entry.RegionCode {
		return false, fmt.Errorf("barangay %s has city/region mismatch: city %s belongs to %s, not %s", entry.Code, entry.CityCode, city.RegionCode, entry.RegionCode)
	}
	if city.ProvinceCode != entry.ProvinceCode {
		return false, fmt.Errorf("barangay %s has city/province mismatch: city %s belongs to province %s, not %s", entry.Code, entry.CityCode, city.ProvinceCode, entry.ProvinceCode)
	}

	if existing, ok := seen[entry.Code]; ok {
		if existing != entry {
			return false, fmt.Errorf("barangay code %s has conflicting values", entry.Code)
		}
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO psgc_barangays (barangay_code, region_code, province_code, city_code, barangay_name) VALUES (?, ?, ?, ?, ?);`, entry.Code, entry.RegionCode, entry.ProvinceCode, entry.CityCode, entry.Name); err != nil {
		return false, fmt.Errorf("insert barangay %s: %w", entry.Code, err)
	}

	seen[entry.Code] = entry
	return true, nil
}

func persistIngestMetadata(ctx context.Context, tx *sql.Tx, fileName, checksum string, rowsRead int, counts ingestCounts, completedAt time.Time) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO psgc_ingest_metadata (
    id,
    source_file_name,
    source_checksum,
    rows_read,
    rows_regions,
    rows_provinces,
    rows_cities,
    rows_barangays,
    ingested_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    source_file_name = excluded.source_file_name,
    source_checksum = excluded.source_checksum,
    rows_read = excluded.rows_read,
    rows_regions = excluded.rows_regions,
    rows_provinces = excluded.rows_provinces,
    rows_cities = excluded.rows_cities,
    rows_barangays = excluded.rows_barangays,
    ingested_at = excluded.ingested_at;
`, 1, fileName, checksum, rowsRead, counts.regions, counts.provinces, counts.cities, counts.barangays, completedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("persist psgc ingest metadata: %w", err)
	}

	return nil
}

func nullString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
