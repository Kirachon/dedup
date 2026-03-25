package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestMigrateUpAndDown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "migration-test.db")

	database, err := OpenSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer database.Close()

	version, err := CurrentVersion(ctx, database)
	if err != nil {
		t.Fatalf("read initial version: %v", err)
	}
	if version != 0 {
		t.Fatalf("expected initial version 0, got %d", version)
	}

	if err := MigrateUp(ctx, database); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	if err := MigrateUp(ctx, database); err != nil {
		t.Fatalf("migrate up idempotent run: %v", err)
	}

	version, err = CurrentVersion(ctx, database)
	if err != nil {
		t.Fatalf("read version after migrate up: %v", err)
	}
	if version != 1 {
		t.Fatalf("expected version 1 after migrate up, got %d", version)
	}

	requiredTables := []string{
		"app_metadata",
		"beneficiaries",
		"id_sequences",
		"dedup_runs",
		"dedup_matches",
		"dedup_decisions",
		"app_settings",
		"import_logs",
		"export_logs",
		"audit_logs",
		"job_states",
		"psgc_regions",
		"psgc_provinces",
		"psgc_cities",
		"psgc_barangays",
		"psgc_ingest_metadata",
	}

	for _, tableName := range requiredTables {
		exists, err := sqliteObjectExists(ctx, database, "table", tableName)
		if err != nil {
			t.Fatalf("check table %s: %v", tableName, err)
		}
		if !exists {
			t.Fatalf("expected table %s to exist after migrate up", tableName)
		}
	}

	requiredIndexes := []string{
		"ux_beneficiaries_generated_id",
		"idx_beneficiaries_norm_name",
		"idx_beneficiaries_city_barangay",
		"idx_beneficiaries_record_dedup_status",
		"idx_beneficiaries_birth_year",
		"idx_dedup_matches_pair_key",
		"idx_dedup_matches_decision_status",
		"ux_beneficiaries_source_reference_live",
	}

	for _, indexName := range requiredIndexes {
		exists, err := sqliteObjectExists(ctx, database, "index", indexName)
		if err != nil {
			t.Fatalf("check index %s: %v", indexName, err)
		}
		if !exists {
			t.Fatalf("expected index %s to exist after migrate up", indexName)
		}
	}

	if err := MigrateDown(ctx, database, 0); err != nil {
		t.Fatalf("migrate down to 0: %v", err)
	}

	version, err = CurrentVersion(ctx, database)
	if err != nil {
		t.Fatalf("read version after migrate down: %v", err)
	}
	if version != 0 {
		t.Fatalf("expected version 0 after migrate down, got %d", version)
	}

	beneficiariesExists, err := sqliteObjectExists(ctx, database, "table", "beneficiaries")
	if err != nil {
		t.Fatalf("check beneficiaries table after down: %v", err)
	}
	if beneficiariesExists {
		t.Fatalf("expected beneficiaries table to be removed after migrate down")
	}
}

func sqliteObjectExists(ctx context.Context, database *sql.DB, objectType, name string) (bool, error) {
	var found int
	err := database.QueryRowContext(ctx, `
SELECT 1
FROM sqlite_master
WHERE type = ? AND name = ?
LIMIT 1;
`, objectType, name).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
