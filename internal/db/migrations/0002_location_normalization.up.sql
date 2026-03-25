CREATE TABLE IF NOT EXISTS location_normalization_runs (
    run_id TEXT PRIMARY KEY,
    import_id TEXT,
    source_reference TEXT,
    mode TEXT NOT NULL CHECK (mode IN ('SHADOW', 'WRITE')),
    status TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    total_rows INTEGER NOT NULL DEFAULT 0 CHECK (total_rows >= 0),
    auto_applied_rows INTEGER NOT NULL DEFAULT 0 CHECK (auto_applied_rows >= 0),
    review_rows INTEGER NOT NULL DEFAULT 0 CHECK (review_rows >= 0),
    failed_rows INTEGER NOT NULL DEFAULT 0 CHECK (failed_rows >= 0),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (import_id) REFERENCES import_logs(import_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_location_normalization_runs_import_id
    ON location_normalization_runs(import_id);
CREATE INDEX IF NOT EXISTS idx_location_normalization_runs_status_started_at
    ON location_normalization_runs(status, started_at DESC);

CREATE TABLE IF NOT EXISTS location_normalization_items (
    item_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    row_number INTEGER NOT NULL CHECK (row_number > 0),
    source_reference TEXT,
    raw_region TEXT NOT NULL DEFAULT '',
    raw_province TEXT NOT NULL DEFAULT '',
    raw_city TEXT NOT NULL DEFAULT '',
    raw_barangay TEXT NOT NULL DEFAULT '',
    resolved_region_code TEXT,
    resolved_region_name TEXT,
    resolved_province_code TEXT,
    resolved_province_name TEXT,
    resolved_city_code TEXT,
    resolved_city_name TEXT,
    resolved_barangay_code TEXT,
    resolved_barangay_name TEXT,
    confidence REAL NOT NULL DEFAULT 0 CHECK (confidence >= 0 AND confidence <= 1),
    match_source TEXT NOT NULL CHECK (match_source IN ('NONE', 'EXACT', 'FUZZY', 'MIXED')),
    status TEXT NOT NULL CHECK (status IN ('AUTO_APPLIED', 'REVIEW_NEEDED')),
    needs_review INTEGER NOT NULL CHECK (needs_review IN (0, 1)),
    reason TEXT,
    normalization_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES location_normalization_runs(run_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_location_normalization_items_run_row_number
    ON location_normalization_items(run_id, row_number);
CREATE INDEX IF NOT EXISTS idx_location_normalization_items_run_row
    ON location_normalization_items(run_id, row_number);
CREATE INDEX IF NOT EXISTS idx_location_normalization_items_status_review
    ON location_normalization_items(status, needs_review);

UPDATE app_metadata
SET value = '2'
WHERE key = 'schema.version';
