CREATE TABLE IF NOT EXISTS app_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO app_metadata (key, value) VALUES
    ('contracts.version', 'v1-frozen'),
    ('schema.version', '1');

CREATE TABLE IF NOT EXISTS id_sequences (
    sequence_key TEXT PRIMARY KEY,
    last_value INTEGER NOT NULL CHECK (last_value >= 0)
);

CREATE TABLE IF NOT EXISTS beneficiaries (
    internal_uuid TEXT PRIMARY KEY,
    generated_id TEXT NOT NULL,
    last_name TEXT NOT NULL,
    first_name TEXT NOT NULL,
    middle_name TEXT,
    extension_name TEXT,
    norm_last_name TEXT NOT NULL,
    norm_first_name TEXT NOT NULL,
    norm_middle_name TEXT,
    norm_extension_name TEXT,
    region_code TEXT NOT NULL,
    region_name TEXT NOT NULL,
    province_code TEXT NOT NULL,
    province_name TEXT NOT NULL,
    city_code TEXT NOT NULL,
    city_name TEXT NOT NULL,
    barangay_code TEXT NOT NULL,
    barangay_name TEXT NOT NULL,
    contact_no TEXT,
    contact_no_norm TEXT,
    birth_month INTEGER CHECK (birth_month BETWEEN 1 AND 12),
    birth_day INTEGER CHECK (birth_day BETWEEN 1 AND 31),
    birth_year INTEGER CHECK (birth_year BETWEEN 1800 AND 3000),
    birthdate_iso TEXT,
    sex TEXT NOT NULL,
    record_status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (record_status IN ('ACTIVE', 'RETAINED', 'DELETED')),
    dedup_status TEXT NOT NULL DEFAULT 'CLEAR' CHECK (dedup_status IN ('CLEAR', 'POSSIBLE_DUPLICATE', 'RESOLVED')),
    source_type TEXT NOT NULL DEFAULT 'LOCAL' CHECK (source_type IN ('LOCAL', 'IMPORT')),
    source_reference TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_beneficiaries_generated_id ON beneficiaries(generated_id);
CREATE INDEX IF NOT EXISTS idx_beneficiaries_norm_name ON beneficiaries(norm_last_name, norm_first_name, norm_middle_name, norm_extension_name);
CREATE INDEX IF NOT EXISTS idx_beneficiaries_city_barangay ON beneficiaries(city_code, barangay_code);
CREATE INDEX IF NOT EXISTS idx_beneficiaries_record_dedup_status ON beneficiaries(record_status, dedup_status);
CREATE INDEX IF NOT EXISTS idx_beneficiaries_birth_year ON beneficiaries(birth_year);
CREATE INDEX IF NOT EXISTS idx_beneficiaries_contact_no_norm ON beneficiaries(contact_no_norm);
CREATE UNIQUE INDEX IF NOT EXISTS ux_beneficiaries_source_reference_live
    ON beneficiaries(source_type, source_reference)
    WHERE source_reference IS NOT NULL AND source_reference <> '' AND deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS dedup_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    total_candidates INTEGER NOT NULL DEFAULT 0 CHECK (total_candidates >= 0),
    total_matches INTEGER NOT NULL DEFAULT 0 CHECK (total_matches >= 0),
    notes TEXT
);

CREATE TABLE IF NOT EXISTS dedup_matches (
    match_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    record_a_uuid TEXT NOT NULL,
    record_b_uuid TEXT NOT NULL,
    pair_key TEXT NOT NULL,
    first_name_score REAL NOT NULL CHECK (first_name_score >= 0 AND first_name_score <= 100),
    middle_name_score REAL NOT NULL CHECK (middle_name_score >= 0 AND middle_name_score <= 100),
    last_name_score REAL NOT NULL CHECK (last_name_score >= 0 AND last_name_score <= 100),
    extension_name_score REAL NOT NULL CHECK (extension_name_score >= 0 AND extension_name_score <= 100),
    total_score REAL NOT NULL CHECK (total_score >= 0 AND total_score <= 100),
    birthdate_compare INTEGER,
    barangay_compare INTEGER,
    decision_status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TEXT NOT NULL,
    CHECK (record_a_uuid <> record_b_uuid),
    FOREIGN KEY (run_id) REFERENCES dedup_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (record_a_uuid) REFERENCES beneficiaries(internal_uuid),
    FOREIGN KEY (record_b_uuid) REFERENCES beneficiaries(internal_uuid)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dedup_matches_run_pair ON dedup_matches(run_id, pair_key);
CREATE INDEX IF NOT EXISTS idx_dedup_matches_pair_key ON dedup_matches(pair_key);
CREATE INDEX IF NOT EXISTS idx_dedup_matches_decision_status ON dedup_matches(decision_status);

CREATE TABLE IF NOT EXISTS dedup_decisions (
    decision_id TEXT PRIMARY KEY,
    pair_key TEXT NOT NULL UNIQUE,
    record_a_uuid TEXT NOT NULL,
    record_b_uuid TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('RETAIN_A', 'RETAIN_B', 'RETAIN_BOTH', 'DELETE_A_SOFT', 'DELETE_B_SOFT', 'DIFFERENT_PERSONS')),
    resolved_by TEXT NOT NULL,
    resolved_at TEXT NOT NULL,
    notes TEXT,
    FOREIGN KEY (record_a_uuid) REFERENCES beneficiaries(internal_uuid),
    FOREIGN KEY (record_b_uuid) REFERENCES beneficiaries(internal_uuid)
);

CREATE TABLE IF NOT EXISTS app_settings (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_logs (
    import_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK (source_type IN ('CSV', 'EXCHANGE_PACKAGE')),
    source_reference TEXT NOT NULL,
    file_name TEXT,
    file_hash TEXT,
    idempotency_key TEXT,
    rows_read INTEGER NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_inserted INTEGER NOT NULL DEFAULT 0 CHECK (rows_inserted >= 0),
    rows_skipped INTEGER NOT NULL DEFAULT 0 CHECK (rows_skipped >= 0),
    rows_failed INTEGER NOT NULL DEFAULT 0 CHECK (rows_failed >= 0),
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    checkpoint_token TEXT,
    operator_name TEXT,
    remarks TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_import_logs_hash_idempotency
    ON import_logs(file_hash, idempotency_key)
    WHERE file_hash IS NOT NULL AND file_hash <> '' AND idempotency_key IS NOT NULL AND idempotency_key <> '';

CREATE TABLE IF NOT EXISTS export_logs (
    export_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    export_type TEXT NOT NULL,
    rows_exported INTEGER NOT NULL DEFAULT 0 CHECK (rows_exported >= 0),
    created_at TEXT NOT NULL,
    performed_by TEXT
);

CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    performed_by TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON audit_logs(entity_type, entity_id, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action_created_at ON audit_logs(action, created_at);

CREATE TABLE IF NOT EXISTS job_states (
    job_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'cancel_requested', 'cancelled', 'succeeded', 'failed', 'recoverable')),
    updated_at_utc TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    progress_percent REAL CHECK (progress_percent IS NULL OR (progress_percent >= 0 AND progress_percent <= 100)),
    message TEXT,
    error_code TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_states_state ON job_states(state);

CREATE TABLE IF NOT EXISTS psgc_regions (
    region_code TEXT PRIMARY KEY,
    region_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS psgc_provinces (
    province_code TEXT PRIMARY KEY,
    region_code TEXT NOT NULL,
    province_name TEXT NOT NULL,
    FOREIGN KEY (region_code) REFERENCES psgc_regions(region_code)
);

CREATE INDEX IF NOT EXISTS idx_psgc_provinces_region ON psgc_provinces(region_code);

CREATE TABLE IF NOT EXISTS psgc_cities (
    city_code TEXT PRIMARY KEY,
    region_code TEXT NOT NULL,
    province_code TEXT,
    city_name TEXT NOT NULL,
    city_type TEXT,
    FOREIGN KEY (region_code) REFERENCES psgc_regions(region_code),
    FOREIGN KEY (province_code) REFERENCES psgc_provinces(province_code)
);

CREATE INDEX IF NOT EXISTS idx_psgc_cities_region ON psgc_cities(region_code);
CREATE INDEX IF NOT EXISTS idx_psgc_cities_province ON psgc_cities(province_code);

CREATE TABLE IF NOT EXISTS psgc_barangays (
    barangay_code TEXT PRIMARY KEY,
    region_code TEXT NOT NULL,
    province_code TEXT,
    city_code TEXT NOT NULL,
    barangay_name TEXT NOT NULL,
    FOREIGN KEY (region_code) REFERENCES psgc_regions(region_code),
    FOREIGN KEY (province_code) REFERENCES psgc_provinces(province_code),
    FOREIGN KEY (city_code) REFERENCES psgc_cities(city_code)
);

CREATE INDEX IF NOT EXISTS idx_psgc_barangays_city ON psgc_barangays(city_code);
CREATE INDEX IF NOT EXISTS idx_psgc_barangays_province ON psgc_barangays(province_code);
CREATE INDEX IF NOT EXISTS idx_psgc_barangays_region ON psgc_barangays(region_code);

CREATE TABLE IF NOT EXISTS psgc_ingest_metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    source_file_name TEXT NOT NULL,
    source_checksum TEXT NOT NULL,
    rows_read INTEGER NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_regions INTEGER NOT NULL DEFAULT 0 CHECK (rows_regions >= 0),
    rows_provinces INTEGER NOT NULL DEFAULT 0 CHECK (rows_provinces >= 0),
    rows_cities INTEGER NOT NULL DEFAULT 0 CHECK (rows_cities >= 0),
    rows_barangays INTEGER NOT NULL DEFAULT 0 CHECK (rows_barangays >= 0),
    ingested_at TEXT NOT NULL
);
