DROP INDEX IF EXISTS idx_location_normalization_items_status_review;
DROP INDEX IF EXISTS idx_location_normalization_items_run_row;
DROP INDEX IF EXISTS ux_location_normalization_items_run_row_number;
DROP TABLE IF EXISTS location_normalization_items;

DROP INDEX IF EXISTS idx_location_normalization_runs_status_started_at;
DROP INDEX IF EXISTS idx_location_normalization_runs_import_id;
DROP TABLE IF EXISTS location_normalization_runs;

UPDATE app_metadata
SET value = '1'
WHERE key = 'schema.version';
