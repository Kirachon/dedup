package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dedup/internal/db"
	"dedup/internal/jobs"
)

func TestBackupServiceCreateSnapshotWritesMetadataManifest(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, time.March, 25, 16, 0, 0, 123456789, time.UTC)
	svc, cleanup := newBackupTestService(t, WithBackupClock(func() time.Time { return fixed }))
	defer cleanup()

	if err := upsertSettingValue(context.Background(), svc.db, "restore.fixture", "v1"); err != nil {
		t.Fatalf("seed setting: %v", err)
	}

	outputDir := filepath.Join(t.TempDir(), "snapshots")
	result, err := svc.CreateSnapshot(context.Background(), SnapshotRequest{
		OutputDir:   outputDir,
		PerformedBy: "operator-a",
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if result.BackupID == "" {
		t.Fatalf("expected backup id")
	}
	if result.SnapshotSHA256 == "" {
		t.Fatalf("expected snapshot checksum")
	}
	if result.SizeBytes <= 0 {
		t.Fatalf("expected positive snapshot size, got %d", result.SizeBytes)
	}
	if !strings.HasPrefix(filepath.Base(result.SnapshotPath), defaultBackupFilePrefix+"-") {
		t.Fatalf("unexpected snapshot filename: %s", result.SnapshotPath)
	}
	if _, err := os.Stat(result.SnapshotPath); err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}
	if _, err := os.Stat(result.ManifestPath); err != nil {
		t.Fatalf("manifest file missing: %v", err)
	}

	manifest, err := readManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if manifest.BackupID != result.BackupID {
		t.Fatalf("manifest backup id mismatch: got=%s want=%s", manifest.BackupID, result.BackupID)
	}
	if manifest.SnapshotSHA256 != result.SnapshotSHA256 {
		t.Fatalf("manifest checksum mismatch: got=%s want=%s", manifest.SnapshotSHA256, result.SnapshotSHA256)
	}
	if manifest.SnapshotSizeBytes != result.SizeBytes {
		t.Fatalf("manifest size mismatch: got=%d want=%d", manifest.SnapshotSizeBytes, result.SizeBytes)
	}
	if manifest.ManifestVersion != manifestVersionV1 {
		t.Fatalf("manifest version mismatch: %s", manifest.ManifestVersion)
	}

	if !result.AuditEvent.Persisted {
		t.Fatalf("expected persisted audit event, got %+v", result.AuditEvent)
	}
	if result.AuditEvent.Action != "BACKUP_CREATED" {
		t.Fatalf("unexpected audit action: %s", result.AuditEvent.Action)
	}
	if result.AuditEvent.PerformedBy != "operator-a" {
		t.Fatalf("unexpected audit actor: %s", result.AuditEvent.PerformedBy)
	}
	snapshotAuditDetails := mustParseBackupAuditDetails(t, result.AuditEvent.DetailsJSON)
	assertAuditPathRedacted(t, snapshotAuditDetails, "snapshot_path", result.SnapshotPath)
	assertAuditPathRedacted(t, snapshotAuditDetails, "manifest_path", result.ManifestPath)

	count, err := countAuditByAction(context.Background(), svc.db, "BACKUP_CREATED")
	if err != nil {
		t.Fatalf("count backup audit logs: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected backup audit row to be inserted")
	}
}

func TestBackupServiceValidateRestoreDryRunPasses(t *testing.T) {
	t.Parallel()

	svc, cleanup := newBackupTestService(t)
	defer cleanup()

	if err := upsertSettingValue(context.Background(), svc.db, "restore.fixture", "v1"); err != nil {
		t.Fatalf("seed setting: %v", err)
	}

	snapshot, err := svc.CreateSnapshot(context.Background(), SnapshotRequest{
		OutputDir: filepath.Join(t.TempDir(), "snapshots"),
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	validation, err := svc.ValidateRestore(context.Background(), RestoreValidationRequest{
		SnapshotPath: snapshot.SnapshotPath,
		ManifestPath: snapshot.ManifestPath,
	})
	if err != nil {
		t.Fatalf("validate restore dry-run: %v", err)
	}

	if !validation.Valid {
		t.Fatalf("expected validation to be valid")
	}
	if !validation.ManifestPresent || !validation.ManifestMatches {
		t.Fatalf("expected manifest to be present and matching: %+v", validation)
	}
	if !validation.ApplyReady {
		t.Fatalf("expected apply-ready validation, blocking=%d", len(validation.BlockingJobs))
	}
	if validation.ExpectedConfirmation != svc.ExpectedRestoreConfirmation() {
		t.Fatalf("unexpected confirmation phrase: got=%s want=%s", validation.ExpectedConfirmation, svc.ExpectedRestoreConfirmation())
	}
	if validation.SnapshotSHA256 == "" || validation.SnapshotSizeBytes <= 0 {
		t.Fatalf("expected checksum and size in validation: %+v", validation)
	}
	if validation.AuditEvent.Action != "RESTORE_DRY_RUN" {
		t.Fatalf("unexpected dry-run audit action: %s", validation.AuditEvent.Action)
	}
	validationAuditDetails := mustParseBackupAuditDetails(t, validation.AuditEvent.DetailsJSON)
	assertAuditPathRedacted(t, validationAuditDetails, "snapshot_path", validation.SnapshotPath)
	assertAuditPathRedacted(t, validationAuditDetails, "manifest_path", validation.ManifestPath)
}

func TestBackupServiceApplyRestoreReplacesLiveDBOnIdle(t *testing.T) {
	t.Parallel()

	svc, cleanup := newBackupTestService(t)
	defer cleanup()

	ctx := context.Background()
	if err := upsertSettingValue(ctx, svc.db, "restore.fixture", "before"); err != nil {
		t.Fatalf("seed setting before snapshot: %v", err)
	}

	snapshot, err := svc.CreateSnapshot(ctx, SnapshotRequest{
		OutputDir:   filepath.Join(t.TempDir(), "snapshots"),
		PerformedBy: "operator-b",
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if err := upsertSettingValue(ctx, svc.db, "restore.fixture", "after"); err != nil {
		t.Fatalf("mutate live db before restore: %v", err)
	}

	restore, err := svc.ApplyRestore(ctx, RestoreApplyRequest{
		SnapshotPath: snapshot.SnapshotPath,
		ManifestPath: snapshot.ManifestPath,
		Confirmation: svc.ExpectedRestoreConfirmation(),
		PerformedBy:  "operator-b",
	})
	if err != nil {
		t.Fatalf("apply restore: %v", err)
	}

	if !restore.Restored {
		t.Fatalf("expected restored result")
	}
	if restore.PreRestorePath == "" {
		t.Fatalf("expected pre-restore path")
	}
	if _, err := os.Stat(restore.PreRestorePath); err != nil {
		t.Fatalf("pre-restore snapshot missing: %v", err)
	}
	if restore.AuditEvent.Action != "RESTORE_APPLIED" {
		t.Fatalf("unexpected restore audit action: %s", restore.AuditEvent.Action)
	}
	restoreAuditDetails := mustParseBackupAuditDetails(t, restore.AuditEvent.DetailsJSON)
	assertAuditPathRedacted(t, restoreAuditDetails, "snapshot_path", restore.SnapshotPath)
	assertAuditPathRedacted(t, restoreAuditDetails, "manifest_path", restore.ManifestPath)
	assertAuditPathRedacted(t, restoreAuditDetails, "pre_restore_path", restore.PreRestorePath)
	assertAuditPathRedacted(t, restoreAuditDetails, "db_path", restore.DBPath)

	value, err := readSettingValue(ctx, svc.db, "restore.fixture")
	if err != nil {
		t.Fatalf("read restored setting: %v", err)
	}
	if value != "before" {
		t.Fatalf("expected restored value 'before', got %q", value)
	}
}

func TestBackupServiceApplyRestoreBlocksWhenActiveJobs(t *testing.T) {
	t.Parallel()

	svc, cleanup := newBackupTestService(t)
	defer cleanup()

	ctx := context.Background()
	snapshot, err := svc.CreateSnapshot(ctx, SnapshotRequest{
		OutputDir: filepath.Join(t.TempDir(), "snapshots"),
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	manager := jobs.NewManager(svc.db, svc.writer)
	if _, err := manager.Enqueue(ctx, "import-job-1", jobs.TransitionOptions{}); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	if _, err := manager.Transition(ctx, "import-job-1", jobs.StateRunning, jobs.TransitionOptions{}); err != nil {
		t.Fatalf("transition job to running: %v", err)
	}

	_, err = svc.ApplyRestore(ctx, RestoreApplyRequest{
		SnapshotPath: snapshot.SnapshotPath,
		ManifestPath: snapshot.ManifestPath,
		Confirmation: svc.ExpectedRestoreConfirmation(),
		PerformedBy:  "operator-c",
	})
	if err == nil {
		t.Fatalf("expected restore to be blocked by active job")
	}
	if !errors.Is(err, ErrRestoreBlockedActiveJobs) {
		t.Fatalf("expected ErrRestoreBlockedActiveJobs, got %v", err)
	}
}

func TestBackupServiceCreateSnapshotFailsWhenOutputDirNotWritable(t *testing.T) {
	t.Parallel()

	svc, cleanup := newBackupTestService(t)
	defer cleanup()

	outputDir := filepath.Join(t.TempDir(), "read-only-snapshots")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}
	if err := os.Chmod(outputDir, 0o555); err != nil {
		t.Skipf("chmod not supported in test environment: %v", err)
	}
	defer func() {
		_ = os.Chmod(outputDir, 0o755)
	}()

	probe, err := os.CreateTemp(outputDir, ".permission-probe-*")
	if err == nil {
		_ = probe.Close()
		_ = os.Remove(probe.Name())
		t.Skip("filesystem permits writes in read-only test directory; skipping writability guard test")
	}

	_, err = svc.CreateSnapshot(context.Background(), SnapshotRequest{
		OutputDir: outputDir,
	})
	if err == nil {
		t.Fatalf("expected snapshot creation to fail for non-writable directory")
	}
	if !strings.Contains(err.Error(), "not writable") {
		t.Fatalf("expected non-writable directory error, got %v", err)
	}
}

func newBackupTestService(t *testing.T, opts ...BackupOption) (*BackupService, func()) {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "live.db")
	handle, err := db.OpenAndMigrate(ctx, dbPath)
	if err != nil {
		t.Fatalf("open and migrate db: %v", err)
	}

	svc, err := NewBackupService(handle.DB, dbPath, handle.Writer, opts...)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("create backup service: %v", err)
	}

	cleanup := func() {
		if svc.db != nil {
			_ = svc.db.Close()
		}
	}
	return svc, cleanup
}

func upsertSettingValue(ctx context.Context, database *sql.DB, key, value string) error {
	_, err := database.ExecContext(ctx, `
INSERT INTO app_settings (setting_key, setting_value, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(setting_key) DO UPDATE SET
    setting_value = excluded.setting_value,
    updated_at = excluded.updated_at;
`, key, value, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func readSettingValue(ctx context.Context, database *sql.DB, key string) (string, error) {
	var value string
	if err := database.QueryRowContext(ctx, `
SELECT setting_value
FROM app_settings
WHERE setting_key = ?
LIMIT 1;
`, key).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func countAuditByAction(ctx context.Context, database *sql.DB, action string) (int, error) {
	var count int
	if err := database.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM audit_logs
WHERE action = ?;
`, action).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func mustParseBackupAuditDetails(t *testing.T, detailsJSON string) map[string]any {
	t.Helper()

	var details map[string]any
	if err := json.Unmarshal([]byte(detailsJSON), &details); err != nil {
		t.Fatalf("unmarshal backup audit details: %v", err)
	}
	return details
}

func assertAuditPathRedacted(t *testing.T, details map[string]any, key, originalPath string) {
	t.Helper()

	rawValue, ok := details[key]
	if !ok {
		t.Fatalf("expected audit details key %q", key)
	}
	value, ok := rawValue.(string)
	if !ok {
		t.Fatalf("expected audit details key %q to be string, got %T", key, rawValue)
	}

	expected := portablePathBase(originalPath)
	if expected == "" {
		expected = redactedPathValue
	}
	if value != expected {
		t.Fatalf("unexpected redacted value for %s: got=%q want=%q", key, value, expected)
	}
	if filepath.IsAbs(value) {
		t.Fatalf("expected non-absolute audit value for %s, got %q", key, value)
	}
}
