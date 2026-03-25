package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"dedup/internal/db"
	"dedup/internal/jobs"

	"github.com/google/uuid"
)

const (
	defaultBackupFilePrefix = "beneficiary-backup"
	manifestVersionV1       = "backup_manifest_v1"
	redactedPathValue       = "[redacted]"
)

var backupAuditPathKeys = map[string]struct{}{
	"snapshot_path":    {},
	"manifest_path":    {},
	"pre_restore_path": {},
	"db_path":          {},
}

var (
	// ErrSnapshotOutputDirRequired means no output directory was provided.
	ErrSnapshotOutputDirRequired = errors.New("snapshot output directory is required")
	// ErrSnapshotPathRequired means no snapshot path was provided.
	ErrSnapshotPathRequired = errors.New("snapshot path is required")
	// ErrRestoreConfirmationRequired means apply restore requires explicit confirmation.
	ErrRestoreConfirmationRequired = errors.New("restore confirmation is required")
	// ErrRestoreConfirmationMismatch means the provided restore confirmation did not match.
	ErrRestoreConfirmationMismatch = errors.New("restore confirmation mismatch")
	// ErrRestoreBlockedActiveJobs means restore was blocked by active/incomplete jobs.
	ErrRestoreBlockedActiveJobs = errors.New("restore blocked by active jobs")
)

// BackupOption configures BackupService behavior.
type BackupOption func(*BackupService)

// BackupManifest stores immutable snapshot metadata used by dry-run/apply validation.
type BackupManifest struct {
	ManifestVersion   string `json:"manifest_version"`
	BackupID          string `json:"backup_id"`
	SourceDBPath      string `json:"source_db_path"`
	SnapshotFile      string `json:"snapshot_file"`
	SnapshotSHA256    string `json:"snapshot_sha256"`
	SnapshotSizeBytes int64  `json:"snapshot_size_bytes"`
	CreatedAtUTC      string `json:"created_at_utc"`
}

// BackupAuditEvent is returned so callers can display/save operator-safe evidence.
type BackupAuditEvent struct {
	AuditID      string `json:"audit_id"`
	Action       string `json:"action"`
	EntityType   string `json:"entity_type"`
	EntityID     string `json:"entity_id"`
	PerformedBy  string `json:"performed_by"`
	CreatedAtUTC string `json:"created_at_utc"`
	DetailsJSON  string `json:"details_json"`
	Persisted    bool   `json:"persisted"`
	PersistError string `json:"persist_error,omitempty"`
}

// BlockingJob captures job rows that prevent restore apply.
type BlockingJob struct {
	JobID        string
	State        jobs.State
	UpdatedAtUTC string
}

// SnapshotRequest controls snapshot output.
type SnapshotRequest struct {
	OutputDir   string
	PerformedBy string
}

// SnapshotResult returns immutable snapshot metadata and audit payload.
type SnapshotResult struct {
	BackupID       string
	SnapshotPath   string
	ManifestPath   string
	SnapshotSHA256 string
	SizeBytes      int64
	CreatedAtUTC   string
	AuditEvent     BackupAuditEvent
}

// RestoreValidationRequest controls dry-run validation inputs.
type RestoreValidationRequest struct {
	SnapshotPath string
	ManifestPath string
}

// RestoreValidationResult is returned for dry-run checks before apply.
type RestoreValidationResult struct {
	SnapshotPath         string
	ManifestPath         string
	ManifestPresent      bool
	ManifestMatches      bool
	SnapshotSHA256       string
	SnapshotSizeBytes    int64
	ExpectedConfirmation string
	BlockingJobs         []BlockingJob
	Valid                bool
	ApplyReady           bool
	AuditEvent           BackupAuditEvent
}

// RestoreApplyRequest controls actual restore apply behavior.
type RestoreApplyRequest struct {
	SnapshotPath string
	ManifestPath string
	Confirmation string
	PerformedBy  string
}

// RestoreApplyResult returns deterministic restore evidence metadata.
type RestoreApplyResult struct {
	Restored       bool
	DBPath         string
	SnapshotPath   string
	ManifestPath   string
	PreRestorePath string
	AppliedAtUTC   string
	SnapshotSHA256 string
	BlockingJobs   []BlockingJob
	AuditEvent     BackupAuditEvent
}

// BackupService implements snapshot, dry-run restore validation, and restore apply.
type BackupService struct {
	db     *sql.DB
	dbPath string
	writer *db.WriterGuard
	now    func() time.Time
	reopen func(context.Context, string) (*sql.DB, error)
}

// Close releases the live database handle managed by the backup service.
func (s *BackupService) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// WithBackupClock overrides the service clock for deterministic tests.
func WithBackupClock(clock func() time.Time) BackupOption {
	return func(s *BackupService) {
		if clock != nil {
			s.now = clock
		}
	}
}

// WithBackupReopen overrides the reopen function used after restore apply.
func WithBackupReopen(reopen func(context.Context, string) (*sql.DB, error)) BackupOption {
	return func(s *BackupService) {
		if reopen != nil {
			s.reopen = reopen
		}
	}
}

// NewBackupService constructs a backup/restore service on top of the active SQLite handle.
func NewBackupService(database *sql.DB, dbPath string, writer *db.WriterGuard, opts ...BackupOption) (*BackupService, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return nil, fmt.Errorf("db path is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("writer guard is nil")
	}

	svc := &BackupService{
		db:     database,
		dbPath: dbPath,
		writer: writer,
		now: func() time.Time {
			return time.Now().UTC()
		},
		reopen: db.OpenSQLite,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}

	return svc, nil
}

// ExpectedRestoreConfirmation returns the exact typed value required for restore apply.
func (s *BackupService) ExpectedRestoreConfirmation() string {
	base := strings.TrimSpace(filepath.Base(s.dbPath))
	if base == "" {
		base = "beneficiary.db"
	}
	return "RESTORE " + base
}

// CreateSnapshot creates a timestamped SQLite file snapshot plus checksum manifest.
func (s *BackupService) CreateSnapshot(ctx context.Context, req SnapshotRequest) (*SnapshotResult, error) {
	if err := s.validateReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	outputDir := strings.TrimSpace(req.OutputDir)
	if outputDir == "" {
		return nil, ErrSnapshotOutputDirRequired
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create snapshot directory: %w", err)
	}
	if err := ensureSnapshotDirWritable(outputDir); err != nil {
		return nil, err
	}

	nowUTC := s.now().UTC()
	backupID := uuid.NewString()
	stamp := compactTimestamp(nowUTC)
	snapshotFileName := fmt.Sprintf("%s-%s-%s.db", defaultBackupFilePrefix, stamp, shortID(backupID))
	snapshotPath := filepath.Join(outputDir, snapshotFileName)
	manifestPath := snapshotPath + ".manifest.json"

	err := s.writer.WithWriteLock(ctx, func() error {
		if err := s.checkpointWAL(ctx); err != nil {
			return err
		}
		return copyFile(s.dbPath, snapshotPath)
	})
	if err != nil {
		return nil, err
	}

	checksum, sizeBytes, err := checksumAndSize(snapshotPath)
	if err != nil {
		return nil, err
	}

	manifest := BackupManifest{
		ManifestVersion:   manifestVersionV1,
		BackupID:          backupID,
		SourceDBPath:      s.dbPath,
		SnapshotFile:      filepath.Base(snapshotPath),
		SnapshotSHA256:    checksum,
		SnapshotSizeBytes: sizeBytes,
		CreatedAtUTC:      nowUTC.Format(time.RFC3339Nano),
	}
	if err := writeManifest(manifestPath, manifest); err != nil {
		return nil, err
	}

	audit := s.writeAudit(
		ctx,
		"BACKUP_CREATED",
		backupID,
		strings.TrimSpace(req.PerformedBy),
		map[string]any{
			"backup_id":           backupID,
			"snapshot_path":       snapshotPath,
			"manifest_path":       manifestPath,
			"snapshot_sha256":     checksum,
			"snapshot_size_bytes": sizeBytes,
		},
	)

	return &SnapshotResult{
		BackupID:       backupID,
		SnapshotPath:   snapshotPath,
		ManifestPath:   manifestPath,
		SnapshotSHA256: checksum,
		SizeBytes:      sizeBytes,
		CreatedAtUTC:   nowUTC.Format(time.RFC3339Nano),
		AuditEvent:     audit,
	}, nil
}

// ListBlockingJobs returns job rows that must block restore apply.
func (s *BackupService) ListBlockingJobs(ctx context.Context) ([]BlockingJob, error) {
	if err := s.validateReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return listBlockingJobs(ctx, s.db)
}

// ValidateRestore performs dry-run checks without mutating the live database.
func (s *BackupService) ValidateRestore(ctx context.Context, req RestoreValidationRequest) (*RestoreValidationResult, error) {
	if err := s.validateReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	snapshotPath, err := normalizeRequiredPath(req.SnapshotPath, ErrSnapshotPathRequired)
	if err != nil {
		return nil, err
	}
	checksum, sizeBytes, err := checksumAndSize(snapshotPath)
	if err != nil {
		return nil, err
	}

	manifestPath, manifestPresent, manifest, err := resolveManifest(snapshotPath, req.ManifestPath)
	if err != nil {
		return nil, err
	}

	manifestMatches := true
	if manifestPresent {
		if err := validateManifestForSnapshot(manifest, snapshotPath, checksum, sizeBytes); err != nil {
			return nil, err
		}
	}
	if !manifestPresent {
		manifestMatches = false
	}

	blockingJobs, err := s.ListBlockingJobs(ctx)
	if err != nil {
		return nil, err
	}

	expectedConfirmation := s.ExpectedRestoreConfirmation()
	applyReady := len(blockingJobs) == 0

	audit := s.writeAudit(
		ctx,
		"RESTORE_DRY_RUN",
		filepath.Base(snapshotPath),
		"",
		map[string]any{
			"snapshot_path":         snapshotPath,
			"manifest_path":         manifestPath,
			"manifest_present":      manifestPresent,
			"manifest_matches":      manifestMatches,
			"snapshot_sha256":       checksum,
			"snapshot_size_bytes":   sizeBytes,
			"expected_confirmation": expectedConfirmation,
			"blocking_jobs":         len(blockingJobs),
		},
	)

	return &RestoreValidationResult{
		SnapshotPath:         snapshotPath,
		ManifestPath:         manifestPath,
		ManifestPresent:      manifestPresent,
		ManifestMatches:      manifestMatches,
		SnapshotSHA256:       checksum,
		SnapshotSizeBytes:    sizeBytes,
		ExpectedConfirmation: expectedConfirmation,
		BlockingJobs:         blockingJobs,
		Valid:                true,
		ApplyReady:           applyReady,
		AuditEvent:           audit,
	}, nil
}

// ApplyRestore validates and applies a restore to the live database file.
func (s *BackupService) ApplyRestore(ctx context.Context, req RestoreApplyRequest) (*RestoreApplyResult, error) {
	if err := s.validateReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if strings.TrimSpace(req.Confirmation) == "" {
		return nil, ErrRestoreConfirmationRequired
	}
	if strings.TrimSpace(req.Confirmation) != s.ExpectedRestoreConfirmation() {
		return nil, ErrRestoreConfirmationMismatch
	}

	validation, err := s.ValidateRestore(ctx, RestoreValidationRequest{
		SnapshotPath: req.SnapshotPath,
		ManifestPath: req.ManifestPath,
	})
	if err != nil {
		return nil, err
	}
	if !validation.ApplyReady {
		return nil, fmt.Errorf("%w: %d blocking job(s)", ErrRestoreBlockedActiveJobs, len(validation.BlockingJobs))
	}

	nowUTC := s.now().UTC()
	rollbackPath := filepath.Join(filepath.Dir(s.dbPath), fmt.Sprintf("pre-restore-%s.db", compactTimestamp(nowUTC)))

	err = s.writer.WithWriteLock(ctx, func() error {
		blockingJobs, listErr := listBlockingJobs(ctx, s.db)
		if listErr != nil {
			return listErr
		}
		if len(blockingJobs) > 0 {
			return fmt.Errorf("%w: %d blocking job(s)", ErrRestoreBlockedActiveJobs, len(blockingJobs))
		}

		if err := s.checkpointWAL(ctx); err != nil {
			return err
		}

		if err := copyFile(s.dbPath, rollbackPath); err != nil {
			return fmt.Errorf("create pre-restore snapshot: %w", err)
		}

		if s.db != nil {
			if err := s.db.Close(); err != nil {
				return fmt.Errorf("close live db before restore: %w", err)
			}
			s.db = nil
		}

		tempPath := s.dbPath + ".restore.tmp"
		_ = os.Remove(tempPath)
		if err := copyFile(validation.SnapshotPath, tempPath); err != nil {
			_ = reopenAfterFailure(ctx, s, rollbackPath)
			return fmt.Errorf("prepare restored db copy: %w", err)
		}

		if err := replaceFile(tempPath, s.dbPath); err != nil {
			_ = restoreFromRollbackPath(s.dbPath, rollbackPath)
			_ = reopenAfterFailure(ctx, s, rollbackPath)
			return fmt.Errorf("replace live db with restored snapshot: %w", err)
		}

		reopened, err := s.reopen(ctx, s.dbPath)
		if err != nil {
			_ = restoreFromRollbackPath(s.dbPath, rollbackPath)
			_ = reopenAfterFailure(ctx, s, rollbackPath)
			return fmt.Errorf("reopen restored db: %w", err)
		}
		s.db = reopened
		return nil
	})
	if err != nil {
		return nil, err
	}

	audit := s.writeAudit(
		ctx,
		"RESTORE_APPLIED",
		filepath.Base(validation.SnapshotPath),
		strings.TrimSpace(req.PerformedBy),
		map[string]any{
			"snapshot_path":       validation.SnapshotPath,
			"manifest_path":       validation.ManifestPath,
			"pre_restore_path":    rollbackPath,
			"snapshot_sha256":     validation.SnapshotSHA256,
			"snapshot_size_bytes": validation.SnapshotSizeBytes,
			"db_path":             s.dbPath,
		},
	)

	return &RestoreApplyResult{
		Restored:       true,
		DBPath:         s.dbPath,
		SnapshotPath:   validation.SnapshotPath,
		ManifestPath:   validation.ManifestPath,
		PreRestorePath: rollbackPath,
		AppliedAtUTC:   nowUTC.Format(time.RFC3339Nano),
		SnapshotSHA256: validation.SnapshotSHA256,
		BlockingJobs:   nil,
		AuditEvent:     audit,
	}, nil
}

func (s *BackupService) validateReady() error {
	if s == nil {
		return fmt.Errorf("backup service is nil")
	}
	if s.db == nil {
		return fmt.Errorf("database is nil")
	}
	if strings.TrimSpace(s.dbPath) == "" {
		return fmt.Errorf("db path is required")
	}
	if s.writer == nil {
		return fmt.Errorf("writer guard is nil")
	}
	if s.now == nil {
		return fmt.Errorf("clock is nil")
	}
	if s.reopen == nil {
		return fmt.Errorf("reopen callback is nil")
	}
	return nil
}

func (s *BackupService) checkpointWAL(ctx context.Context) error {
	if s.db == nil {
		return fmt.Errorf("database is nil")
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);"); err != nil {
		return fmt.Errorf("checkpoint sqlite wal: %w", err)
	}
	return nil
}

func (s *BackupService) writeAudit(ctx context.Context, action, entityID, performedBy string, details map[string]any) BackupAuditEvent {
	nowUTC := s.now().UTC().Format(time.RFC3339Nano)
	safeDetails := sanitizeBackupAuditDetails(details)
	payload, _ := json.Marshal(safeDetails)
	detailsJSON := string(payload)

	audit := BackupAuditEvent{
		AuditID:      uuid.NewString(),
		Action:       strings.TrimSpace(action),
		EntityType:   "DATABASE_BACKUP",
		EntityID:     strings.TrimSpace(entityID),
		PerformedBy:  normalizeActor(performedBy),
		CreatedAtUTC: nowUTC,
		DetailsJSON:  detailsJSON,
	}

	if s.db == nil || s.writer == nil {
		audit.Persisted = false
		audit.PersistError = "audit persistence unavailable"
		return audit
	}

	err := s.writer.WithWriteTx(ctx, s.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
INSERT INTO audit_logs (
    audit_id,
    entity_type,
    entity_id,
    action,
    performed_by,
    details_json,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?);
`, audit.AuditID, audit.EntityType, audit.EntityID, audit.Action, audit.PerformedBy, detailsJSON, audit.CreatedAtUTC)
		return err
	})
	if err != nil {
		audit.Persisted = false
		audit.PersistError = err.Error()
		return audit
	}

	audit.Persisted = true
	return audit
}

func normalizeActor(performedBy string) string {
	performedBy = strings.TrimSpace(performedBy)
	if performedBy == "" {
		return "system"
	}
	return performedBy
}

func ensureSnapshotDirWritable(path string) error {
	path = filepath.Clean(path)

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect snapshot directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("snapshot output path is not a directory")
	}

	probeFile, err := os.CreateTemp(path, ".snapshot-write-check-*")
	if err != nil {
		return fmt.Errorf("snapshot output directory is not writable: %w", err)
	}
	probePath := probeFile.Name()
	if err := probeFile.Close(); err != nil {
		_ = os.Remove(probePath)
		return fmt.Errorf("close snapshot directory write check file: %w", err)
	}
	if err := os.Remove(probePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cleanup snapshot directory write check file: %w", err)
	}

	return nil
}

func sanitizeBackupAuditDetails(details map[string]any) map[string]any {
	if len(details) == 0 {
		return details
	}

	safeDetails := make(map[string]any, len(details))
	for key, value := range details {
		if key == "expected_confirmation" {
			safeDetails[key] = redactedPathValue
			continue
		}
		if _, shouldSanitize := backupAuditPathKeys[key]; shouldSanitize {
			safeDetails[key] = sanitizeAuditPathValue(value)
			continue
		}
		safeDetails[key] = value
	}

	return safeDetails
}

func sanitizeAuditPathValue(value any) any {
	pathValue, ok := value.(string)
	if !ok {
		return value
	}

	pathValue = strings.TrimSpace(pathValue)
	if pathValue == "" {
		return ""
	}

	base := portablePathBase(pathValue)
	if base == "" {
		return redactedPathValue
	}
	return base
}

func portablePathBase(value string) string {
	normalized := strings.ReplaceAll(strings.TrimSpace(value), "\\", "/")
	if normalized == "" {
		return ""
	}

	base := path.Base(path.Clean(normalized))
	if base == "." || base == "/" {
		return ""
	}
	return base
}

func listBlockingJobs(ctx context.Context, database *sql.DB) ([]BlockingJob, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}

	rows, err := database.QueryContext(ctx, `
SELECT job_id, state, updated_at_utc
FROM job_states
WHERE state IN (?, ?, ?)
ORDER BY updated_at_utc ASC, job_id ASC;
`, string(jobs.StateRunning), string(jobs.StateCancelRequested), string(jobs.StateRecoverable))
	if err != nil {
		return nil, fmt.Errorf("list blocking jobs: %w", err)
	}
	defer rows.Close()

	items := make([]BlockingJob, 0)
	for rows.Next() {
		var (
			item     BlockingJob
			rawState string
		)
		if err := rows.Scan(&item.JobID, &rawState, &item.UpdatedAtUTC); err != nil {
			return nil, fmt.Errorf("scan blocking job: %w", err)
		}
		item.State = jobs.State(strings.TrimSpace(rawState))
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate blocking jobs: %w", err)
	}
	return items, nil
}

func normalizeRequiredPath(path string, errRequired error) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errRequired
	}
	return filepath.Clean(path), nil
}

func resolveManifest(snapshotPath, providedPath string) (string, bool, BackupManifest, error) {
	providedPath = strings.TrimSpace(providedPath)
	if providedPath != "" {
		manifestPath := filepath.Clean(providedPath)
		manifest, err := readManifest(manifestPath)
		if err != nil {
			return "", false, BackupManifest{}, err
		}
		return manifestPath, true, manifest, nil
	}

	candidate := snapshotPath + ".manifest.json"
	if _, err := os.Stat(candidate); err == nil {
		manifest, err := readManifest(candidate)
		if err != nil {
			return "", false, BackupManifest{}, err
		}
		return candidate, true, manifest, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, BackupManifest{}, fmt.Errorf("inspect manifest candidate: %w", err)
	}

	return "", false, BackupManifest{}, nil
}

func validateManifestForSnapshot(manifest BackupManifest, snapshotPath, checksum string, sizeBytes int64) error {
	if strings.TrimSpace(manifest.ManifestVersion) == "" {
		return fmt.Errorf("manifest version is required")
	}
	if strings.TrimSpace(manifest.SnapshotFile) != filepath.Base(snapshotPath) {
		return fmt.Errorf("manifest snapshot_file does not match snapshot path")
	}
	if strings.TrimSpace(manifest.SnapshotSHA256) != checksum {
		return fmt.Errorf("manifest checksum mismatch")
	}
	if manifest.SnapshotSizeBytes != sizeBytes {
		return fmt.Errorf("manifest snapshot size mismatch")
	}
	return nil
}

func checksumAndSize(path string) (string, int64, error) {
	path = filepath.Clean(path)
	file, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("open file for checksum: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return "", 0, fmt.Errorf("hash file: %w", err)
	}
	if size <= 0 {
		return "", 0, fmt.Errorf("file is empty: %s", path)
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}

func writeManifest(path string, manifest BackupManifest) error {
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	body = append(body, '\n')

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create manifest directory: %w", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

func readManifest(path string) (BackupManifest, error) {
	body, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return BackupManifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var manifest BackupManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return BackupManifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return manifest, nil
}

func copyFile(srcPath, dstPath string) error {
	srcPath = filepath.Clean(srcPath)
	dstPath = filepath.Clean(dstPath)

	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open destination file: %w", err)
	}

	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return fmt.Errorf("copy file bytes: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return fmt.Errorf("sync destination file: %w", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("close destination file: %w", err)
	}
	return nil
}

func replaceFile(srcPath, dstPath string) error {
	srcPath = filepath.Clean(srcPath)
	dstPath = filepath.Clean(dstPath)

	if _, err := os.Stat(dstPath); err == nil {
		if err := os.Remove(dstPath); err != nil {
			return fmt.Errorf("remove existing destination file: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect destination file before replace: %w", err)
	}

	if err := os.Rename(srcPath, dstPath); err != nil {
		return fmt.Errorf("rename replacement file: %w", err)
	}
	return nil
}

func restoreFromRollbackPath(dbPath, rollbackPath string) error {
	tempPath := dbPath + ".rollback.tmp"
	_ = os.Remove(tempPath)
	if err := copyFile(rollbackPath, tempPath); err != nil {
		return err
	}
	return replaceFile(tempPath, dbPath)
}

func reopenAfterFailure(ctx context.Context, s *BackupService, rollbackPath string) error {
	if s == nil || s.reopen == nil {
		return nil
	}

	reopened, err := s.reopen(ctx, s.dbPath)
	if err == nil {
		s.db = reopened
		return nil
	}

	if rollbackErr := restoreFromRollbackPath(s.dbPath, rollbackPath); rollbackErr != nil {
		return rollbackErr
	}

	reopened, reopenErr := s.reopen(ctx, s.dbPath)
	if reopenErr != nil {
		return reopenErr
	}
	s.db = reopened
	return nil
}

func compactTimestamp(value time.Time) string {
	value = value.UTC()
	return fmt.Sprintf(
		"%s%09dZ",
		value.Format("20060102T150405"),
		value.Nanosecond(),
	)
}

func shortID(id string) string {
	id = strings.ReplaceAll(strings.TrimSpace(id), "-", "")
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}
