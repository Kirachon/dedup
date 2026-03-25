package repository

import (
	"context"
	"database/sql"
	"fmt"

	"dedup/internal/db"
	"dedup/internal/model"
)

type sqlQueryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Repository provides SQL-backed access to the frozen SQLite schema.
type Repository struct {
	db     *sql.DB
	tx     *sql.Tx
	writer *db.WriterGuard
}

// New constructs a writable repository backed by the given database and write guard.
func New(database *sql.DB, writer *db.WriterGuard) (*Repository, error) {
	if database == nil {
		return nil, fmt.Errorf("database is nil")
	}
	if writer == nil {
		return nil, fmt.Errorf("writer guard is nil")
	}

	return &Repository{db: database, writer: writer}, nil
}

// WithinTx runs fn inside a write transaction guarded by the single-writer lock.
func (r *Repository) WithinTx(ctx context.Context, fn func(*Repository) error) error {
	return r.write(ctx, fn)
}

func (r *Repository) queryer() (sqlQueryer, error) {
	if r == nil {
		return nil, fmt.Errorf("repository is nil")
	}
	if r.tx != nil {
		return r.tx, nil
	}
	if r.db == nil {
		return nil, fmt.Errorf("database is nil")
	}

	return r.db, nil
}

func (r *Repository) write(ctx context.Context, fn func(*Repository) error) error {
	if r == nil {
		return fmt.Errorf("repository is nil")
	}
	if fn == nil {
		return fmt.Errorf("write callback is nil")
	}

	if r.tx != nil {
		return fn(r)
	}
	if r.db == nil {
		return fmt.Errorf("database is nil")
	}
	if r.writer == nil {
		return fmt.Errorf("writer guard is nil")
	}

	return r.writer.WithWriteTx(ctx, r.db, func(tx *sql.Tx) error {
		return fn(&Repository{
			db:     r.db,
			tx:     tx,
			writer: r.writer,
		})
	})
}

// BeneficiaryListQuery controls filtering and paging for beneficiary reads.
type BeneficiaryListQuery struct {
	Search         string
	RecordStatus   string
	DedupStatus    string
	SourceType     string
	RegionCode     string
	ProvinceCode   string
	CityCode       string
	BarangayCode   string
	IncludeDeleted bool
	Limit          int
	Offset         int
}

// BeneficiaryPage is the deterministic paged result for beneficiary reads.
type BeneficiaryPage = model.BeneficiaryPage

// BeneficiaryDuplicateLookup captures the normalized fields used for duplicate prechecks.
type BeneficiaryDuplicateLookup struct {
	ExcludeInternalUUID string
	NormLastName        string
	NormFirstName       string
	NormMiddleName      string
	NormExtensionName   string
	BirthdateISO        string
	ContactNoNorm       string
	RegionCode          string
	ProvinceCode        string
	CityCode            string
	BarangayCode        string
	Sex                 string
	IncludeDeleted      bool
	Limit               int
}

// AuditLogQuery controls filtered audit log reads.
type AuditLogQuery struct {
	EntityType  string
	EntityID    string
	Action      string
	PerformedBy string
	Limit       int
	Offset      int
}

// DedupRunListQuery controls filtered dedup run reads.
type DedupRunListQuery struct {
	Status string
	Limit  int
	Offset int
}

// DedupMatchListQuery controls filtered dedup match reads.
type DedupMatchListQuery struct {
	RunID          string
	DecisionStatus string
	Limit          int
	Offset         int
}

// DedupDecisionListQuery controls filtered dedup decision reads.
type DedupDecisionListQuery struct {
	Limit  int
	Offset int
}

// ImportLogListQuery controls filtered import log reads.
type ImportLogListQuery struct {
	SourceType string
	Status     string
	Limit      int
	Offset     int
}

// ExportLogListQuery controls filtered export log reads.
type ExportLogListQuery struct {
	ExportType string
	Limit      int
	Offset     int
}

// LocationNormalizationRunListQuery controls filtered run history reads.
type LocationNormalizationRunListQuery struct {
	ImportID string
	Status   string
	Mode     string
	Limit    int
	Offset   int
}

// LocationNormalizationItemListQuery controls filtered item history reads.
type LocationNormalizationItemListQuery struct {
	RunID       string
	Status      string
	NeedsReview *bool
	Limit       int
	Offset      int
}

// BeneficiaryRepository groups beneficiary operations.
type BeneficiaryRepository interface {
	CreateBeneficiary(context.Context, *model.Beneficiary) error
	UpdateBeneficiary(context.Context, *model.Beneficiary) error
	SoftDeleteBeneficiary(context.Context, string, string) error
	GetBeneficiary(context.Context, string) (*model.Beneficiary, error)
	GetBeneficiaryByGeneratedID(context.Context, string) (*model.Beneficiary, error)
	ListBeneficiaries(context.Context, BeneficiaryListQuery) (BeneficiaryPage, error)
	FindDuplicateBeneficiaries(context.Context, BeneficiaryDuplicateLookup) ([]model.Beneficiary, error)
}

// SettingsRepository groups app setting operations.
type SettingsRepository interface {
	UpsertSetting(context.Context, *model.AppSetting) error
	GetSetting(context.Context, string) (*model.AppSetting, error)
	ListSettings(context.Context) ([]model.AppSetting, error)
}

// AuditRepository groups audit log operations.
type AuditRepository interface {
	CreateAuditLog(context.Context, *model.AuditLog) error
	GetAuditLog(context.Context, string) (*model.AuditLog, error)
	ListAuditLogs(context.Context, AuditLogQuery) ([]model.AuditLog, error)
}

// DedupRepository groups dedup run, match, and decision operations.
type DedupRepository interface {
	CreateDedupRun(context.Context, *model.DedupRun) error
	UpdateDedupRun(context.Context, *model.DedupRun) error
	GetDedupRun(context.Context, string) (*model.DedupRun, error)
	ListDedupRuns(context.Context, DedupRunListQuery) ([]model.DedupRun, error)
	CreateDedupMatch(context.Context, *model.DedupMatch) error
	UpdateDedupMatchDecisionStatus(context.Context, string, string) error
	GetDedupMatch(context.Context, string) (*model.DedupMatch, error)
	ListDedupMatchesByRun(context.Context, string) ([]model.DedupMatch, error)
	CreateDedupDecision(context.Context, *model.DedupDecision) error
	UpdateDedupDecision(context.Context, *model.DedupDecision) error
	GetDedupDecision(context.Context, string) (*model.DedupDecision, error)
	GetDedupDecisionByPairKey(context.Context, string) (*model.DedupDecision, error)
	ListDedupDecisions(context.Context, DedupDecisionListQuery) ([]model.DedupDecision, error)
}

// LogRepository groups import and export log operations.
type LogRepository interface {
	CreateImportLog(context.Context, *model.ImportLog) error
	UpdateImportLog(context.Context, *model.ImportLog) error
	GetImportLog(context.Context, string) (*model.ImportLog, error)
	ListImportLogs(context.Context, ImportLogListQuery) ([]model.ImportLog, error)
	CreateExportLog(context.Context, *model.ExportLog) error
	UpdateExportLog(context.Context, *model.ExportLog) error
	GetExportLog(context.Context, string) (*model.ExportLog, error)
	ListExportLogs(context.Context, ExportLogListQuery) ([]model.ExportLog, error)
}

// LocationNormalizationRepository groups run/item lineage operations.
type LocationNormalizationRepository interface {
	CreateLocationNormalizationRun(context.Context, *model.LocationNormalizationRun) error
	UpdateLocationNormalizationRun(context.Context, *model.LocationNormalizationRun) error
	GetLocationNormalizationRun(context.Context, string) (*model.LocationNormalizationRun, error)
	ListLocationNormalizationRuns(context.Context, LocationNormalizationRunListQuery) ([]model.LocationNormalizationRun, error)
	CreateLocationNormalizationItem(context.Context, *model.LocationNormalizationItem) error
	GetLocationNormalizationItem(context.Context, string) (*model.LocationNormalizationItem, error)
	ListLocationNormalizationItems(context.Context, LocationNormalizationItemListQuery) ([]model.LocationNormalizationItem, error)
}

// PSGCRepository groups read helpers for PSGC rows.
type PSGCRepository interface {
	GetRegion(context.Context, string) (*model.PSGCRegion, error)
	ListRegions(context.Context) ([]model.PSGCRegion, error)
	GetProvince(context.Context, string) (*model.PSGCProvince, error)
	ListProvincesByRegion(context.Context, string) ([]model.PSGCProvince, error)
	GetCity(context.Context, string) (*model.PSGCCity, error)
	ListCitiesByRegion(context.Context, string) ([]model.PSGCCity, error)
	ListCitiesByProvince(context.Context, string) ([]model.PSGCCity, error)
	GetBarangay(context.Context, string) (*model.PSGCBarangay, error)
	ListBarangaysByCity(context.Context, string) ([]model.PSGCBarangay, error)
	GetIngestMetadata(context.Context) (*model.PSGCIngestMetadata, error)
}

// RepositoryAPI is the stable composite surface exposed by this package.
type RepositoryAPI interface {
	BeneficiaryRepository
	SettingsRepository
	AuditRepository
	DedupRepository
	LogRepository
	LocationNormalizationRepository
	PSGCRepository
	WithinTx(context.Context, func(*Repository) error) error
}
