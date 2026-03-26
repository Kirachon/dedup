package importer

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"dedup/internal/locationnorm"
	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"

	"github.com/google/uuid"
)

const (
	importStatusRunning   = "RUNNING"
	importStatusPartial   = "PARTIAL"
	importStatusSucceeded = "SUCCEEDED"
	importStatusResumed   = "RESUMED"
	importStatusFailed    = "FAILED"

	importTokenStagePreview    = "preview"
	importTokenStageCheckpoint = "checkpoint"

	packageManifestFileName   = "manifest.json"
	packageBeneficiariesName  = "beneficiaries.csv"
	packageChecksumsFileName  = "checksums.txt"
	packageExportMetaFileName = "export_meta.json"

	// Rows that are not auto-applied can still be imported when the normalizer
	// resolves a complete PSGC chain with enough confidence to keep the batch moving.
	importReviewAcceptanceThreshold = 0.90
)

// Importer orchestrates preview, commit, and resume for offline beneficiary imports.
type Importer struct {
	repo            importRepository
	creator         beneficiaryCreator
	now             func() time.Time
	commitRowBudget int
	catalogMu       sync.Mutex
	catalog         *psgcCatalog
	normalizerMu    sync.Mutex
	normalizer      *locationnorm.LocationNormalizer
}

// ImportToken stores the stable source details used for preview/commit/resume.
type ImportToken struct {
	Stage                string             `json:"stage"`
	SourceType           model.ImportSource `json:"source_type"`
	SourcePath           string             `json:"source_path"`
	SourceHash           string             `json:"source_hash"`
	SourceReference      string             `json:"source_reference"`
	FileName             string             `json:"file_name"`
	OperatorName         string             `json:"operator_name"`
	CreatedAtUTC         string             `json:"created_at_utc"`
	ImportID             string             `json:"import_id,omitempty"`
	IdempotencyKey       string             `json:"idempotency_key,omitempty"`
	NextRow              int                `json:"next_row,omitempty"`
	RowsRead             int                `json:"rows_read,omitempty"`
	RowsInserted         int                `json:"rows_inserted,omitempty"`
	RowsSkipped          int                `json:"rows_skipped,omitempty"`
	RowsFailed           int                `json:"rows_failed,omitempty"`
	NormalizationVersion string             `json:"normalization_version,omitempty"`
	NormalizationHash    string             `json:"normalization_hash,omitempty"`
}

type sourceDocument struct {
	SourceType      model.ImportSource
	SourcePath      string
	SourceHash      string
	SourceReference string
	FileName        string
	CSVBytes        []byte
	PackageManifest *exchangePackageManifest
}

type exchangePackageManifest struct {
	SpecVersion       string `json:"spec_version"`
	PackageID         string `json:"package_id"`
	CreatedAtUTC      string `json:"created_at_utc"`
	SourceLGUName     string `json:"source_lgu_name"`
	SourceSystemName  string `json:"source_system_name"`
	RowsDeclared      int    `json:"rows_declared"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
}

type commitState struct {
	importID        string
	idempotencyKey  string
	preview         ImportToken
	document        sourceDocument
	startRow        int
	commitRowBudget int
	rowsRead        int
	rowsInserted    int
	rowsSkipped     int
	rowsFailed      int
	nextRow         int
}

type normalizationLedgerRepository interface {
	GetLocationNormalizationRun(ctx context.Context, runID string) (*model.LocationNormalizationRun, error)
	CreateLocationNormalizationRun(ctx context.Context, run *model.LocationNormalizationRun) error
	UpdateLocationNormalizationRun(ctx context.Context, run *model.LocationNormalizationRun) error
	CreateLocationNormalizationItem(ctx context.Context, item *model.LocationNormalizationItem) error
}

type normalizationReviewRequiredError struct {
	result model.LocationNormalizationResult
}

func (e *normalizationReviewRequiredError) Error() string {
	reason := strings.TrimSpace(e.result.Reason)
	if reason == "" {
		reason = "location normalization needs operator review"
	}
	return fmt.Sprintf(
		"location normalization review required (%s, confidence=%.2f): %s",
		e.result.MatchSource,
		e.result.Confidence,
		reason,
	)
}

type psgcCatalog struct {
	regions           []model.PSGCRegion
	provincesByRegion map[string][]model.PSGCProvince
	citiesByProvince  map[string][]model.PSGCCity
	barangaysByCity   map[string][]model.PSGCBarangay
}

// New constructs an importer service on top of the repository and beneficiary service surfaces.
func New(repo importRepository, creator beneficiaryCreator, opts ...Option) (*Importer, error) {
	if repo == nil {
		return nil, fmt.Errorf("import repository is nil")
	}
	if creator == nil {
		return nil, fmt.Errorf("beneficiary creator is nil")
	}

	imp := &Importer{
		repo:    repo,
		creator: creator,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(imp)
		}
	}

	return imp, nil
}

// NewImporter is an explicit alias for New.
func NewImporter(repo importRepository, creator beneficiaryCreator, opts ...Option) (*Importer, error) {
	return New(repo, creator, opts...)
}

// Preview validates a source and returns a durable preview token.
func (i *Importer) Preview(ctx context.Context, source Source) (*PreviewReport, error) {
	if err := i.validateReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	doc, err := i.loadSourceDocument(ctx, source)
	if err != nil {
		return nil, err
	}
	normalizationVersion, normalizationHash, err := i.computeNormalizationMetadata(ctx)
	if err != nil {
		return nil, err
	}

	stats, sampleErrors, err := i.scanSource(ctx, doc, 0, false)
	if err != nil {
		return nil, err
	}
	if doc.PackageManifest != nil && doc.PackageManifest.RowsDeclared != stats.Total {
		return nil, fmt.Errorf("package row count mismatch: manifest declared %d rows, source contains %d", doc.PackageManifest.RowsDeclared, stats.Total)
	}

	token, err := encodeImportToken(ImportToken{
		Stage:                importTokenStagePreview,
		SourceType:           doc.SourceType,
		SourcePath:           doc.SourcePath,
		SourceHash:           doc.SourceHash,
		SourceReference:      doc.SourceReference,
		FileName:             doc.FileName,
		OperatorName:         strings.TrimSpace(source.OperatorName),
		CreatedAtUTC:         i.now().UTC().Format(time.RFC3339Nano),
		NormalizationVersion: normalizationVersion,
		NormalizationHash:    normalizationHash,
	})
	if err != nil {
		return nil, err
	}

	return &PreviewReport{
		PreviewToken:            token,
		SourceType:              doc.SourceType,
		SourceHash:              doc.SourceHash,
		RowCountTotal:           stats.Total,
		RowCountValid:           stats.Valid,
		RowCountInvalid:         stats.Invalid,
		HeaderValidationPassed:  true,
		SampleErrors:            sampleErrors,
		GeneratedAtUTC:          i.now().UTC().Format(time.RFC3339Nano),
		DetectedSourceReference: doc.SourceReference,
	}, nil
}

// Commit starts a new import run using a preview token and idempotency key.
func (i *Importer) Commit(ctx context.Context, previewToken, idempotencyKey string) (*ImportResult, error) {
	if err := i.validateReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey == "" {
		return nil, fmt.Errorf("idempotency key is required")
	}

	preview, err := decodeImportToken(previewToken)
	if err != nil {
		return nil, fmt.Errorf("decode preview token: %w", err)
	}
	if preview.Stage != importTokenStagePreview {
		return nil, fmt.Errorf("preview token stage mismatch: %s", preview.Stage)
	}
	if err := i.validateNormalizationMetadata(ctx, preview); err != nil {
		return nil, err
	}

	existing, err := i.findExistingImport(ctx, preview.SourceHash, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return importResultFromLog(existing), nil
	}

	doc, err := i.loadSourceDocumentFromToken(ctx, preview)
	if err != nil {
		return nil, err
	}

	state := commitState{
		importID:        uuid.NewString(),
		idempotencyKey:  idempotencyKey,
		preview:         preview,
		document:        doc,
		startRow:        1,
		commitRowBudget: i.commitRowBudget,
	}

	result, err := i.runImport(ctx, state, false)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Resume continues a partial import using a checkpoint token.
func (i *Importer) Resume(ctx context.Context, checkpointToken string) (*ImportResult, error) {
	if err := i.validateReady(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	checkpoint, err := decodeImportToken(checkpointToken)
	if err != nil {
		return nil, fmt.Errorf("decode checkpoint token: %w", err)
	}
	if checkpoint.Stage != importTokenStageCheckpoint {
		return nil, fmt.Errorf("checkpoint token stage mismatch: %s", checkpoint.Stage)
	}
	if checkpoint.ImportID == "" {
		return nil, fmt.Errorf("checkpoint token missing import id")
	}
	if err := i.validateNormalizationMetadata(ctx, checkpoint); err != nil {
		return nil, err
	}

	doc, err := i.loadSourceDocumentFromToken(ctx, checkpoint)
	if err != nil {
		return nil, err
	}

	log, err := i.getImportLogByID(ctx, checkpoint.ImportID)
	if err != nil {
		return nil, err
	}
	if log == nil {
		return nil, fmt.Errorf("import log not found for checkpoint %s", checkpoint.ImportID)
	}
	if log.FileHash != nil && *log.FileHash != checkpoint.SourceHash {
		return nil, fmt.Errorf("checkpoint hash mismatch")
	}

	state := commitState{
		importID:        checkpoint.ImportID,
		idempotencyKey:  checkpoint.IdempotencyKey,
		preview:         ImportToken{Stage: importTokenStagePreview, SourceType: checkpoint.SourceType, SourcePath: checkpoint.SourcePath, SourceHash: checkpoint.SourceHash, SourceReference: checkpoint.SourceReference, FileName: checkpoint.FileName, OperatorName: checkpoint.OperatorName, CreatedAtUTC: checkpoint.CreatedAtUTC, NormalizationVersion: checkpoint.NormalizationVersion, NormalizationHash: checkpoint.NormalizationHash},
		document:        doc,
		startRow:        checkpoint.NextRow,
		commitRowBudget: i.commitRowBudget,
		rowsRead:        checkpoint.RowsRead,
		rowsInserted:    checkpoint.RowsInserted,
		rowsSkipped:     checkpoint.RowsSkipped,
		rowsFailed:      checkpoint.RowsFailed,
		nextRow:         checkpoint.NextRow,
	}

	result, err := i.runImport(ctx, state, true)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (i *Importer) validateReady() error {
	if i == nil {
		return fmt.Errorf("importer is nil")
	}
	if i.repo == nil {
		return fmt.Errorf("import repository is nil")
	}
	if i.creator == nil {
		return fmt.Errorf("beneficiary creator is nil")
	}
	if i.now == nil {
		return fmt.Errorf("clock is nil")
	}
	return nil
}

func (i *Importer) loadSourceDocumentFromToken(ctx context.Context, token ImportToken) (sourceDocument, error) {
	source := Source{
		Type:            token.SourceType,
		Path:            token.SourcePath,
		OperatorName:    token.OperatorName,
		SourceReference: token.SourceReference,
	}
	return i.loadSourceDocument(ctx, source)
}

func (i *Importer) loadSourceDocument(ctx context.Context, source Source) (sourceDocument, error) {
	source.Path = strings.TrimSpace(source.Path)
	if source.Path == "" {
		return sourceDocument{}, fmt.Errorf("source path is required")
	}

	switch source.Type {
	case model.ImportSourceCSV:
		return loadCSVDocument(source)
	case model.ImportSourceExchangePackage:
		return loadPackageDocument(source)
	default:
		return sourceDocument{}, fmt.Errorf("unsupported source type: %s", source.Type)
	}
}

func loadCSVDocument(source Source) (sourceDocument, error) {
	body, err := os.ReadFile(filepath.Clean(source.Path))
	if err != nil {
		return sourceDocument{}, fmt.Errorf("read csv source: %w", err)
	}

	hash := sha256.Sum256(body)
	sourceReference := strings.TrimSpace(source.SourceReference)
	if sourceReference == "" {
		sourceReference = filepath.Base(source.Path)
	}

	return sourceDocument{
		SourceType:      source.Type,
		SourcePath:      source.Path,
		SourceHash:      hex.EncodeToString(hash[:]),
		SourceReference: sourceReference,
		FileName:        filepath.Base(source.Path),
		CSVBytes:        body,
	}, nil
}

func loadPackageDocument(source Source) (sourceDocument, error) {
	body, err := os.ReadFile(filepath.Clean(source.Path))
	if err != nil {
		return sourceDocument{}, fmt.Errorf("read package source: %w", err)
	}

	hash := sha256.Sum256(body)
	reader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return sourceDocument{}, fmt.Errorf("open package archive: %w", err)
	}

	entries := map[string]int{}
	files := map[string][]byte{}
	for _, file := range reader.File {
		name := filepath.ToSlash(strings.TrimSpace(file.Name))
		if name == "" {
			return sourceDocument{}, fmt.Errorf("package contains empty file name")
		}
		if strings.Contains(name, "..") || filepath.IsAbs(name) {
			return sourceDocument{}, fmt.Errorf("package contains forbidden path: %s", name)
		}
		entries[name]++
		if entries[name] > 1 {
			return sourceDocument{}, fmt.Errorf("package contains duplicate file: %s", name)
		}

		handle, err := file.Open()
		if err != nil {
			return sourceDocument{}, fmt.Errorf("open package file %s: %w", name, err)
		}
		content, readErr := io.ReadAll(handle)
		_ = handle.Close()
		if readErr != nil {
			return sourceDocument{}, fmt.Errorf("read package file %s: %w", name, readErr)
		}
		files[name] = content
	}

	required := []string{
		packageManifestFileName,
		packageBeneficiariesName,
		packageChecksumsFileName,
		packageExportMetaFileName,
	}
	for _, name := range required {
		if entries[name] != 1 {
			return sourceDocument{}, fmt.Errorf("package missing required file: %s", name)
		}
	}

	manifest, err := parsePackageManifest(files[packageManifestFileName])
	if err != nil {
		return sourceDocument{}, err
	}
	if manifest.SpecVersion != "v1" {
		return sourceDocument{}, fmt.Errorf("unsupported package spec version: %s", manifest.SpecVersion)
	}
	if strings.TrimSpace(manifest.ChecksumAlgorithm) != "sha256" {
		return sourceDocument{}, fmt.Errorf("unsupported package checksum algorithm: %s", manifest.ChecksumAlgorithm)
	}
	if manifest.RowsDeclared < 0 {
		return sourceDocument{}, fmt.Errorf("manifest rows_declared must be >= 0")
	}

	checksums, err := parseChecksumList(files[packageChecksumsFileName])
	if err != nil {
		return sourceDocument{}, err
	}
	if err := verifyPackageChecksums(checksums, files); err != nil {
		return sourceDocument{}, err
	}

	sourceReference := strings.TrimSpace(source.SourceReference)
	if sourceReference == "" {
		sourceReference = strings.TrimSpace(manifest.PackageID)
	}
	if sourceReference == "" {
		sourceReference = filepath.Base(source.Path)
	}

	return sourceDocument{
		SourceType:      source.Type,
		SourcePath:      source.Path,
		SourceHash:      hex.EncodeToString(hash[:]),
		SourceReference: sourceReference,
		FileName:        filepath.Base(source.Path),
		CSVBytes:        files[packageBeneficiariesName],
		PackageManifest: &manifest,
	}, nil
}

func (i *Importer) scanSource(ctx context.Context, doc sourceDocument, startRow int, commitMode bool) (scanStats, []string, error) {
	reader := csv.NewReader(bytes.NewReader(doc.CSVBytes))
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return scanStats{}, nil, fmt.Errorf("read import header: %w", err)
	}
	if err := validateImportHeader(header); err != nil {
		return scanStats{}, nil, err
	}

	stats := scanStats{}
	sampleErrors := make([]string, 0, maxSampleErrors)
	rowIndex := 0

	for {
		if err := ctx.Err(); err != nil {
			return stats, sampleErrors, err
		}

		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return stats, sampleErrors, fmt.Errorf("read source row %d: %w", rowIndex+2, err)
		}

		rowIndex++
		if rowIndex < startRow {
			continue
		}

		stats.Total++

		_, _, _, _, validationErr := i.buildDraftFromRecord(ctx, doc, record, rowIndex)
		if validationErr != nil {
			stats.Invalid++
			if len(sampleErrors) < maxSampleErrors {
				sampleErrors = append(sampleErrors, fmt.Sprintf("row %d: %v", rowIndex, validationErr))
			}
			continue
		}

		stats.Valid++
	}

	if commitMode && startRow > 0 {
		// no-op; commit mode uses row processing in runImport
	}

	return stats, sampleErrors, nil
}

type scanStats struct {
	Total   int
	Valid   int
	Invalid int
}

func (i *Importer) runImport(ctx context.Context, state commitState, resumed bool) (*ImportResult, error) {
	startedAt := i.now().UTC().Format(time.RFC3339Nano)
	if state.preview.CreatedAtUTC != "" && !resumed {
		startedAt = state.preview.CreatedAtUTC
	}
	remarks := normalizationLogRemarks(state.preview.NormalizationVersion, state.preview.NormalizationHash)
	log := &model.ImportLog{
		ImportID:        state.importID,
		SourceType:      state.document.SourceType,
		SourceReference: state.document.SourceReference,
		FileName:        stringPtr(state.document.FileName),
		FileHash:        stringPtr(state.document.SourceHash),
		IdempotencyKey:  stringPtr(state.idempotencyKey),
		RowsRead:        state.rowsRead,
		RowsInserted:    state.rowsInserted,
		RowsSkipped:     state.rowsSkipped,
		RowsFailed:      state.rowsFailed,
		Status:          importStatusRunning,
		StartedAt:       startedAt,
		OperatorName:    stringPtr(state.preview.OperatorName),
		Remarks:         stringPtr(remarks),
	}

	if resumed {
		log.StartedAt = state.preview.CreatedAtUTC
	}

	if existing, err := i.getImportLogByID(ctx, state.importID); err == nil && existing != nil && existing.Status != "" {
		log.StartedAt = existing.StartedAt
	}

	if err := i.repo.CreateImportLog(ctx, log); err != nil {
		if existing, lookupErr := i.getImportLogByID(ctx, state.importID); lookupErr == nil && existing != nil {
			return i.resumeExistingImport(ctx, state, existing)
		}
		return nil, fmt.Errorf("create import log: %w", err)
	}

	return i.processImport(ctx, state, log, resumed)
}

func (i *Importer) resumeExistingImport(ctx context.Context, state commitState, log *model.ImportLog) (*ImportResult, error) {
	if log == nil {
		return nil, fmt.Errorf("import log is nil")
	}
	if log.Status == importStatusSucceeded || log.Status == importStatusResumed {
		return importResultFromLog(log), nil
	}
	if log.CheckpointToken == nil || strings.TrimSpace(*log.CheckpointToken) == "" {
		return importResultFromLog(log), nil
	}
	return i.processImport(ctx, state, log, true)
}

func (i *Importer) processImport(ctx context.Context, state commitState, log *model.ImportLog, resumed bool) (*ImportResult, error) {
	headerStats, _, err := i.scanSource(ctx, state.document, 0, false)
	if err != nil {
		return nil, err
	}
	if headerStats.Total == 0 && headerStats.Valid == 0 && headerStats.Invalid == 0 {
		// proceed; rows are checked below
	}

	reader := csv.NewReader(bytes.NewReader(state.document.CSVBytes))
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read import header: %w", err)
	}
	if err := validateImportHeader(header); err != nil {
		return nil, err
	}

	rowIndex := 0
	result := importResultFromLog(log)
	result.Status = importStatusRunning
	log.Status = importStatusRunning

	ledgerRepo := i.normalizationLedgerRepository()
	var normalizationRun *model.LocationNormalizationRun
	if ledgerRepo != nil {
		version := strings.TrimSpace(state.preview.NormalizationVersion)
		if version == "" {
			version = locationnorm.NormalizationVersion
		}
		importID := state.importID
		sourceReference := state.document.SourceReference
		runID := state.importID + "-location-normalization"
		existingRun, err := ledgerRepo.GetLocationNormalizationRun(ctx, runID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("load location normalization run: %w", err)
		}
		if existingRun != nil {
			normalizationRun = existingRun
		} else {
			normalizationRun = &model.LocationNormalizationRun{
				RunID:                runID,
				ImportID:             &importID,
				SourceReference:      &sourceReference,
				Mode:                 model.LocationNormalizationModeWrite,
				Status:               "RUNNING",
				NormalizationVersion: version,
				TotalRows:            0,
				AutoAppliedRows:      0,
				ReviewRows:           0,
				FailedRows:           0,
				StartedAt:            i.now().UTC().Format(time.RFC3339Nano),
				CompletedAt:          nil,
			}
			if err := ledgerRepo.CreateLocationNormalizationRun(ctx, normalizationRun); err != nil {
				return nil, fmt.Errorf("create location normalization run: %w", err)
			}
		}
	}

	persistRun := func(status string, completed bool) error {
		if normalizationRun == nil {
			return nil
		}
		normalizationRun.Status = status
		normalizationRun.TotalRows = result.RowsRead
		normalizationRun.FailedRows = result.RowsFailed
		if completed {
			nowUTC := i.now().UTC().Format(time.RFC3339Nano)
			normalizationRun.CompletedAt = &nowUTC
		} else {
			normalizationRun.CompletedAt = nil
		}
		return ledgerRepo.UpdateLocationNormalizationRun(ctx, normalizationRun)
	}
	markRunFailedOnError := normalizationRun != nil
	defer func() {
		if !markRunFailedOnError {
			return
		}
		_ = persistRun("FAILED", true)
	}()

	recordNormalizationItem := func(rowNumber int, rowSourceReference string, normalizationResult *model.LocationNormalizationResult, reason error) error {
		if normalizationRun == nil || normalizationResult == nil {
			return nil
		}

		status := normalizationResult.Status
		needsReview := normalizationResult.NeedsReview
		if reason != nil {
			var reviewErr *normalizationReviewRequiredError
			if errors.As(reason, &reviewErr) {
				status = model.LocationNormalizationStatusReviewNeeded
				needsReview = true
			}
		}
		if status == model.LocationNormalizationStatusAutoApplied {
			normalizationRun.AutoAppliedRows++
		} else {
			normalizationRun.ReviewRows++
		}

		itemReason := strings.TrimSpace(normalizationResult.Reason)
		if reason != nil && itemReason == "" {
			itemReason = reason.Error()
		}

		nowUTC := i.now().UTC().Format(time.RFC3339Nano)
		item := &model.LocationNormalizationItem{
			ItemID:               uuid.NewString(),
			RunID:                normalizationRun.RunID,
			RowNumber:            rowNumber,
			SourceReference:      stringPtr(rowSourceReference),
			RawRegion:            normalizationResult.Raw.Region,
			RawProvince:          normalizationResult.Raw.Province,
			RawCity:              normalizationResult.Raw.City,
			RawBarangay:          normalizationResult.Raw.Barangay,
			ResolvedRegionCode:   stringPtr(normalizationResult.Resolved.RegionCode),
			ResolvedRegionName:   stringPtr(normalizationResult.Resolved.RegionName),
			ResolvedProvinceCode: stringPtr(normalizationResult.Resolved.ProvinceCode),
			ResolvedProvinceName: stringPtr(normalizationResult.Resolved.ProvinceName),
			ResolvedCityCode:     stringPtr(normalizationResult.Resolved.CityCode),
			ResolvedCityName:     stringPtr(normalizationResult.Resolved.CityName),
			ResolvedBarangayCode: stringPtr(normalizationResult.Resolved.BarangayCode),
			ResolvedBarangayName: stringPtr(normalizationResult.Resolved.BarangayName),
			Confidence:           normalizationResult.Confidence,
			MatchSource:          normalizationResult.MatchSource,
			Status:               status,
			NeedsReview:          needsReview,
			Reason:               stringPtr(itemReason),
			NormalizationVersion: normalizationResult.NormalizationVersion,
			CreatedAt:            nowUTC,
		}
		return ledgerRepo.CreateLocationNormalizationItem(ctx, item)
	}

	for {
		if err := ctx.Err(); err != nil {
			break
		}

		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read source row %d: %w", rowIndex+2, err)
		}

		rowIndex++
		if rowIndex < state.startRow {
			continue
		}

		result.RowsRead++
		log.RowsRead = result.RowsRead

		draft, rowSourceReference, preferredGeneratedID, normalizationResult, err := i.buildDraftFromRecord(ctx, state.document, record, rowIndex)
		if err != nil {
			var reviewErr *normalizationReviewRequiredError
			if errors.As(err, &reviewErr) {
				result.RowsSkipped++
				log.RowsSkipped = result.RowsSkipped
			} else {
				result.RowsFailed++
				log.RowsFailed = result.RowsFailed
			}
			if recErr := recordNormalizationItem(rowIndex, rowSourceReference, normalizationResult, err); recErr != nil {
				return nil, recErr
			}
			if err := i.updateImportLog(ctx, log); err != nil {
				return nil, err
			}
			if state.commitRowBudget > 0 && result.RowsRead >= state.commitRowBudget {
				next := i.makeCheckpointToken(state, result, rowIndex+1, importTokenStageCheckpoint)
				log.Status = importStatusPartial
				log.CheckpointToken = stringPtr(next)
				if err := i.updateImportLog(ctx, log); err != nil {
					return nil, err
				}
				if err := persistRun("PARTIAL", false); err != nil {
					return nil, err
				}
				markRunFailedOnError = false
				return i.resultFromPartial(log, next), nil
			}
			continue
		}
		if recErr := recordNormalizationItem(rowIndex, rowSourceReference, normalizationResult, nil); recErr != nil {
			return nil, recErr
		}

		exists, err := i.hasSourceReference(ctx, rowSourceReference)
		if err != nil {
			return nil, err
		}
		if exists {
			result.RowsSkipped++
			log.RowsSkipped = result.RowsSkipped
			if err := i.updateImportLog(ctx, log); err != nil {
				return nil, err
			}
			if state.commitRowBudget > 0 && result.RowsRead >= state.commitRowBudget {
				next := i.makeCheckpointToken(state, result, rowIndex+1, importTokenStageCheckpoint)
				log.Status = importStatusPartial
				log.CheckpointToken = stringPtr(next)
				if err := i.updateImportLog(ctx, log); err != nil {
					return nil, err
				}
				if err := persistRun("PARTIAL", false); err != nil {
					return nil, err
				}
				markRunFailedOnError = false
				return i.resultFromPartial(log, next), nil
			}
			continue
		}

		prompt, err := i.creator.BuildDuplicatePrecheckPrompt(ctx, draft, "")
		if err != nil {
			result.RowsFailed++
			log.RowsFailed = result.RowsFailed
			if err := i.updateImportLog(ctx, log); err != nil {
				return nil, err
			}
			continue
		}
		if prompt != nil && prompt.HasExactDuplicate {
			result.RowsSkipped++
			log.RowsSkipped = result.RowsSkipped
			if err := i.updateImportLog(ctx, log); err != nil {
				return nil, err
			}
			if state.commitRowBudget > 0 && result.RowsRead >= state.commitRowBudget {
				next := i.makeCheckpointToken(state, result, rowIndex+1, importTokenStageCheckpoint)
				log.Status = importStatusPartial
				log.CheckpointToken = stringPtr(next)
				if err := i.updateImportLog(ctx, log); err != nil {
					return nil, err
				}
				if err := persistRun("PARTIAL", false); err != nil {
					return nil, err
				}
				markRunFailedOnError = false
				return i.resultFromPartial(log, next), nil
			}
			continue
		}

		created, err := i.creator.CreateBeneficiary(ctx, draft, service.CreateOptions{
			InternalUUID:         uuid.NewString(),
			PreferredGeneratedID: preferredGeneratedID,
			SourceType:           model.BeneficiarySourceImport,
			SourceReference:      rowSourceReference,
			RecordStatus:         model.RecordStatusActive,
			DedupStatus:          model.DedupStatusClear,
		})
		if err != nil {
			result.RowsFailed++
			log.RowsFailed = result.RowsFailed
			if err := i.updateImportLog(ctx, log); err != nil {
				return nil, err
			}
			continue
		}
		if created != nil {
			result.RowsInserted++
			log.RowsInserted = result.RowsInserted
		}

		if err := i.updateImportLog(ctx, log); err != nil {
			return nil, err
		}

		if state.commitRowBudget > 0 && result.RowsRead >= state.commitRowBudget {
			next := i.makeCheckpointToken(state, result, rowIndex+1, importTokenStageCheckpoint)
			log.Status = importStatusPartial
			log.CheckpointToken = stringPtr(next)
			if err := i.updateImportLog(ctx, log); err != nil {
				return nil, err
			}
			if err := persistRun("PARTIAL", false); err != nil {
				return nil, err
			}
			markRunFailedOnError = false
			return i.resultFromPartial(log, next), nil
		}
	}

	finalStatus := importStatusSucceeded
	if resumed {
		finalStatus = importStatusResumed
	}
	log.Status = finalStatus
	nowUTC := i.now().UTC().Format(time.RFC3339Nano)
	log.CompletedAt = stringPtr(nowUTC)
	log.CheckpointToken = nil
	if err := i.updateImportLog(ctx, log); err != nil {
		return nil, err
	}
	if err := persistRun("COMPLETED", true); err != nil {
		return nil, err
	}
	markRunFailedOnError = false
	return importResultFromLog(log), nil
}

func (i *Importer) resultFromPartial(log *model.ImportLog, checkpoint string) *ImportResult {
	if log == nil {
		return &ImportResult{Status: importStatusPartial, CheckpointToken: stringPtr(checkpoint)}
	}
	log.Status = importStatusPartial
	log.CheckpointToken = stringPtr(checkpoint)
	log.CompletedAt = nil
	return importResultFromLog(log)
}

func (i *Importer) makeCheckpointToken(state commitState, result *ImportResult, nextRow int, stage string) string {
	if result == nil {
		result = &ImportResult{}
	}
	token := ImportToken{
		Stage:                stage,
		SourceType:           state.document.SourceType,
		SourcePath:           state.document.SourcePath,
		SourceHash:           state.document.SourceHash,
		SourceReference:      state.document.SourceReference,
		FileName:             state.document.FileName,
		OperatorName:         state.preview.OperatorName,
		CreatedAtUTC:         state.preview.CreatedAtUTC,
		ImportID:             state.importID,
		IdempotencyKey:       state.idempotencyKey,
		NextRow:              nextRow,
		RowsRead:             result.RowsRead,
		RowsInserted:         result.RowsInserted,
		RowsSkipped:          result.RowsSkipped,
		RowsFailed:           result.RowsFailed,
		NormalizationVersion: state.preview.NormalizationVersion,
		NormalizationHash:    state.preview.NormalizationHash,
	}
	encoded, err := encodeImportToken(token)
	if err != nil {
		return ""
	}
	return encoded
}

func (i *Importer) updateImportLog(ctx context.Context, log *model.ImportLog) error {
	if log == nil {
		return fmt.Errorf("import log is nil")
	}
	return i.repo.UpdateImportLog(ctx, log)
}

func (i *Importer) getImportLogByID(ctx context.Context, importID string) (*model.ImportLog, error) {
	log, err := i.repo.GetImportLog(ctx, importID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return log, nil
}

func (i *Importer) findExistingImport(ctx context.Context, sourceHash, idempotencyKey string) (*model.ImportLog, error) {
	logs, err := i.repo.ListImportLogs(ctx, repository.ImportLogListQuery{Limit: 500})
	if err != nil {
		return nil, err
	}
	for idx := range logs {
		log := logs[idx]
		if log.FileHash == nil || log.IdempotencyKey == nil {
			continue
		}
		if *log.FileHash == sourceHash && *log.IdempotencyKey == idempotencyKey {
			item := log
			return &item, nil
		}
	}
	return nil, nil
}

func (i *Importer) hasSourceReference(ctx context.Context, sourceReference string) (bool, error) {
	sourceReference = strings.TrimSpace(sourceReference)
	if sourceReference == "" {
		return false, nil
	}
	page, err := i.repo.ListBeneficiaries(ctx, repository.BeneficiaryListQuery{
		Search:         sourceReference,
		IncludeDeleted: false,
		Limit:          50,
		Offset:         0,
	})
	if err != nil {
		return false, err
	}
	for _, item := range page.Items {
		if item.SourceReference != nil && strings.TrimSpace(*item.SourceReference) == sourceReference {
			return true, nil
		}
	}
	return false, nil
}

func (i *Importer) buildDraftFromRecord(ctx context.Context, doc sourceDocument, record []string, rowIndex int) (service.BeneficiaryDraft, string, string, *model.LocationNormalizationResult, error) {
	switch len(record) {
	case len(publicBeneficiaryHeaders):
		return i.buildPublicDraftFromRecord(ctx, doc, record, rowIndex)
	case len(legacyBeneficiaryHeaders):
		return i.buildLegacyDraftFromRecord(ctx, doc, record, rowIndex)
	default:
		return service.BeneficiaryDraft{}, "", "", nil, fmt.Errorf("row %d: expected %d columns for the public template or %d columns for the legacy package layout, got %d", rowIndex, len(publicBeneficiaryHeaders), len(legacyBeneficiaryHeaders), len(record))
	}
}

func (i *Importer) buildPublicDraftFromRecord(ctx context.Context, doc sourceDocument, record []string, rowIndex int) (service.BeneficiaryDraft, string, string, *model.LocationNormalizationResult, error) {
	rowSourceReference := rowProvenance(doc.SourceReference, rowIndex)
	preferredGeneratedID := strings.TrimSpace(record[0])
	lastName := strings.TrimSpace(record[1])
	firstName := strings.TrimSpace(record[2])
	middleName := strings.TrimSpace(record[3])
	extensionName := strings.TrimSpace(record[4])
	regionValue := strings.TrimSpace(record[5])
	provinceValue := strings.TrimSpace(record[6])
	cityValue := strings.TrimSpace(record[7])
	barangayValue := strings.TrimSpace(record[8])
	contactNo := strings.TrimSpace(record[9])
	birthMonthText := strings.TrimSpace(record[10])
	birthDayText := strings.TrimSpace(record[11])
	birthYearText := strings.TrimSpace(record[12])
	sex := strings.TrimSpace(record[13])

	if regionValue == "" || provinceValue == "" || cityValue == "" || barangayValue == "" {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: region, province, city_municipality, and barangay are required", rowIndex)
	}

	normalizer, err := i.getLocationNormalizer(ctx)
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: load location normalizer: %w", rowIndex, err)
	}
	normalization := normalizer.NormalizeChain(model.LocationChainRaw{
		Region:   regionValue,
		Province: provinceValue,
		City:     cityValue,
		Barangay: barangayValue,
	})
	if normalization.Resolved.RegionCode == "" ||
		normalization.Resolved.ProvinceCode == "" ||
		normalization.Resolved.CityCode == "" ||
		normalization.Resolved.BarangayCode == "" {
		return service.BeneficiaryDraft{}, rowSourceReference, "", &normalization, &normalizationReviewRequiredError{result: normalization}
	}
	if normalization.NeedsReview && normalization.Confidence < importReviewAcceptanceThreshold {
		return service.BeneficiaryDraft{}, rowSourceReference, "", &normalization, &normalizationReviewRequiredError{result: normalization}
	}
	birthMonth, err := parseNullableInt64Field(birthMonthText, "month_mm")
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", &normalization, fmt.Errorf("row %d: %w", rowIndex, err)
	}
	birthDay, err := parseNullableInt64Field(birthDayText, "day_dd")
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", &normalization, fmt.Errorf("row %d: %w", rowIndex, err)
	}
	birthYear, err := parseNullableInt64Field(birthYearText, "year_yyyy")
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", &normalization, fmt.Errorf("row %d: %w", rowIndex, err)
	}

	draft := service.BeneficiaryDraft{
		LastName:      lastName,
		FirstName:     firstName,
		MiddleName:    middleName,
		ExtensionName: extensionName,
		RegionCode:    normalization.Resolved.RegionCode,
		RegionName:    normalization.Resolved.RegionName,
		ProvinceCode:  normalization.Resolved.ProvinceCode,
		ProvinceName:  normalization.Resolved.ProvinceName,
		CityCode:      normalization.Resolved.CityCode,
		CityName:      normalization.Resolved.CityName,
		BarangayCode:  normalization.Resolved.BarangayCode,
		BarangayName:  normalization.Resolved.BarangayName,
		ContactNo:     contactNo,
		BirthMonth:    birthMonth,
		BirthDay:      birthDay,
		BirthYear:     birthYear,
		Sex:           sex,
	}

	if _, err := i.creator.NormalizeAndValidateDraft(draft); err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", &normalization, fmt.Errorf("row %d: %w", rowIndex, err)
	}

	return draft, rowSourceReference, preferredGeneratedID, &normalization, nil
}

func (i *Importer) buildLegacyDraftFromRecord(ctx context.Context, doc sourceDocument, record []string, rowIndex int) (service.BeneficiaryDraft, string, string, *model.LocationNormalizationResult, error) {
	rowSourceReference := rowProvenance(doc.SourceReference, rowIndex)
	preferredGeneratedID := strings.TrimSpace(record[0])
	lastName := strings.TrimSpace(record[1])
	firstName := strings.TrimSpace(record[2])
	middleName := strings.TrimSpace(record[3])
	extensionName := strings.TrimSpace(record[4])
	sex := strings.TrimSpace(record[5])
	birthdateISO := strings.TrimSpace(record[6])
	regionCode := strings.TrimSpace(record[7])
	provinceCode := strings.TrimSpace(record[8])
	cityCode := strings.TrimSpace(record[9])
	barangayCode := strings.TrimSpace(record[10])
	contactNo := strings.TrimSpace(record[11])

	if regionCode == "" || provinceCode == "" || cityCode == "" || barangayCode == "" {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: location codes are required", rowIndex)
	}

	region, err := i.repo.GetRegion(ctx, regionCode)
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: load region %s: %w", rowIndex, regionCode, err)
	}
	province, err := i.repo.GetProvince(ctx, provinceCode)
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: load province %s: %w", rowIndex, provinceCode, err)
	}
	city, err := i.repo.GetCity(ctx, cityCode)
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: load city %s: %w", rowIndex, cityCode, err)
	}
	barangay, err := i.repo.GetBarangay(ctx, barangayCode)
	if err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: load barangay %s: %w", rowIndex, barangayCode, err)
	}

	if strings.TrimSpace(region.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: region code mismatch", rowIndex)
	}
	if strings.TrimSpace(province.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: province %s does not belong to region %s", rowIndex, provinceCode, regionCode)
	}
	if strings.TrimSpace(city.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: city %s does not belong to region %s", rowIndex, cityCode, regionCode)
	}
	if city.ProvinceCode != nil && strings.TrimSpace(*city.ProvinceCode) != provinceCode {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: city %s does not belong to province %s", rowIndex, cityCode, provinceCode)
	}
	if strings.TrimSpace(barangay.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: barangay %s does not belong to region %s", rowIndex, barangayCode, regionCode)
	}
	if barangay.ProvinceCode != nil && strings.TrimSpace(*barangay.ProvinceCode) != provinceCode {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: barangay %s does not belong to province %s", rowIndex, barangayCode, provinceCode)
	}
	if strings.TrimSpace(barangay.CityCode) != cityCode {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: barangay %s does not belong to city %s", rowIndex, barangayCode, cityCode)
	}

	draft := service.BeneficiaryDraft{
		LastName:      lastName,
		FirstName:     firstName,
		MiddleName:    middleName,
		ExtensionName: extensionName,
		RegionCode:    region.RegionCode,
		RegionName:    region.RegionName,
		ProvinceCode:  province.ProvinceCode,
		ProvinceName:  province.ProvinceName,
		CityCode:      city.CityCode,
		CityName:      city.CityName,
		BarangayCode:  barangay.BarangayCode,
		BarangayName:  barangay.BarangayName,
		ContactNo:     contactNo,
		BirthdateISO:  birthdateISO,
		Sex:           sex,
	}

	if _, err := i.creator.NormalizeAndValidateDraft(draft); err != nil {
		return service.BeneficiaryDraft{}, rowSourceReference, "", nil, fmt.Errorf("row %d: %w", rowIndex, err)
	}

	result := model.LocationNormalizationResult{
		Raw: model.LocationChainRaw{
			Region:   regionCode,
			Province: provinceCode,
			City:     cityCode,
			Barangay: barangayCode,
		},
		Resolved: model.LocationChainResolved{
			RegionCode:   region.RegionCode,
			RegionName:   region.RegionName,
			ProvinceCode: province.ProvinceCode,
			ProvinceName: province.ProvinceName,
			CityCode:     city.CityCode,
			CityName:     city.CityName,
			BarangayCode: barangay.BarangayCode,
			BarangayName: barangay.BarangayName,
		},
		Confidence:           1,
		MatchSource:          model.LocationMatchSourceExact,
		Status:               model.LocationNormalizationStatusAutoApplied,
		NeedsReview:          false,
		AutoApply:            true,
		NormalizationVersion: locationnorm.NormalizationVersion,
	}
	return draft, rowSourceReference, preferredGeneratedID, &result, nil
}

func validateImportHeader(header []string) error {
	if headerMatches(header, publicBeneficiaryHeaders) || headerMatches(header, legacyBeneficiaryHeaders) {
		return nil
	}
	return fmt.Errorf("expected either public template headers (%d columns) or legacy package headers (%d columns), got %d", len(publicBeneficiaryHeaders), len(legacyBeneficiaryHeaders), len(header))
}

func headerMatches(header []string, expected []string) bool {
	if len(header) != len(expected) {
		return false
	}
	for idx, want := range expected {
		got := strings.TrimSpace(header[idx])
		if idx == 0 {
			got = strings.TrimPrefix(got, "\ufeff")
		}
		if got != want {
			return false
		}
	}
	return true
}

func (i *Importer) normalizationLedgerRepository() normalizationLedgerRepository {
	repo, ok := i.repo.(normalizationLedgerRepository)
	if !ok {
		return nil
	}
	return repo
}

func (i *Importer) computeNormalizationMetadata(ctx context.Context) (string, string, error) {
	version := locationnorm.NormalizationVersion
	checksum := ""
	metadata, err := i.repo.GetIngestMetadata(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", "", fmt.Errorf("load psgc ingest metadata: %w", err)
	}
	if metadata != nil {
		checksum = strings.TrimSpace(metadata.SourceChecksum)
	}
	basis := version + "|" + checksum
	sum := sha256.Sum256([]byte(basis))
	return version, hex.EncodeToString(sum[:]), nil
}

func (i *Importer) validateNormalizationMetadata(ctx context.Context, token ImportToken) error {
	if strings.TrimSpace(token.NormalizationVersion) == "" || strings.TrimSpace(token.NormalizationHash) == "" {
		return nil
	}
	currentVersion, currentHash, err := i.computeNormalizationMetadata(ctx)
	if err != nil {
		return err
	}
	if token.NormalizationVersion != currentVersion || token.NormalizationHash != currentHash {
		return fmt.Errorf(
			"normalization metadata mismatch: token version/hash (%s/%s) != current (%s/%s)",
			token.NormalizationVersion,
			token.NormalizationHash,
			currentVersion,
			currentHash,
		)
	}
	return nil
}

func normalizationLogRemarks(version, hash string) string {
	meta := map[string]string{
		"normalization_version": strings.TrimSpace(version),
		"normalization_hash":    strings.TrimSpace(hash),
	}
	body, err := json.Marshal(meta)
	if err != nil {
		return fmt.Sprintf("normalization_version=%s normalization_hash=%s", strings.TrimSpace(version), strings.TrimSpace(hash))
	}
	return string(body)
}

type resolvedPSGCLocation struct {
	RegionCode   string
	RegionName   string
	ProvinceCode string
	ProvinceName string
	CityCode     string
	CityName     string
	BarangayCode string
	BarangayName string
}

func (i *Importer) getLocationNormalizer(ctx context.Context) (*locationnorm.LocationNormalizer, error) {
	i.normalizerMu.Lock()
	normalizer := i.normalizer
	i.normalizerMu.Unlock()
	if normalizer != nil {
		return normalizer, nil
	}

	catalog, err := i.getPSGCCatalog(ctx)
	if err != nil {
		return nil, err
	}
	regions, provinces, cities, barangays := flattenPSGCCatalog(catalog)
	locCatalog, err := locationnorm.NewCatalog(regions, provinces, cities, barangays)
	if err != nil {
		return nil, fmt.Errorf("build location normalization catalog: %w", err)
	}
	built, err := locationnorm.New(locCatalog)
	if err != nil {
		return nil, fmt.Errorf("build location normalizer: %w", err)
	}

	i.normalizerMu.Lock()
	defer i.normalizerMu.Unlock()
	if i.normalizer == nil {
		i.normalizer = built
	}
	return i.normalizer, nil
}

func flattenPSGCCatalog(catalog *psgcCatalog) ([]model.PSGCRegion, []model.PSGCProvince, []model.PSGCCity, []model.PSGCBarangay) {
	if catalog == nil {
		return nil, nil, nil, nil
	}
	regions := append([]model.PSGCRegion(nil), catalog.regions...)

	provinces := make([]model.PSGCProvince, 0)
	seenProvince := make(map[string]struct{})
	for _, items := range catalog.provincesByRegion {
		for _, item := range items {
			key := strings.TrimSpace(item.ProvinceCode)
			if key == "" {
				continue
			}
			if _, exists := seenProvince[key]; exists {
				continue
			}
			seenProvince[key] = struct{}{}
			provinces = append(provinces, item)
		}
	}

	cities := make([]model.PSGCCity, 0)
	seenCity := make(map[string]struct{})
	for _, items := range catalog.citiesByProvince {
		for _, item := range items {
			key := strings.TrimSpace(item.CityCode)
			if key == "" {
				continue
			}
			if _, exists := seenCity[key]; exists {
				continue
			}
			seenCity[key] = struct{}{}
			cities = append(cities, item)
		}
	}

	barangays := make([]model.PSGCBarangay, 0)
	seenBarangay := make(map[string]struct{})
	for _, items := range catalog.barangaysByCity {
		for _, item := range items {
			key := strings.TrimSpace(item.BarangayCode)
			if key == "" {
				continue
			}
			if _, exists := seenBarangay[key]; exists {
				continue
			}
			seenBarangay[key] = struct{}{}
			barangays = append(barangays, item)
		}
	}

	return regions, provinces, cities, barangays
}

func (i *Importer) getPSGCCatalog(ctx context.Context) (*psgcCatalog, error) {
	i.catalogMu.Lock()
	catalog := i.catalog
	i.catalogMu.Unlock()
	if catalog != nil {
		return catalog, nil
	}

	built, err := i.buildPSGCCatalog(ctx)
	if err != nil {
		return nil, err
	}

	i.catalogMu.Lock()
	defer i.catalogMu.Unlock()
	if i.catalog == nil {
		i.catalog = built
	}
	return i.catalog, nil
}

func (i *Importer) buildPSGCCatalog(ctx context.Context) (*psgcCatalog, error) {
	regions, err := i.repo.ListRegions(ctx)
	if err != nil {
		return nil, err
	}

	catalog := &psgcCatalog{
		regions:           regions,
		provincesByRegion: make(map[string][]model.PSGCProvince, len(regions)),
		citiesByProvince:  make(map[string][]model.PSGCCity),
		barangaysByCity:   make(map[string][]model.PSGCBarangay),
	}

	for _, region := range regions {
		provinces, err := i.repo.ListProvincesByRegion(ctx, region.RegionCode)
		if err != nil {
			return nil, fmt.Errorf("load provinces for region %s: %w", region.RegionCode, err)
		}
		catalog.provincesByRegion[region.RegionCode] = provinces

		for _, province := range provinces {
			cities, err := i.repo.ListCitiesByProvince(ctx, province.ProvinceCode)
			if err != nil {
				return nil, fmt.Errorf("load cities for province %s: %w", province.ProvinceCode, err)
			}
			catalog.citiesByProvince[province.ProvinceCode] = cities

			for _, city := range cities {
				barangays, err := i.repo.ListBarangaysByCity(ctx, city.CityCode)
				if err != nil {
					return nil, fmt.Errorf("load barangays for city %s: %w", city.CityCode, err)
				}
				catalog.barangaysByCity[city.CityCode] = barangays
			}
		}
	}

	return catalog, nil
}

func (c *psgcCatalog) resolvePublicLocation(regionValue, provinceValue, cityValue, barangayValue string) (resolvedPSGCLocation, error) {
	region, err := resolvePSGCRegion(c.regions, regionValue)
	if err != nil {
		return resolvedPSGCLocation{}, err
	}
	provinces := c.provincesByRegion[region.RegionCode]
	province, err := resolvePSGCProvince(provinces, provinceValue)
	if err != nil {
		return resolvedPSGCLocation{}, err
	}
	cities := c.citiesByProvince[province.ProvinceCode]
	city, err := resolvePSGCCity(cities, cityValue)
	if err != nil {
		return resolvedPSGCLocation{}, err
	}
	barangays := c.barangaysByCity[city.CityCode]
	barangay, err := resolvePSGCBarangay(barangays, barangayValue)
	if err != nil {
		return resolvedPSGCLocation{}, err
	}

	if strings.TrimSpace(province.RegionCode) != region.RegionCode {
		return resolvedPSGCLocation{}, fmt.Errorf("province %q does not belong to region %q", provinceValue, regionValue)
	}
	if city.RegionCode != region.RegionCode {
		return resolvedPSGCLocation{}, fmt.Errorf("city %q does not belong to region %q", cityValue, regionValue)
	}
	if city.ProvinceCode != nil && strings.TrimSpace(*city.ProvinceCode) != province.ProvinceCode {
		return resolvedPSGCLocation{}, fmt.Errorf("city %q does not belong to province %q", cityValue, provinceValue)
	}
	if barangay.RegionCode != region.RegionCode {
		return resolvedPSGCLocation{}, fmt.Errorf("barangay %q does not belong to region %q", barangayValue, regionValue)
	}
	if barangay.ProvinceCode != nil && strings.TrimSpace(*barangay.ProvinceCode) != province.ProvinceCode {
		return resolvedPSGCLocation{}, fmt.Errorf("barangay %q does not belong to province %q", barangayValue, provinceValue)
	}
	if barangay.CityCode != city.CityCode {
		return resolvedPSGCLocation{}, fmt.Errorf("barangay %q does not belong to city %q", barangayValue, cityValue)
	}

	return resolvedPSGCLocation{
		RegionCode:   region.RegionCode,
		RegionName:   region.RegionName,
		ProvinceCode: province.ProvinceCode,
		ProvinceName: province.ProvinceName,
		CityCode:     city.CityCode,
		CityName:     city.CityName,
		BarangayCode: barangay.BarangayCode,
		BarangayName: barangay.BarangayName,
	}, nil
}

func resolvePSGCRegion(items []model.PSGCRegion, raw string) (model.PSGCRegion, error) {
	matches := make([]model.PSGCRegion, 0, 1)
	for _, item := range items {
		if matchPSGCValue(item.RegionCode, item.RegionName, raw) {
			matches = append(matches, item)
		}
	}
	return selectSinglePSGCRegion(matches, "region", raw)
}

func resolvePSGCProvince(items []model.PSGCProvince, raw string) (model.PSGCProvince, error) {
	matches := make([]model.PSGCProvince, 0, 1)
	for _, item := range items {
		if matchPSGCValue(item.ProvinceCode, item.ProvinceName, raw) {
			matches = append(matches, item)
		}
	}
	return selectSinglePSGCProvince(matches, "province", raw)
}

func resolvePSGCCity(items []model.PSGCCity, raw string) (model.PSGCCity, error) {
	matches := make([]model.PSGCCity, 0, 1)
	for _, item := range items {
		if matchPSGCValue(item.CityCode, item.CityName, raw) {
			matches = append(matches, item)
		}
	}
	return selectSinglePSGCCity(matches, "city", raw)
}

func resolvePSGCBarangay(items []model.PSGCBarangay, raw string) (model.PSGCBarangay, error) {
	matches := make([]model.PSGCBarangay, 0, 1)
	for _, item := range items {
		if matchPSGCValue(item.BarangayCode, item.BarangayName, raw) {
			matches = append(matches, item)
		}
	}
	return selectSinglePSGCBarangay(matches, "barangay", raw)
}

func selectSinglePSGCRegion(items []model.PSGCRegion, label, raw string) (model.PSGCRegion, error) {
	switch len(items) {
	case 0:
		return model.PSGCRegion{}, fmt.Errorf("no %s found for %q", label, raw)
	case 1:
		return items[0], nil
	default:
		return model.PSGCRegion{}, fmt.Errorf("ambiguous %s value %q", label, raw)
	}
}

func selectSinglePSGCProvince(items []model.PSGCProvince, label, raw string) (model.PSGCProvince, error) {
	switch len(items) {
	case 0:
		return model.PSGCProvince{}, fmt.Errorf("no %s found for %q", label, raw)
	case 1:
		return items[0], nil
	default:
		return model.PSGCProvince{}, fmt.Errorf("ambiguous %s value %q", label, raw)
	}
}

func selectSinglePSGCCity(items []model.PSGCCity, label, raw string) (model.PSGCCity, error) {
	switch len(items) {
	case 0:
		return model.PSGCCity{}, fmt.Errorf("no %s found for %q", label, raw)
	case 1:
		return items[0], nil
	default:
		return model.PSGCCity{}, fmt.Errorf("ambiguous %s value %q", label, raw)
	}
}

func selectSinglePSGCBarangay(items []model.PSGCBarangay, label, raw string) (model.PSGCBarangay, error) {
	switch len(items) {
	case 0:
		return model.PSGCBarangay{}, fmt.Errorf("no %s found for %q", label, raw)
	case 1:
		return items[0], nil
	default:
		return model.PSGCBarangay{}, fmt.Errorf("ambiguous %s value %q", label, raw)
	}
}

func matchPSGCValue(code, name, raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if strings.TrimSpace(code) == raw {
		return true
	}
	return normalizeLookupString(name) == normalizeLookupString(raw)
}

func normalizeLookupString(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func parseNullableInt64Field(value, fieldName string) (*int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be numeric", fieldName)
	}
	return &parsed, nil
}

func rowProvenance(base string, rowIndex int) string {
	base = strings.TrimSpace(base)
	if base == "" {
		base = "import-source"
	}
	return fmt.Sprintf("%s#row=%06d", base, rowIndex)
}

func parsePackageManifest(body []byte) (exchangePackageManifest, error) {
	var manifest exchangePackageManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return exchangePackageManifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return manifest, nil
}

func parseChecksumList(body []byte) (map[string]string, error) {
	lines := strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n")
	checksums := make(map[string]string, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid checksum line: %q", line)
		}
		checksums[filepath.ToSlash(parts[1])] = strings.TrimSpace(parts[0])
	}
	return checksums, nil
}

func verifyPackageChecksums(expected map[string]string, files map[string][]byte) error {
	for name, body := range files {
		name = filepath.ToSlash(name)
		switch {
		case name == packageChecksumsFileName:
			continue
		case name == packageManifestFileName || name == packageBeneficiariesName || name == packageExportMetaFileName:
			// core files must be validated
		case name == "README.txt" || strings.HasPrefix(name, "attachments/"):
			// v1 optional files are intentionally ignored by the core importer
			continue
		default:
			return fmt.Errorf("unexpected file in package: %s", name)
		}
		sum := sha256.Sum256(body)
		got := hex.EncodeToString(sum[:])
		if expected[name] != got {
			return fmt.Errorf("checksum mismatch for %s", name)
		}
	}
	return nil
}

func encodeImportToken(token ImportToken) (string, error) {
	body, err := json.Marshal(token)
	if err != nil {
		return "", fmt.Errorf("marshal import token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(body), nil
}

func decodeImportToken(token string) (ImportToken, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return ImportToken{}, fmt.Errorf("token is required")
	}
	body, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ImportToken{}, fmt.Errorf("decode token: %w", err)
	}
	var decoded ImportToken
	if err := json.Unmarshal(body, &decoded); err != nil {
		return ImportToken{}, fmt.Errorf("unmarshal token: %w", err)
	}
	return decoded, nil
}

func importResultFromLog(log *model.ImportLog) *ImportResult {
	if log == nil {
		return &ImportResult{}
	}
	return &ImportResult{
		ImportID:        log.ImportID,
		Status:          log.Status,
		RowsRead:        log.RowsRead,
		RowsInserted:    log.RowsInserted,
		RowsSkipped:     log.RowsSkipped,
		RowsFailed:      log.RowsFailed,
		CheckpointToken: log.CheckpointToken,
		CompletedAtUTC:  log.CompletedAt,
	}
}

func stringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
