package importer

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

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
)

// Importer orchestrates preview, commit, and resume for offline beneficiary imports.
type Importer struct {
	repo            importRepository
	creator         beneficiaryCreator
	now             func() time.Time
	commitRowBudget int
}

// ImportToken stores the stable source details used for preview/commit/resume.
type ImportToken struct {
	Stage           string             `json:"stage"`
	SourceType      model.ImportSource `json:"source_type"`
	SourcePath      string             `json:"source_path"`
	SourceHash      string             `json:"source_hash"`
	SourceReference string             `json:"source_reference"`
	FileName        string             `json:"file_name"`
	OperatorName    string             `json:"operator_name"`
	CreatedAtUTC    string             `json:"created_at_utc"`
	ImportID        string             `json:"import_id,omitempty"`
	IdempotencyKey  string             `json:"idempotency_key,omitempty"`
	NextRow         int                `json:"next_row,omitempty"`
	RowsRead        int                `json:"rows_read,omitempty"`
	RowsInserted    int                `json:"rows_inserted,omitempty"`
	RowsSkipped     int                `json:"rows_skipped,omitempty"`
	RowsFailed      int                `json:"rows_failed,omitempty"`
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

	stats, sampleErrors, err := i.scanSource(ctx, doc, 0, false)
	if err != nil {
		return nil, err
	}

	token, err := encodeImportToken(ImportToken{
		Stage:           importTokenStagePreview,
		SourceType:      doc.SourceType,
		SourcePath:      doc.SourcePath,
		SourceHash:      doc.SourceHash,
		SourceReference: doc.SourceReference,
		FileName:        doc.FileName,
		OperatorName:    strings.TrimSpace(source.OperatorName),
		CreatedAtUTC:    i.now().UTC().Format(time.RFC3339Nano),
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
		preview:         ImportToken{Stage: importTokenStagePreview, SourceType: checkpoint.SourceType, SourcePath: checkpoint.SourcePath, SourceHash: checkpoint.SourceHash, SourceReference: checkpoint.SourceReference, FileName: checkpoint.FileName, OperatorName: checkpoint.OperatorName, CreatedAtUTC: checkpoint.CreatedAtUTC},
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

		_, _, validationErr := i.buildDraftFromRecord(ctx, doc, record, rowIndex)
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

		draft, rowSourceReference, err := i.buildDraftFromRecord(ctx, state.document, record, rowIndex)
		if err != nil {
			result.RowsFailed++
			log.RowsFailed = result.RowsFailed
			if err := i.updateImportLog(ctx, log); err != nil {
				return nil, err
			}
			continue
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
				return i.resultFromPartial(log, next), nil
			}
			continue
		}

		created, err := i.creator.CreateBeneficiary(ctx, draft, service.CreateOptions{
			InternalUUID:         uuid.NewString(),
			PreferredGeneratedID: strings.TrimSpace(record[0]),
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
		Stage:           stage,
		SourceType:      state.document.SourceType,
		SourcePath:      state.document.SourcePath,
		SourceHash:      state.document.SourceHash,
		SourceReference: state.document.SourceReference,
		FileName:        state.document.FileName,
		OperatorName:    state.preview.OperatorName,
		CreatedAtUTC:    state.preview.CreatedAtUTC,
		ImportID:        state.importID,
		IdempotencyKey:  state.idempotencyKey,
		NextRow:         nextRow,
		RowsRead:        result.RowsRead,
		RowsInserted:    result.RowsInserted,
		RowsSkipped:     result.RowsSkipped,
		RowsFailed:      result.RowsFailed,
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
	logs, err := i.repo.ListImportLogs(ctx, repository.ImportLogListQuery{Limit: 500})
	if err != nil {
		return nil, err
	}
	for idx := range logs {
		if logs[idx].ImportID == importID {
			item := logs[idx]
			return &item, nil
		}
	}
	return nil, nil
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

func (i *Importer) buildDraftFromRecord(ctx context.Context, doc sourceDocument, record []string, rowIndex int) (service.BeneficiaryDraft, string, error) {
	if len(record) != len(requiredBeneficiaryHeaders) {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: expected %d columns, got %d", rowIndex, len(requiredBeneficiaryHeaders), len(record))
	}

	generatedID := strings.TrimSpace(record[0])
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

	if generatedID == "" {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: generated_id is required", rowIndex)
	}
	if regionCode == "" || provinceCode == "" || cityCode == "" || barangayCode == "" {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: location codes are required", rowIndex)
	}

	region, err := i.repo.GetRegion(ctx, regionCode)
	if err != nil {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: load region %s: %w", rowIndex, regionCode, err)
	}
	province, err := i.repo.GetProvince(ctx, provinceCode)
	if err != nil {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: load province %s: %w", rowIndex, provinceCode, err)
	}
	city, err := i.repo.GetCity(ctx, cityCode)
	if err != nil {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: load city %s: %w", rowIndex, cityCode, err)
	}
	barangay, err := i.repo.GetBarangay(ctx, barangayCode)
	if err != nil {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: load barangay %s: %w", rowIndex, barangayCode, err)
	}

	if strings.TrimSpace(region.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: region code mismatch", rowIndex)
	}
	if strings.TrimSpace(province.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: province %s does not belong to region %s", rowIndex, provinceCode, regionCode)
	}
	if strings.TrimSpace(city.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: city %s does not belong to region %s", rowIndex, cityCode, regionCode)
	}
	if city.ProvinceCode != nil && strings.TrimSpace(*city.ProvinceCode) != provinceCode {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: city %s does not belong to province %s", rowIndex, cityCode, provinceCode)
	}
	if strings.TrimSpace(barangay.RegionCode) != regionCode {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: barangay %s does not belong to region %s", rowIndex, barangayCode, regionCode)
	}
	if barangay.ProvinceCode != nil && strings.TrimSpace(*barangay.ProvinceCode) != provinceCode {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: barangay %s does not belong to province %s", rowIndex, barangayCode, provinceCode)
	}
	if strings.TrimSpace(barangay.CityCode) != cityCode {
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: barangay %s does not belong to city %s", rowIndex, barangayCode, cityCode)
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
		return service.BeneficiaryDraft{}, "", fmt.Errorf("row %d: %w", rowIndex, err)
	}

	rowSourceReference := rowProvenance(doc.SourceReference, rowIndex)
	return draft, rowSourceReference, nil
}

func validateImportHeader(header []string) error {
	if len(header) != len(requiredBeneficiaryHeaders) {
		return fmt.Errorf("expected %d columns, got %d", len(requiredBeneficiaryHeaders), len(header))
	}
	for idx, want := range requiredBeneficiaryHeaders {
		got := strings.TrimSpace(header[idx])
		if idx == 0 {
			got = strings.TrimPrefix(got, "\ufeff")
		}
		if got != want {
			return fmt.Errorf("unexpected header at column %d: got %q want %q", idx+1, got, want)
		}
	}
	return nil
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
		if filepath.ToSlash(name) == packageChecksumsFileName {
			continue
		}
		sum := sha256.Sum256(body)
		got := hex.EncodeToString(sum[:])
		if expected[filepath.ToSlash(name)] != got {
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
