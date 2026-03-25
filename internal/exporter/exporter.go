package exporter

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dedup/internal/model"
	"dedup/internal/repository"

	"github.com/google/uuid"
)

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// New constructs an exporter over repository surfaces.
func New(repo exportRepository, opts ...Option) (*Exporter, error) {
	if repo == nil {
		return nil, fmt.Errorf("export repository is nil")
	}

	exp := &Exporter{
		repo: repo,
		now: func() time.Time {
			return time.Now().UTC()
		},
		pageSize: defaultPageSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(exp)
		}
	}

	return exp, nil
}

// NewExporter is an explicit alias for New.
func NewExporter(repo exportRepository, opts ...Option) (*Exporter, error) {
	return New(repo, opts...)
}

// ExportCSV writes Excel-friendly beneficiary CSV output and records an export log.
func (e *Exporter) ExportCSV(ctx context.Context, req Request) (*Result, error) {
	if err := e.validateReady(); err != nil {
		return nil, err
	}
	outputPath := strings.TrimSpace(req.OutputPath)
	if outputPath == "" {
		return nil, fmt.Errorf("output path is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	rows, considered, err := e.collectRows(ctx, req.IncludeUnresolvedDuplicates)
	if err != nil {
		return nil, err
	}
	if err := writeCSV(outputPath, rows); err != nil {
		return nil, err
	}

	createdAt := e.now().UTC().Format(time.RFC3339Nano)
	exportID := uuid.NewString()
	fileName := filepath.Base(outputPath)
	if err := e.repo.CreateExportLog(ctx, &model.ExportLog{
		ExportID:     exportID,
		FileName:     fileName,
		ExportType:   exportTypeBeneficiaries,
		RowsExported: len(rows),
		CreatedAt:    createdAt,
		PerformedBy:  trimmedStringPtr(req.OperatorName),
	}); err != nil {
		return nil, fmt.Errorf("record export log: %w", err)
	}

	return &Result{
		ExportID:       exportID,
		OutputPath:     outputPath,
		FileName:       fileName,
		RowsConsidered: considered,
		RowsExported:   len(rows),
		CreatedAtUTC:   createdAt,
	}, nil
}

func (e *Exporter) collectRows(ctx context.Context, includeUnresolvedDuplicates bool) ([]model.Beneficiary, int, error) {
	offset := 0
	rows := make([]model.Beneficiary, 0)
	considered := 0

	for {
		page, err := e.repo.ListBeneficiaries(ctx, repository.BeneficiaryListQuery{
			IncludeDeleted: false,
			Limit:          e.pageSize,
			Offset:         offset,
		})
		if err != nil {
			return nil, 0, fmt.Errorf("list beneficiaries for export: %w", err)
		}
		if len(page.Items) == 0 {
			break
		}

		for _, item := range page.Items {
			considered++
			if !isExportable(item, includeUnresolvedDuplicates) {
				continue
			}
			rows = append(rows, item)
		}

		offset += len(page.Items)
		if offset >= page.Total {
			break
		}
	}

	return rows, considered, nil
}

func writeCSV(path string, rows []model.Beneficiary) (err error) {
	parentDir := filepath.Dir(path)
	if parentDir != "." && parentDir != "" {
		if mkErr := os.MkdirAll(parentDir, 0o755); mkErr != nil {
			return fmt.Errorf("create export directory: %w", mkErr)
		}
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create export file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close export file: %w", closeErr)
		}
	}()

	if _, err = file.Write(utf8BOM); err != nil {
		return fmt.Errorf("write utf-8 bom: %w", err)
	}

	writer := csv.NewWriter(file)
	writer.UseCRLF = true
	if err = writer.Write(beneficiaryExportHeaders); err != nil {
		return fmt.Errorf("write export header: %w", err)
	}
	for _, item := range rows {
		if err = writer.Write(toCSVRow(item)); err != nil {
			return fmt.Errorf("write export row: %w", err)
		}
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		return fmt.Errorf("flush export csv: %w", err)
	}

	return nil
}

func toCSVRow(item model.Beneficiary) []string {
	return []string{
		sanitizeCSVCell(item.GeneratedID),
		sanitizeCSVCell(item.LastName),
		sanitizeCSVCell(item.FirstName),
		sanitizeCSVCell(stringValue(item.MiddleName)),
		sanitizeCSVCell(stringValue(item.ExtensionName)),
		sanitizeCSVCell(prefer(item.RegionName, item.RegionCode)),
		sanitizeCSVCell(prefer(item.ProvinceName, item.ProvinceCode)),
		sanitizeCSVCell(prefer(item.CityName, item.CityCode)),
		sanitizeCSVCell(prefer(item.BarangayName, item.BarangayCode)),
		sanitizeCSVCell(stringValue(item.ContactNo)),
		sanitizeCSVCell(mmString(item.BirthMonth)),
		sanitizeCSVCell(ddString(item.BirthDay)),
		sanitizeCSVCell(yyyyString(item.BirthYear)),
		sanitizeCSVCell(item.Sex),
	}
}

func isExportable(item model.Beneficiary, includeUnresolvedDuplicates bool) bool {
	if item.RecordStatus == model.RecordStatusDeleted {
		return false
	}
	if !isFinalRecord(item.RecordStatus) {
		return false
	}
	if !includeUnresolvedDuplicates && item.DedupStatus == model.DedupStatusPossibleDuplicate {
		return false
	}
	return true
}

func isFinalRecord(status model.RecordStatus) bool {
	return status == model.RecordStatusActive || status == model.RecordStatusRetained
}

func sanitizeCSVCell(value string) string {
	trimmed := strings.TrimLeft(value, " \t\r\n")
	if trimmed == "" {
		return value
	}

	switch trimmed[0] {
	case '=', '+', '-', '@':
		return "'" + value
	default:
		return value
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func trimmedStringPtr(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func prefer(primary, fallback string) string {
	if strings.TrimSpace(primary) != "" {
		return primary
	}
	return fallback
}

func mmString(value *int64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%02d", *value)
}

func ddString(value *int64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%02d", *value)
}

func yyyyString(value *int64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%04d", *value)
}

func (e *Exporter) validateReady() error {
	if e == nil {
		return fmt.Errorf("exporter is nil")
	}
	if e.repo == nil {
		return fmt.Errorf("export repository is nil")
	}
	if e.now == nil {
		return fmt.Errorf("export clock is nil")
	}
	if e.pageSize <= 0 {
		return fmt.Errorf("export page size must be greater than zero")
	}
	return nil
}
