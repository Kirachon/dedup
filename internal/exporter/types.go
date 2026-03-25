package exporter

import (
	"time"

	"dedup/internal/repository"
)

const (
	defaultPageSize         = 500
	exportTypeBeneficiaries = "BENEFICIARIES"
)

var beneficiaryExportHeaders = []string{
	"id",
	"last_name",
	"first_name",
	"middle_name",
	"extension_name",
	"region",
	"province",
	"city_municipality",
	"barangay",
	"contact_no",
	"month_mm",
	"day_dd",
	"year_yyyy",
	"sex",
}

// Request describes one beneficiary CSV export request.
type Request struct {
	OutputPath                  string
	OperatorName                string
	IncludeUnresolvedDuplicates bool
}

// Result summarizes one completed beneficiary CSV export.
type Result struct {
	ExportID       string
	OutputPath     string
	FileName       string
	RowsConsidered int
	RowsExported   int
	CreatedAtUTC   string
}

// Option configures Exporter behavior.
type Option func(*Exporter)

// WithClock overrides the exporter clock for deterministic tests.
func WithClock(clock func() time.Time) Option {
	return func(e *Exporter) {
		if clock != nil {
			e.now = clock
		}
	}
}

// WithPageSize overrides repository paging size for exports.
func WithPageSize(size int) Option {
	return func(e *Exporter) {
		if size > 0 {
			e.pageSize = size
		}
	}
}

type exportRepository interface {
	repository.BeneficiaryRepository
	repository.LogRepository
}

// Exporter handles policy-gated beneficiary CSV exports.
type Exporter struct {
	repo     exportRepository
	now      func() time.Time
	pageSize int
}
