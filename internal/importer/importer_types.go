package importer

import (
	"context"
	"time"

	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"
)

const (
	maxSampleErrors = 10
)

var requiredBeneficiaryHeaders = []string{
	"generated_id",
	"last_name",
	"first_name",
	"middle_name",
	"extension_name",
	"sex",
	"birthdate_iso",
	"region_code",
	"province_code",
	"city_code",
	"barangay_code",
	"contact_no",
}

// Source describes one import source path and optional metadata.
type Source struct {
	Type            model.ImportSource
	Path            string
	OperatorName    string
	SourceReference string
}

// PreviewReport is returned by Preview(source).
type PreviewReport struct {
	PreviewToken            string
	SourceType              model.ImportSource
	SourceHash              string
	RowCountTotal           int
	RowCountValid           int
	RowCountInvalid         int
	HeaderValidationPassed  bool
	SampleErrors            []string
	GeneratedAtUTC          string
	DetectedSourceReference string
}

// ImportResult is returned by Commit/Resume.
type ImportResult struct {
	ImportID        string
	Status          string
	RowsRead        int
	RowsInserted    int
	RowsSkipped     int
	RowsFailed      int
	CheckpointToken *string
	CompletedAtUTC  *string
}

// Option configures Importer behavior.
type Option func(*Importer)

// WithClock overrides the importer clock for deterministic tests.
func WithClock(clock func() time.Time) Option {
	return func(i *Importer) {
		if clock != nil {
			i.now = clock
		}
	}
}

// WithCommitRowBudget limits per-run row processing and forces resumable PARTIAL results in tests.
func WithCommitRowBudget(budget int) Option {
	return func(i *Importer) {
		if budget > 0 {
			i.commitRowBudget = budget
		}
	}
}

type beneficiaryCreator interface {
	NormalizeAndValidateDraft(draft service.BeneficiaryDraft) (*model.Beneficiary, error)
	BuildDuplicatePrecheckPrompt(ctx context.Context, draft service.BeneficiaryDraft, excludeInternalUUID string) (*service.DuplicatePrecheckPrompt, error)
	CreateBeneficiary(ctx context.Context, draft service.BeneficiaryDraft, opts service.CreateOptions) (*model.Beneficiary, error)
}

type importRepository interface {
	repository.BeneficiaryRepository
	repository.LogRepository
	repository.PSGCRepository
}

