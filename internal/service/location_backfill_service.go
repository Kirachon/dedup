package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"dedup/internal/locationnorm"
	"dedup/internal/model"
	"dedup/internal/repository"

	"github.com/google/uuid"
)

const (
	defaultBackfillPageSize = 250

	backfillRunStatusCompleted = "COMPLETED"
	backfillRunStatusFailed    = "FAILED"
)

// LocationBackfillOption configures LocationBackfillService behavior.
type LocationBackfillOption func(*LocationBackfillService)

// NormalizeExistingBeneficiariesRequest controls how backfill scans and applies updates.
type NormalizeExistingBeneficiariesRequest struct {
	DryRun          bool
	IncludeDeleted  bool
	Search          string
	SourceReference string
	Limit           int
}

// BackfillReport summarizes one backfill run.
type BackfillReport struct {
	RunID                string
	Mode                 model.LocationNormalizationMode
	Status               string
	NormalizationVersion string
	TotalRows            int
	AutoAppliedRows      int
	ReviewRows           int
	FailedRows           int
	UpdatedRows          int
	StartedAt            string
	CompletedAt          string
}

// LocationBackfillService reprocesses existing beneficiary location chains safely.
type LocationBackfillService struct {
	repo       *repository.Repository
	normalizer *locationnorm.LocationNormalizer
	now        func() time.Time
	pageSize   int
}

// WithLocationBackfillClock overrides the service clock for deterministic tests.
func WithLocationBackfillClock(clock func() time.Time) LocationBackfillOption {
	return func(s *LocationBackfillService) {
		if clock != nil {
			s.now = clock
		}
	}
}

// WithLocationBackfillPageSize overrides beneficiary scan page size.
func WithLocationBackfillPageSize(size int) LocationBackfillOption {
	return func(s *LocationBackfillService) {
		if size > 0 {
			s.pageSize = size
		}
	}
}

// WithLocationBackfillNormalizer injects a prebuilt normalizer.
func WithLocationBackfillNormalizer(normalizer *locationnorm.LocationNormalizer) LocationBackfillOption {
	return func(s *LocationBackfillService) {
		if normalizer != nil {
			s.normalizer = normalizer
		}
	}
}

// NewLocationBackfillService constructs a backfill service from repository + PSGC catalog data.
func NewLocationBackfillService(repo *repository.Repository, opts ...LocationBackfillOption) (*LocationBackfillService, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is nil")
	}

	svc := &LocationBackfillService{
		repo: repo,
		now: func() time.Time {
			return time.Now().UTC()
		},
		pageSize: defaultBackfillPageSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}

	if svc.normalizer == nil {
		normalizer, err := buildLocationNormalizerFromRepository(context.Background(), repo)
		if err != nil {
			return nil, err
		}
		svc.normalizer = normalizer
	}

	return svc, nil
}

// NormalizeExistingBeneficiaries runs dry-run/apply normalization against persisted rows.
func (s *LocationBackfillService) NormalizeExistingBeneficiaries(ctx context.Context, req NormalizeExistingBeneficiariesRequest) (*BackfillReport, error) {
	if s == nil {
		return nil, fmt.Errorf("location backfill service is nil")
	}
	if s.repo == nil {
		return nil, fmt.Errorf("repository is nil")
	}
	if s.normalizer == nil {
		return nil, fmt.Errorf("location normalizer is nil")
	}
	if s.now == nil {
		return nil, fmt.Errorf("clock is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	limit := req.Limit
	if limit < 0 {
		return nil, fmt.Errorf("limit must be >= 0")
	}

	mode := model.LocationNormalizationModeWrite
	if req.DryRun {
		mode = model.LocationNormalizationModeShadow
	}
	sourceReference := strings.TrimSpace(req.SourceReference)
	if sourceReference == "" {
		sourceReference = "beneficiary-location-backfill"
	}

	startedAt := s.now().UTC().Format(time.RFC3339Nano)
	report := &BackfillReport{
		RunID:                uuid.NewString(),
		Mode:                 mode,
		Status:               backfillRunStatusCompleted,
		NormalizationVersion: locationnorm.NormalizationVersion,
		StartedAt:            startedAt,
	}

	err := s.repo.WithinTx(ctx, func(txRepo *repository.Repository) error {
		run := &model.LocationNormalizationRun{
			RunID:                report.RunID,
			SourceReference:      backfillStringPtr(sourceReference),
			Mode:                 mode,
			Status:               "RUNNING",
			NormalizationVersion: report.NormalizationVersion,
			TotalRows:            0,
			AutoAppliedRows:      0,
			ReviewRows:           0,
			FailedRows:           0,
			StartedAt:            startedAt,
		}
		if err := txRepo.CreateLocationNormalizationRun(ctx, run); err != nil {
			return fmt.Errorf("create location normalization run: %w", err)
		}

		offset := 0
		remaining := limit
		rowNumber := 1

		for {
			if limit > 0 && remaining <= 0 {
				break
			}

			pageSize := s.pageSize
			if pageSize <= 0 {
				pageSize = defaultBackfillPageSize
			}
			if limit > 0 && remaining < pageSize {
				pageSize = remaining
			}

			page, err := txRepo.ListBeneficiaries(ctx, repository.BeneficiaryListQuery{
				Search:         strings.TrimSpace(req.Search),
				IncludeDeleted: req.IncludeDeleted,
				Limit:          pageSize,
				Offset:         offset,
			})
			if err != nil {
				return fmt.Errorf("list beneficiaries for backfill: %w", err)
			}
			if len(page.Items) == 0 {
				break
			}

			for _, beneficiary := range page.Items {
				if limit > 0 && remaining <= 0 {
					break
				}

				normalized := s.normalizer.NormalizeChain(rawChainFromBeneficiary(beneficiary))
				if normalized.AutoApply && !rawChainHasAllFields(normalized.Raw) {
					normalized = forceReview(normalized, "raw location chain incomplete")
				}

				item := toLocationNormalizationItem(
					report.RunID,
					rowNumber,
					beneficiary,
					normalized,
					s.now().UTC().Format(time.RFC3339Nano),
				)
				if err := txRepo.CreateLocationNormalizationItem(ctx, item); err != nil {
					return fmt.Errorf("create location normalization item: %w", err)
				}

				report.TotalRows++
				if normalized.AutoApply {
					report.AutoAppliedRows++
					if !req.DryRun && locationChainDiffers(beneficiary, normalized.Resolved) {
						updated := beneficiary
						applyResolvedChain(&updated, normalized.Resolved)
						updated.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
						if err := txRepo.UpdateBeneficiary(ctx, &updated); err != nil {
							return fmt.Errorf("apply normalized location for %s: %w", beneficiary.InternalUUID, err)
						}
						report.UpdatedRows++
					}
				} else {
					report.ReviewRows++
				}

				rowNumber++
				if limit > 0 {
					remaining--
				}
			}

			offset += len(page.Items)
			if offset >= page.Total {
				break
			}
		}

		completedAt := s.now().UTC().Format(time.RFC3339Nano)
		report.CompletedAt = completedAt
		run.Status = report.Status
		run.TotalRows = report.TotalRows
		run.AutoAppliedRows = report.AutoAppliedRows
		run.ReviewRows = report.ReviewRows
		run.FailedRows = report.FailedRows
		run.CompletedAt = &completedAt
		if err := txRepo.UpdateLocationNormalizationRun(ctx, run); err != nil {
			return fmt.Errorf("complete location normalization run: %w", err)
		}

		return nil
	})
	if err != nil {
		report.Status = backfillRunStatusFailed
		report.FailedRows++
		report.CompletedAt = s.now().UTC().Format(time.RFC3339Nano)
		return nil, err
	}

	return report, nil
}

func buildLocationNormalizerFromRepository(ctx context.Context, repo repository.PSGCRepository) (*locationnorm.LocationNormalizer, error) {
	regions, err := repo.ListRegions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list PSGC regions: %w", err)
	}

	provinces := make([]model.PSGCProvince, 0)
	cities := make([]model.PSGCCity, 0)
	barangays := make([]model.PSGCBarangay, 0)

	for _, region := range regions {
		regionProvinces, err := repo.ListProvincesByRegion(ctx, region.RegionCode)
		if err != nil {
			return nil, fmt.Errorf("list PSGC provinces for region %s: %w", region.RegionCode, err)
		}
		provinces = append(provinces, regionProvinces...)

		regionCities, err := repo.ListCitiesByRegion(ctx, region.RegionCode)
		if err != nil {
			return nil, fmt.Errorf("list PSGC cities for region %s: %w", region.RegionCode, err)
		}
		cities = append(cities, regionCities...)

		for _, city := range regionCities {
			cityBarangays, err := repo.ListBarangaysByCity(ctx, city.CityCode)
			if err != nil {
				return nil, fmt.Errorf("list PSGC barangays for city %s: %w", city.CityCode, err)
			}
			barangays = append(barangays, cityBarangays...)
		}
	}

	catalog, err := locationnorm.NewCatalog(regions, provinces, cities, barangays)
	if err != nil {
		return nil, err
	}
	return locationnorm.New(catalog)
}

func toLocationNormalizationItem(
	runID string,
	rowNumber int,
	beneficiary model.Beneficiary,
	normalized model.LocationNormalizationResult,
	createdAt string,
) *model.LocationNormalizationItem {
	sourceReference := strings.TrimSpace(beneficiary.GeneratedID)
	if sourceReference == "" {
		sourceReference = strings.TrimSpace(beneficiary.InternalUUID)
	}
	if beneficiary.SourceReference != nil && strings.TrimSpace(*beneficiary.SourceReference) != "" {
		sourceReference = strings.TrimSpace(*beneficiary.SourceReference)
	}

	return &model.LocationNormalizationItem{
		ItemID:               uuid.NewString(),
		RunID:                runID,
		RowNumber:            rowNumber,
		SourceReference:      backfillStringPtr(sourceReference),
		RawRegion:            strings.TrimSpace(normalized.Raw.Region),
		RawProvince:          strings.TrimSpace(normalized.Raw.Province),
		RawCity:              strings.TrimSpace(normalized.Raw.City),
		RawBarangay:          strings.TrimSpace(normalized.Raw.Barangay),
		ResolvedRegionCode:   backfillStringPtr(strings.TrimSpace(normalized.Resolved.RegionCode)),
		ResolvedRegionName:   backfillStringPtr(strings.TrimSpace(normalized.Resolved.RegionName)),
		ResolvedProvinceCode: backfillStringPtr(strings.TrimSpace(normalized.Resolved.ProvinceCode)),
		ResolvedProvinceName: backfillStringPtr(strings.TrimSpace(normalized.Resolved.ProvinceName)),
		ResolvedCityCode:     backfillStringPtr(strings.TrimSpace(normalized.Resolved.CityCode)),
		ResolvedCityName:     backfillStringPtr(strings.TrimSpace(normalized.Resolved.CityName)),
		ResolvedBarangayCode: backfillStringPtr(strings.TrimSpace(normalized.Resolved.BarangayCode)),
		ResolvedBarangayName: backfillStringPtr(strings.TrimSpace(normalized.Resolved.BarangayName)),
		Confidence:           normalized.Confidence,
		MatchSource:          normalized.MatchSource,
		Status:               normalized.Status,
		NeedsReview:          normalized.NeedsReview,
		Reason:               backfillStringPtr(strings.TrimSpace(normalized.Reason)),
		NormalizationVersion: normalized.NormalizationVersion,
		CreatedAt:            createdAt,
	}
}

func rawChainFromBeneficiary(item model.Beneficiary) model.LocationChainRaw {
	return model.LocationChainRaw{
		Region:   firstNonEmpty(item.RegionName, item.RegionCode),
		Province: firstNonEmpty(item.ProvinceName, item.ProvinceCode),
		City:     firstNonEmpty(item.CityName, item.CityCode),
		Barangay: firstNonEmpty(item.BarangayName, item.BarangayCode),
	}
}

func rawChainHasAllFields(raw model.LocationChainRaw) bool {
	return strings.TrimSpace(raw.Region) != "" &&
		strings.TrimSpace(raw.Province) != "" &&
		strings.TrimSpace(raw.City) != "" &&
		strings.TrimSpace(raw.Barangay) != ""
}

func forceReview(result model.LocationNormalizationResult, reason string) model.LocationNormalizationResult {
	result.Status = model.LocationNormalizationStatusReviewNeeded
	result.NeedsReview = true
	result.AutoApply = false
	result.Reason = strings.TrimSpace(reason)
	return result
}

func locationChainDiffers(item model.Beneficiary, resolved model.LocationChainResolved) bool {
	return strings.TrimSpace(item.RegionCode) != strings.TrimSpace(resolved.RegionCode) ||
		strings.TrimSpace(item.RegionName) != strings.TrimSpace(resolved.RegionName) ||
		strings.TrimSpace(item.ProvinceCode) != strings.TrimSpace(resolved.ProvinceCode) ||
		strings.TrimSpace(item.ProvinceName) != strings.TrimSpace(resolved.ProvinceName) ||
		strings.TrimSpace(item.CityCode) != strings.TrimSpace(resolved.CityCode) ||
		strings.TrimSpace(item.CityName) != strings.TrimSpace(resolved.CityName) ||
		strings.TrimSpace(item.BarangayCode) != strings.TrimSpace(resolved.BarangayCode) ||
		strings.TrimSpace(item.BarangayName) != strings.TrimSpace(resolved.BarangayName)
}

func applyResolvedChain(item *model.Beneficiary, resolved model.LocationChainResolved) {
	item.RegionCode = strings.TrimSpace(resolved.RegionCode)
	item.RegionName = strings.TrimSpace(resolved.RegionName)
	item.ProvinceCode = strings.TrimSpace(resolved.ProvinceCode)
	item.ProvinceName = strings.TrimSpace(resolved.ProvinceName)
	item.CityCode = strings.TrimSpace(resolved.CityCode)
	item.CityName = strings.TrimSpace(resolved.CityName)
	item.BarangayCode = strings.TrimSpace(resolved.BarangayCode)
	item.BarangayName = strings.TrimSpace(resolved.BarangayName)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func backfillStringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
