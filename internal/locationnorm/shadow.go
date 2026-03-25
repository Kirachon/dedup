package locationnorm

import (
	"fmt"

	"dedup/internal/model"
)

// LegacyExactResolver resolves canonical chains using the legacy exact-only strategy.
type LegacyExactResolver func(model.LocationChainRaw) (model.LocationChainResolved, bool, error)

// ShadowRowInput is one row fed into shadow-mode comparison.
type ShadowRowInput struct {
	RowNumber       int
	SourceReference string
	Raw             model.LocationChainRaw
}

// ShadowRowResult captures one deterministic shadow comparison row.
type ShadowRowResult struct {
	RowNumber       int
	SourceReference string
	Raw             model.LocationChainRaw
	LegacyResolved  *model.LocationChainResolved
	LegacyMatched   bool
	Normalized      model.LocationNormalizationResult
	HasDrift        bool
	DriftFields     []string
}

// ShadowReport summarizes shadow-mode diff output without mutating beneficiaries.
type ShadowReport struct {
	NormalizationVersion string
	TotalRows            int
	AutoApplyRows        int
	ReviewRows           int
	DriftRows            int
	Rows                 []ShadowRowResult
}

// RunShadowComparison compares legacy exact matching against the new normalizer.
func RunShadowComparison(normalizer *LocationNormalizer, rows []ShadowRowInput, legacyResolver LegacyExactResolver) (*ShadowReport, error) {
	if normalizer == nil {
		return nil, fmt.Errorf("normalizer is nil")
	}
	if legacyResolver == nil {
		return nil, fmt.Errorf("legacy resolver is nil")
	}

	report := &ShadowReport{
		NormalizationVersion: normalizer.version,
		Rows:                 make([]ShadowRowResult, 0, len(rows)),
	}

	for _, row := range rows {
		legacyResolved, legacyMatched, legacyErr := legacyResolver(row.Raw)
		if legacyErr != nil {
			return nil, fmt.Errorf("legacy resolve row %d: %w", row.RowNumber, legacyErr)
		}

		normalized := normalizer.NormalizeChain(row.Raw)
		rowResult := ShadowRowResult{
			RowNumber:       row.RowNumber,
			SourceReference: row.SourceReference,
			Raw:             sanitizeRawChain(row.Raw),
			LegacyMatched:   legacyMatched,
			Normalized:      normalized,
		}
		if legacyMatched {
			copyValue := legacyResolved
			rowResult.LegacyResolved = &copyValue
		}

		driftFields := driftFieldsForComparison(rowResult.LegacyResolved, normalized.Resolved, legacyMatched, normalized.AutoApply)
		rowResult.DriftFields = driftFields
		rowResult.HasDrift = len(driftFields) > 0

		report.TotalRows++
		if normalized.AutoApply {
			report.AutoApplyRows++
		} else {
			report.ReviewRows++
		}
		if rowResult.HasDrift {
			report.DriftRows++
		}
		report.Rows = append(report.Rows, rowResult)
	}

	return report, nil
}

func driftFieldsForComparison(
	legacyResolved *model.LocationChainResolved,
	normalized model.LocationChainResolved,
	legacyMatched bool,
	normalizedApplied bool,
) []string {
	drifts := make([]string, 0, 10)

	if legacyMatched != normalizedApplied {
		drifts = append(drifts, "apply_state")
	}

	if !legacyMatched || legacyResolved == nil || !normalizedApplied {
		return drifts
	}

	appendIfDifferent := func(name, left, right string) {
		if left != right {
			drifts = append(drifts, name)
		}
	}

	appendIfDifferent("region_code", legacyResolved.RegionCode, normalized.RegionCode)
	appendIfDifferent("region_name", legacyResolved.RegionName, normalized.RegionName)
	appendIfDifferent("province_code", legacyResolved.ProvinceCode, normalized.ProvinceCode)
	appendIfDifferent("province_name", legacyResolved.ProvinceName, normalized.ProvinceName)
	appendIfDifferent("city_code", legacyResolved.CityCode, normalized.CityCode)
	appendIfDifferent("city_name", legacyResolved.CityName, normalized.CityName)
	appendIfDifferent("barangay_code", legacyResolved.BarangayCode, normalized.BarangayCode)
	appendIfDifferent("barangay_name", legacyResolved.BarangayName, normalized.BarangayName)

	return drifts
}
