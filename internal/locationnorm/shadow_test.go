package locationnorm

import (
	"testing"

	"dedup/internal/model"
)

func TestRunShadowComparisonReportsDriftAndVersion(t *testing.T) {
	t.Parallel()

	normalizer := testNormalizer(t)
	rows := []ShadowRowInput{
		{
			RowNumber:       1,
			SourceReference: "row-1",
			Raw: model.LocationChainRaw{
				Region:   "Region One",
				Province: "Alpha Provnce",
				City:     "Santa Cruz Cty",
				Barangay: "Sto Nino",
			},
		},
		{
			RowNumber:       2,
			SourceReference: "row-2",
			Raw: model.LocationChainRaw{
				Region:   "01",
				Province: "0101",
				City:     "010101",
				Barangay: "010101001",
			},
		},
	}

	legacyResolver := func(raw model.LocationChainRaw) (model.LocationChainResolved, bool, error) {
		if raw.Region == "01" && raw.Province == "0101" && raw.City == "010101" && raw.Barangay == "010101001" {
			return model.LocationChainResolved{
				RegionCode:   "01",
				RegionName:   "Region One",
				ProvinceCode: "0101",
				ProvinceName: "Alpha Province",
				CityCode:     "010101",
				CityName:     "Santa Cruz City",
				BarangayCode: "010101001",
				BarangayName: "Sto. Niño",
			}, true, nil
		}
		return model.LocationChainResolved{}, false, nil
	}

	report, err := RunShadowComparison(normalizer, rows, legacyResolver)
	if err != nil {
		t.Fatalf("run shadow comparison: %v", err)
	}

	if report.NormalizationVersion != NormalizationVersion {
		t.Fatalf("expected normalization version %q, got %q", NormalizationVersion, report.NormalizationVersion)
	}
	if report.TotalRows != 2 || report.AutoApplyRows != 2 || report.ReviewRows != 0 {
		t.Fatalf("unexpected row totals: %+v", report)
	}
	if report.DriftRows != 1 {
		t.Fatalf("expected one drift row, got %d", report.DriftRows)
	}
	if !report.Rows[0].HasDrift || len(report.Rows[0].DriftFields) != 1 || report.Rows[0].DriftFields[0] != "apply_state" {
		t.Fatalf("expected apply_state drift for fuzzy-only row, got %+v", report.Rows[0])
	}
	if report.Rows[1].HasDrift {
		t.Fatalf("expected exact row to have no drift, got %+v", report.Rows[1])
	}
}
