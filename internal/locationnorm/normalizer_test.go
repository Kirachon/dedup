package locationnorm

import (
	"testing"

	"dedup/internal/model"
)

func TestNormalizeChainExactCodeMatchAutoApplies(t *testing.T) {
	t.Parallel()

	normalizer := testNormalizer(t)
	result := normalizer.NormalizeChain(model.LocationChainRaw{
		Region:   "01",
		Province: "0101",
		City:     "010101",
		Barangay: "010101001",
	})

	if !result.AutoApply || result.NeedsReview {
		t.Fatalf("expected exact code chain to auto-apply, got %+v", result)
	}
	if result.Status != model.LocationNormalizationStatusAutoApplied {
		t.Fatalf("expected AUTO_APPLIED status, got %s", result.Status)
	}
	if result.MatchSource != model.LocationMatchSourceExact {
		t.Fatalf("expected EXACT match source, got %s", result.MatchSource)
	}
	if result.Confidence != 1 {
		t.Fatalf("expected confidence 1 for exact code chain, got %.6f", result.Confidence)
	}
}

func TestNormalizeChainFuzzyMatchAboveCutoffAutoApplies(t *testing.T) {
	t.Parallel()

	normalizer := testNormalizer(t)
	result := normalizer.NormalizeChain(model.LocationChainRaw{
		Region:   "Region One",
		Province: "Alpha Provnce",
		City:     "Santa Cruz Cty",
		Barangay: "Sto Nino",
	})

	if !result.AutoApply {
		t.Fatalf("expected fuzzy chain above cutoff to auto-apply, got %+v", result)
	}
	if result.Confidence < 0.95 {
		t.Fatalf("expected confidence >= 0.95, got %.6f", result.Confidence)
	}
	if result.Resolved.BarangayName != "Sto. Niño" {
		t.Fatalf("expected punctuation-preserving barangay name, got %q", result.Resolved.BarangayName)
	}
}

func TestNormalizeChainRegionAliasAutoApplies(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(
		[]model.PSGCRegion{
			{RegionCode: "02", RegionName: "Region II"},
		},
		[]model.PSGCProvince{
			{ProvinceCode: "0201", RegionCode: "02", ProvinceName: "Cagayan"},
		},
		[]model.PSGCCity{
			{CityCode: "020101", RegionCode: "02", ProvinceCode: stringPtr("0201"), CityName: "Tuguegarao City"},
		},
		[]model.PSGCBarangay{
			{BarangayCode: "020101001", RegionCode: "02", ProvinceCode: stringPtr("0201"), CityCode: "020101", BarangayName: "Caggay"},
		},
	)
	if err != nil {
		t.Fatalf("build alias test catalog: %v", err)
	}

	normalizer, err := New(catalog)
	if err != nil {
		t.Fatalf("create alias test normalizer: %v", err)
	}

	result := normalizer.NormalizeChain(model.LocationChainRaw{
		Region:   "ii",
		Province: "CAGAYAN",
		City:     "TUGUEGARAO CITY",
		Barangay: "CAGGAY",
	})

	if !result.AutoApply || result.NeedsReview {
		t.Fatalf("expected roman-numeral region alias to auto-apply, got %+v", result)
	}
	if result.Resolved.RegionName != "Region II" {
		t.Fatalf("expected region alias to canonicalize to Region II, got %q", result.Resolved.RegionName)
	}
}

func TestNormalizeChainAmbiguousTieNeedsReview(t *testing.T) {
	t.Parallel()

	normalizer := testNormalizer(t)
	result := normalizer.NormalizeChain(model.LocationChainRaw{
		Region:   "Region One",
		City:     "San Jose City",
		Barangay: "Poblacion",
	})

	if result.AutoApply || !result.NeedsReview {
		t.Fatalf("expected ambiguous tie to require review, got %+v", result)
	}
	if result.Status != model.LocationNormalizationStatusReviewNeeded {
		t.Fatalf("expected REVIEW_NEEDED status, got %s", result.Status)
	}
	if result.Reason != "ambiguous location candidates" {
		t.Fatalf("expected ambiguous reason, got %q", result.Reason)
	}
}

func TestNormalizeChainParentMismatchNeedsReview(t *testing.T) {
	t.Parallel()

	normalizer := testNormalizer(t)
	result := normalizer.NormalizeChain(model.LocationChainRaw{
		Region:   "01",
		Province: "0101",
		City:     "010201",
		Barangay: "010201001",
	})

	if result.AutoApply || !result.NeedsReview {
		t.Fatalf("expected parent-mismatch chain to require review, got %+v", result)
	}
	if result.Status != model.LocationNormalizationStatusReviewNeeded {
		t.Fatalf("expected REVIEW_NEEDED status, got %s", result.Status)
	}
	if result.Reason == "" {
		t.Fatalf("expected mismatch reason to be populated")
	}
}

func testNormalizer(t *testing.T) *LocationNormalizer {
	t.Helper()

	catalog, err := NewCatalog(
		[]model.PSGCRegion{
			{RegionCode: "01", RegionName: "Region One"},
		},
		[]model.PSGCProvince{
			{ProvinceCode: "0101", RegionCode: "01", ProvinceName: "Alpha Province"},
			{ProvinceCode: "0102", RegionCode: "01", ProvinceName: "Beta Province"},
		},
		[]model.PSGCCity{
			{CityCode: "010101", RegionCode: "01", ProvinceCode: stringPtr("0101"), CityName: "Santa Cruz City"},
			{CityCode: "010102", RegionCode: "01", ProvinceCode: stringPtr("0101"), CityName: "San Jose City"},
			{CityCode: "010201", RegionCode: "01", ProvinceCode: stringPtr("0102"), CityName: "San Jose City"},
		},
		[]model.PSGCBarangay{
			{BarangayCode: "010101001", RegionCode: "01", ProvinceCode: stringPtr("0101"), CityCode: "010101", BarangayName: "Sto. Niño"},
			{BarangayCode: "010102001", RegionCode: "01", ProvinceCode: stringPtr("0101"), CityCode: "010102", BarangayName: "Poblacion"},
			{BarangayCode: "010201001", RegionCode: "01", ProvinceCode: stringPtr("0102"), CityCode: "010201", BarangayName: "Poblacion"},
		},
	)
	if err != nil {
		t.Fatalf("build test catalog: %v", err)
	}

	normalizer, err := New(catalog)
	if err != nil {
		t.Fatalf("create test normalizer: %v", err)
	}
	return normalizer
}

func stringPtr(v string) *string {
	return &v
}
