package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"dedup/internal/db"
	"dedup/internal/model"
	"dedup/internal/repository"
)

func TestLocationBackfillDryRunWritesLineageWithoutMutatingBeneficiaries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixed := time.Date(2026, time.March, 25, 19, 0, 0, 0, time.UTC)
	repo, database, cleanup := newBackfillTestRepository(t)
	defer cleanup()

	if err := seedBackfillPSGC(ctx, database); err != nil {
		t.Fatalf("seed PSGC hierarchy: %v", err)
	}

	beneficiary := backfillBeneficiaryFixture("uuid-dry-run", "G-900001", "Dela Cruz", "Ana")
	beneficiary.RegionCode = ""
	beneficiary.ProvinceCode = ""
	beneficiary.CityCode = ""
	beneficiary.BarangayCode = ""
	beneficiary.RegionName = "Region One"
	beneficiary.ProvinceName = "Alpha Province"
	beneficiary.CityName = "Santa Cruz Cty"
	beneficiary.BarangayName = "Sto Nino"
	if err := repo.CreateBeneficiary(ctx, &beneficiary); err != nil {
		t.Fatalf("seed beneficiary: %v", err)
	}

	svc, err := NewLocationBackfillService(repo, WithLocationBackfillClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	result, err := svc.NormalizeExistingBeneficiaries(ctx, NormalizeExistingBeneficiariesRequest{
		DryRun:          true,
		SourceReference: "backfill-dry-run",
	})
	if err != nil {
		t.Fatalf("run dry backfill: %v", err)
	}

	if result.Mode != model.LocationNormalizationModeShadow {
		t.Fatalf("expected SHADOW mode, got %s", result.Mode)
	}
	if result.TotalRows != 1 || result.AutoAppliedRows != 1 || result.UpdatedRows != 0 || result.ReviewRows != 0 {
		t.Fatalf("unexpected dry-run counts: %+v", result)
	}

	stored, err := repo.GetBeneficiary(ctx, beneficiary.InternalUUID)
	if err != nil {
		t.Fatalf("get beneficiary after dry-run: %v", err)
	}
	if stored.CityCode != "" || stored.BarangayCode != "" {
		t.Fatalf("expected dry-run to keep location codes unchanged, got city=%q barangay=%q", stored.CityCode, stored.BarangayCode)
	}

	runs, err := repo.ListLocationNormalizationRuns(ctx, repository.LocationNormalizationRunListQuery{
		Mode:  string(model.LocationNormalizationModeShadow),
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list normalization runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != result.RunID {
		t.Fatalf("unexpected dry-run lineage runs: %+v", runs)
	}

	items, err := repo.ListLocationNormalizationItems(ctx, repository.LocationNormalizationItemListQuery{
		RunID: result.RunID,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list normalization items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 normalization item, got %d", len(items))
	}
	if items[0].Status != model.LocationNormalizationStatusAutoApplied || items[0].NeedsReview {
		t.Fatalf("expected auto-applied dry-run item, got %+v", items[0])
	}
}

func TestLocationBackfillApplyUpdatesOnlyFullChainsAndFlagsPartialRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixed := time.Date(2026, time.March, 25, 20, 0, 0, 0, time.UTC)
	repo, database, cleanup := newBackfillTestRepository(t)
	defer cleanup()

	if err := seedBackfillPSGC(ctx, database); err != nil {
		t.Fatalf("seed PSGC hierarchy: %v", err)
	}

	fixable := backfillBeneficiaryFixture("uuid-apply-fixable", "G-900101", "Reyes", "Bella")
	fixable.SourceReference = backfillStringPtr("test-source-fixable")
	fixable.RegionCode = ""
	fixable.ProvinceCode = ""
	fixable.CityCode = ""
	fixable.BarangayCode = ""
	fixable.RegionName = "Region One"
	fixable.ProvinceName = "Alpha Province"
	fixable.CityName = "Santa Cruz Cty"
	fixable.BarangayName = "Sto Nino"
	if err := repo.CreateBeneficiary(ctx, &fixable); err != nil {
		t.Fatalf("seed fixable beneficiary: %v", err)
	}

	partial := backfillBeneficiaryFixture("uuid-apply-partial", "G-900102", "Santos", "Carlo")
	partial.SourceReference = backfillStringPtr("test-source-partial")
	partial.RegionCode = ""
	partial.RegionName = ""
	partial.ProvinceCode = ""
	partial.ProvinceName = ""
	partial.CityCode = ""
	partial.CityName = "Santa Cruz City"
	partial.BarangayCode = ""
	partial.BarangayName = "Sto. Niño"
	partial.SourceReference = backfillStringPtr("test-source-partial")
	if err := repo.CreateBeneficiary(ctx, &partial); err != nil {
		t.Fatalf("seed partial beneficiary: %v", err)
	}

	svc, err := NewLocationBackfillService(repo, WithLocationBackfillClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	result, err := svc.NormalizeExistingBeneficiaries(ctx, NormalizeExistingBeneficiariesRequest{
		DryRun:          false,
		SourceReference: "backfill-apply",
	})
	if err != nil {
		t.Fatalf("run apply backfill: %v", err)
	}

	if result.Mode != model.LocationNormalizationModeWrite {
		t.Fatalf("expected WRITE mode, got %s", result.Mode)
	}
	if result.TotalRows != 2 || result.AutoAppliedRows != 1 || result.ReviewRows != 1 || result.UpdatedRows != 1 {
		t.Fatalf("unexpected apply counts: %+v", result)
	}

	storedFixable, err := repo.GetBeneficiary(ctx, fixable.InternalUUID)
	if err != nil {
		t.Fatalf("get fixable beneficiary: %v", err)
	}
	if storedFixable.RegionCode != "01" || storedFixable.ProvinceCode != "0101" || storedFixable.CityCode != "010101" || storedFixable.BarangayCode != "010101001" {
		t.Fatalf("expected fully corrected PSGC chain, got %+v", storedFixable)
	}
	if storedFixable.CityName != "Santa Cruz City" || storedFixable.BarangayName != "Sto. Niño" {
		t.Fatalf("expected canonical location names after apply, got city=%q barangay=%q", storedFixable.CityName, storedFixable.BarangayName)
	}

	storedPartial, err := repo.GetBeneficiary(ctx, partial.InternalUUID)
	if err != nil {
		t.Fatalf("get partial beneficiary: %v", err)
	}
	if storedPartial.RegionCode != "" || storedPartial.ProvinceCode != "" {
		t.Fatalf("expected partial chain row to remain unchanged, got region=%q province=%q", storedPartial.RegionCode, storedPartial.ProvinceCode)
	}

	items, err := repo.ListLocationNormalizationItems(ctx, repository.LocationNormalizationItemListQuery{
		RunID: result.RunID,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list normalization items: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 normalization items, got %d", len(items))
	}

	reviewCount := 0
	for _, item := range items {
		if item.Status == model.LocationNormalizationStatusReviewNeeded {
			reviewCount++
			if item.Reason == nil || *item.Reason != "raw location chain incomplete" {
				t.Fatalf("expected partial-chain review reason, got %+v", item.Reason)
			}
		}
	}
	if reviewCount != 1 {
		t.Fatalf("expected one review-needed lineage item, got %d", reviewCount)
	}
}

func newBackfillTestRepository(t *testing.T) (*repository.Repository, *sql.DB, func()) {
	t.Helper()

	handle, err := db.OpenAndMigrate(context.Background(), t.TempDir()+"\\location-backfill-test.db")
	if err != nil {
		t.Fatalf("open and migrate test db: %v", err)
	}
	repo, err := repository.New(handle.DB, handle.Writer)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new repository: %v", err)
	}
	return repo, handle.DB, func() { _ = handle.DB.Close() }
}

func seedBackfillPSGC(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return fmt.Errorf("database is nil")
	}

	queries := []string{
		`INSERT INTO psgc_regions (region_code, region_name) VALUES ('01', 'Region One');`,
		`INSERT INTO psgc_provinces (province_code, region_code, province_name) VALUES ('0101', '01', 'Alpha Province');`,
		`INSERT INTO psgc_cities (city_code, region_code, province_code, city_name, city_type) VALUES ('010101', '01', '0101', 'Santa Cruz City', 'CITY');`,
		`INSERT INTO psgc_barangays (barangay_code, region_code, province_code, city_code, barangay_name) VALUES ('010101001', '01', '0101', '010101', 'Sto. Niño');`,
	}
	for _, query := range queries {
		if _, err := database.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

func backfillBeneficiaryFixture(internalUUID, generatedID, lastName, firstName string) model.Beneficiary {
	month := int64(3)
	day := int64(25)
	year := int64(1990)
	now := "2026-03-25T10:00:00Z"
	return model.Beneficiary{
		InternalUUID:      internalUUID,
		GeneratedID:       generatedID,
		LastName:          lastName,
		FirstName:         firstName,
		MiddleName:        backfillStringPtr("M"),
		ExtensionName:     nil,
		NormLastName:      strings.ToUpper(lastName),
		NormFirstName:     strings.ToUpper(firstName),
		NormMiddleName:    backfillStringPtr("M"),
		NormExtensionName: nil,
		RegionCode:        "01",
		RegionName:        "Region One",
		ProvinceCode:      "0101",
		ProvinceName:      "Alpha Province",
		CityCode:          "010101",
		CityName:          "Santa Cruz City",
		BarangayCode:      "010101001",
		BarangayName:      "Sto. Niño",
		ContactNo:         backfillStringPtr("09171234567"),
		ContactNoNorm:     backfillStringPtr("09171234567"),
		BirthMonth:        &month,
		BirthDay:          &day,
		BirthYear:         &year,
		BirthdateISO:      backfillStringPtr("1990-03-25"),
		Sex:               "F",
		RecordStatus:      model.RecordStatusActive,
		DedupStatus:       model.DedupStatusPossibleDuplicate,
		SourceType:        model.BeneficiarySourceImport,
		SourceReference:   backfillStringPtr("test-source"),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}
