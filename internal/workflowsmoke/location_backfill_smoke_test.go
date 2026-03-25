package workflowsmoke

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appcore "dedup/internal/app"
	"dedup/internal/config"
	"dedup/internal/model"
	"dedup/internal/repository"
	"dedup/internal/service"
)

func TestLocationBackfillSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	repoRoot := mustRepoRoot(t)
	psgcPath := filepath.Join(repoRoot, smokePSGCCSVFileName)
	if _, err := os.Stat(psgcPath); err != nil {
		t.Fatalf("psgc csv missing at %s: %v", psgcPath, err)
	}

	dbPath := filepath.Join(t.TempDir(), "location-backfill-smoke.db")
	bootstrap, err := appcore.BootstrapDatabase(ctx, config.Config{
		AppID:       "ph.lgu.beneficiary",
		WindowTitle: "Offline Beneficiary Tool (Location Backfill Smoke)",
		DBPath:      dbPath,
		PSGCCSVPath: psgcPath,
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	defer func() { _ = bootstrap.DB.Close() }()

	repo, err := repository.New(bootstrap.DB, bootstrap.Writer)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	location := mustSmokeLocation(t, ctx, repo)

	clock := time.Date(2026, time.March, 25, 21, 0, 0, 0, time.UTC)
	beneficiarySvc, err := service.NewBeneficiaryService(
		bootstrap.DB,
		bootstrap.Writer,
		repo,
		service.WithClock(func() time.Time { return clock }),
	)
	if err != nil {
		t.Fatalf("beneficiary service: %v", err)
	}

	created, err := beneficiarySvc.CreateBeneficiary(ctx, service.BeneficiaryDraft{
		LastName:      "Salazar",
		FirstName:     "Mina",
		MiddleName:    "P",
		ExtensionName: "",
		RegionCode:    location.RegionCode,
		RegionName:    location.RegionName,
		ProvinceCode:  location.ProvinceCode,
		ProvinceName:  location.ProvinceName,
		CityCode:      location.CityCode,
		CityName:      location.CityName,
		BarangayCode:  location.BarangayCode,
		BarangayName:  location.BarangayName,
		ContactNo:     "09170000123",
		BirthMonth:    int64Ptr(2),
		BirthDay:      int64Ptr(3),
		BirthYear:     int64Ptr(1992),
		Sex:           "F",
	}, service.CreateOptions{
		SourceType:      model.BeneficiarySourceImport,
		SourceReference: "location-backfill-smoke-row-1",
		RecordStatus:    model.RecordStatusActive,
		DedupStatus:     model.DedupStatusClear,
	})
	if err != nil {
		t.Fatalf("seed beneficiary: %v", err)
	}

	mutated := *created
	mutated.RegionCode = ""
	mutated.ProvinceCode = ""
	mutated.CityCode = ""
	mutated.BarangayCode = ""
	mutated.CityName = strings.Replace(location.CityName, "City", "Cty", 1)
	if mutated.CityName == location.CityName {
		mutated.CityName = location.CityName + " Cty"
	}
	mutated.BarangayName = strings.Replace(location.BarangayName, "Sto.", "Sto", 1)
	mutated.UpdatedAt = clock.Add(1 * time.Minute).Format(time.RFC3339Nano)
	if err := repo.UpdateBeneficiary(ctx, &mutated); err != nil {
		t.Fatalf("mutate beneficiary: %v", err)
	}

	backfillSvc, err := service.NewLocationBackfillService(
		repo,
		service.WithLocationBackfillClock(func() time.Time { return clock.Add(2 * time.Minute) }),
	)
	if err != nil {
		t.Fatalf("new location backfill service: %v", err)
	}

	dryRun, err := backfillSvc.NormalizeExistingBeneficiaries(ctx, service.NormalizeExistingBeneficiariesRequest{
		DryRun:          true,
		IncludeDeleted:  false,
		SourceReference: "location-backfill-smoke-dry-run",
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("dry-run backfill: %v", err)
	}
	if dryRun.AutoAppliedRows < 1 || dryRun.UpdatedRows != 0 {
		t.Fatalf("unexpected dry-run backfill report: %+v", dryRun)
	}

	apply, err := backfillSvc.NormalizeExistingBeneficiaries(ctx, service.NormalizeExistingBeneficiariesRequest{
		DryRun:          false,
		IncludeDeleted:  false,
		SourceReference: "location-backfill-smoke-apply",
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("apply backfill: %v", err)
	}
	if apply.UpdatedRows < 1 {
		t.Fatalf("expected applied backfill updates, got %+v", apply)
	}

	stored, err := repo.GetBeneficiary(ctx, created.InternalUUID)
	if err != nil {
		t.Fatalf("reload backfilled beneficiary: %v", err)
	}
	if stored.RegionCode != location.RegionCode ||
		stored.ProvinceCode != location.ProvinceCode ||
		stored.CityCode != location.CityCode ||
		stored.BarangayCode != location.BarangayCode {
		t.Fatalf("expected canonical PSGC chain after backfill, got %+v", stored)
	}

	items, err := repo.ListLocationNormalizationItems(ctx, repository.LocationNormalizationItemListQuery{
		RunID: apply.RunID,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list backfill lineage items: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("expected lineage items for apply run %s", apply.RunID)
	}
}
