package service

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"dedup/internal/db"
	"dedup/internal/model"
	"dedup/internal/repository"
)

func TestNormalizeAndValidateDraftDeterministic(t *testing.T) {
	t.Parallel()

	svc := &BeneficiaryService{}
	draft := BeneficiaryDraft{
		LastName:      "  dela   Cruz ",
		FirstName:     "  juan ",
		MiddleName:    "  p  ",
		ExtensionName: "  jr ",
		RegionCode:    " 01 ",
		RegionName:    " region  one ",
		ProvinceCode:  "0101",
		ProvinceName:  " province   one",
		CityCode:      "010101",
		CityName:      " city   one ",
		BarangayCode:  "010101001",
		BarangayName:  " barangay   one ",
		ContactNo:     " +63 917-123-4567 ",
		BirthdateISO:  "1990-03-25",
		Sex:           " f ",
	}

	left, err := svc.NormalizeAndValidateDraft(draft)
	if err != nil {
		t.Fatalf("normalize left: %v", err)
	}
	right, err := svc.NormalizeAndValidateDraft(draft)
	if err != nil {
		t.Fatalf("normalize right: %v", err)
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("expected deterministic normalization, left=%+v right=%+v", left, right)
	}

	if left.LastName != "dela Cruz" || left.FirstName != "juan" {
		t.Fatalf("unexpected normalized names: %+v", left)
	}
	if left.NormLastName != "DELA CRUZ" || left.NormFirstName != "JUAN" {
		t.Fatalf("unexpected normalized match names: %+v", left)
	}
	if left.ContactNoNorm == nil || *left.ContactNoNorm != "639171234567" {
		t.Fatalf("unexpected contact normalization: %+v", left.ContactNoNorm)
	}
	if left.BirthdateISO == nil || *left.BirthdateISO != "1990-03-25" {
		t.Fatalf("unexpected birthdate normalization: %+v", left.BirthdateISO)
	}
}

func TestNormalizeAndValidateDraftRejectsMissingRequired(t *testing.T) {
	t.Parallel()

	svc := &BeneficiaryService{}
	_, err := svc.NormalizeAndValidateDraft(BeneficiaryDraft{})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "last_name is required") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestCreateBeneficiaryAllocatesSequentialVisibleIDs(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC)
	svc, _, cleanup := newTestService(t, WithClock(func() time.Time { return fixed }))
	defer cleanup()

	first, err := svc.CreateBeneficiary(context.Background(), sampleDraft("Alpha", "Anne"), CreateOptions{})
	if err != nil {
		t.Fatalf("create first beneficiary: %v", err)
	}
	second, err := svc.CreateBeneficiary(context.Background(), sampleDraft("Beta", "Ben"), CreateOptions{})
	if err != nil {
		t.Fatalf("create second beneficiary: %v", err)
	}

	if first.GeneratedID != "G-000001" || second.GeneratedID != "G-000002" {
		t.Fatalf("unexpected generated ids: first=%s second=%s", first.GeneratedID, second.GeneratedID)
	}
	if first.CreatedAt != fixed.Format(time.RFC3339Nano) || first.UpdatedAt != fixed.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected create timestamps: %+v", first)
	}
}

func TestCreateBeneficiaryImportCollisionAppendsProvenance(t *testing.T) {
	t.Parallel()

	svc, _, cleanup := newTestService(t)
	defer cleanup()

	_, err := svc.CreateBeneficiary(context.Background(), sampleDraft("Gamma", "Gia"), CreateOptions{
		PreferredGeneratedID: "IMP-0001",
		SourceType:           model.BeneficiarySourceImport,
		SourceReference:      "row-1",
	})
	if err != nil {
		t.Fatalf("seed import beneficiary: %v", err)
	}

	created, err := svc.CreateBeneficiary(context.Background(), sampleDraft("Delta", "Dio"), CreateOptions{
		PreferredGeneratedID: "IMP-0001",
		SourceType:           model.BeneficiarySourceImport,
		SourceReference:      "row-2",
	})
	if err != nil {
		t.Fatalf("create collided import beneficiary: %v", err)
	}

	if created.GeneratedID == "IMP-0001" {
		t.Fatalf("expected collision fallback generated id, got %s", created.GeneratedID)
	}
	if !strings.HasPrefix(created.GeneratedID, "G-") {
		t.Fatalf("expected local generated id prefix, got %s", created.GeneratedID)
	}
	if created.SourceReference == nil || !strings.Contains(*created.SourceReference, "source_generated_id=IMP-0001") {
		t.Fatalf("expected collision provenance in source reference, got %+v", created.SourceReference)
	}
	if !strings.Contains(*created.SourceReference, "row-2") {
		t.Fatalf("expected original source reference to be preserved, got %+v", created.SourceReference)
	}
}

func TestSoftDeleteBeneficiaryStampsDeletedAtAndUpdatedAt(t *testing.T) {
	t.Parallel()

	createdClock := time.Date(2026, time.March, 25, 13, 0, 0, 0, time.UTC)
	deleteClock := time.Date(2026, time.March, 25, 13, 30, 0, 0, time.UTC)

	svc, repo, cleanup := newTestService(t, WithClock(func() time.Time { return createdClock }))
	defer cleanup()

	created, err := svc.CreateBeneficiary(context.Background(), sampleDraft("Epsilon", "Eve"), CreateOptions{})
	if err != nil {
		t.Fatalf("create beneficiary: %v", err)
	}

	deleteSvc, err := NewBeneficiaryService(svc.db, svc.writer, svc.repo, WithClock(func() time.Time { return deleteClock }))
	if err != nil {
		t.Fatalf("create delete service: %v", err)
	}
	if err := deleteSvc.SoftDeleteBeneficiary(context.Background(), created.InternalUUID); err != nil {
		t.Fatalf("soft delete beneficiary: %v", err)
	}

	stored, err := repo.GetBeneficiary(context.Background(), created.InternalUUID)
	if err != nil {
		t.Fatalf("get deleted beneficiary: %v", err)
	}
	if stored.RecordStatus != model.RecordStatusDeleted {
		t.Fatalf("expected record status DELETED, got %s", stored.RecordStatus)
	}
	if stored.DeletedAt == nil || *stored.DeletedAt != deleteClock.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected deleted_at stamp: %+v", stored.DeletedAt)
	}
	if stored.UpdatedAt != deleteClock.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected updated_at after soft delete: %s", stored.UpdatedAt)
	}
}

func TestBuildDuplicatePrecheckPromptReturnsExactDuplicate(t *testing.T) {
	t.Parallel()

	svc, _, cleanup := newTestService(t)
	defer cleanup()

	draft := sampleDraft("Theta", "Theo")
	_, err := svc.CreateBeneficiary(context.Background(), draft, CreateOptions{})
	if err != nil {
		t.Fatalf("seed beneficiary: %v", err)
	}

	prompt, err := svc.BuildDuplicatePrecheckPrompt(context.Background(), draft, "")
	if err != nil {
		t.Fatalf("build duplicate precheck prompt: %v", err)
	}
	if !prompt.HasExactDuplicate || len(prompt.ExactDuplicates) != 1 {
		t.Fatalf("expected one exact duplicate, got %+v", prompt)
	}
	if prompt.Message != "Exact duplicate detected. Confirm before saving." {
		t.Fatalf("unexpected prompt message: %s", prompt.Message)
	}

	freshPrompt, err := svc.BuildDuplicatePrecheckPrompt(context.Background(), sampleDraft("Iota", "Ian"), "")
	if err != nil {
		t.Fatalf("build no-duplicate prompt: %v", err)
	}
	if freshPrompt.HasExactDuplicate || len(freshPrompt.Candidates) != 0 {
		t.Fatalf("expected no duplicate candidates, got %+v", freshPrompt)
	}
}

func TestBuildDuplicatePrecheckPromptUsesPSGCCodesNotRawLocationLabels(t *testing.T) {
	t.Parallel()

	svc, _, cleanup := newTestService(t)
	defer cleanup()

	canonical := sampleDraft("Kappa", "Kara")
	_, err := svc.CreateBeneficiary(context.Background(), canonical, CreateOptions{})
	if err != nil {
		t.Fatalf("seed canonical beneficiary: %v", err)
	}

	noisyLabels := canonical
	noisyLabels.RegionName = "Region One -- noisy label"
	noisyLabels.ProvinceName = "Province One (variant text)"
	noisyLabels.CityName = "City One ???"
	noisyLabels.BarangayName = "Barangay One [alias]"

	prompt, err := svc.BuildDuplicatePrecheckPrompt(context.Background(), noisyLabels, "")
	if err != nil {
		t.Fatalf("build duplicate precheck prompt with noisy location labels: %v", err)
	}

	if !prompt.HasExactDuplicate || len(prompt.ExactDuplicates) != 1 {
		t.Fatalf("expected exact duplicate with matching PSGC codes despite noisy labels, got %+v", prompt)
	}
	if prompt.Lookup.RegionCode != canonical.RegionCode ||
		prompt.Lookup.ProvinceCode != canonical.ProvinceCode ||
		prompt.Lookup.CityCode != canonical.CityCode ||
		prompt.Lookup.BarangayCode != canonical.BarangayCode {
		t.Fatalf("expected lookup to use canonical PSGC codes, got %+v", prompt.Lookup)
	}
}

func newTestService(t *testing.T, opts ...Option) (*BeneficiaryService, *repository.Repository, func()) {
	t.Helper()

	handle, err := db.OpenAndMigrate(context.Background(), t.TempDir()+"\\service-test.db")
	if err != nil {
		t.Fatalf("open and migrate test db: %v", err)
	}

	repo, err := repository.New(handle.DB, handle.Writer)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("create repository: %v", err)
	}

	svc, err := NewBeneficiaryService(handle.DB, handle.Writer, repo, opts...)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("create service: %v", err)
	}

	cleanup := func() {
		_ = handle.DB.Close()
	}

	return svc, repo, cleanup
}

func sampleDraft(lastName, firstName string) BeneficiaryDraft {
	month := int64(3)
	day := int64(25)
	year := int64(1990)
	return BeneficiaryDraft{
		LastName:      lastName,
		FirstName:     firstName,
		MiddleName:    "M",
		ExtensionName: "",
		RegionCode:    "01",
		RegionName:    "Region One",
		ProvinceCode:  "0101",
		ProvinceName:  "Province One",
		CityCode:      "010101",
		CityName:      "City One",
		BarangayCode:  "010101001",
		BarangayName:  "Barangay One",
		ContactNo:     "0917-123-4567",
		BirthMonth:    &month,
		BirthDay:      &day,
		BirthYear:     &year,
		Sex:           "F",
	}
}
