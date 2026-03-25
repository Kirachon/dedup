package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"dedup/internal/db"
	"dedup/internal/model"
	"dedup/internal/repository"
)

func TestDedupDecisionServiceApplyDeleteAndResetRestoresBeneficiary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	applyAt := time.Date(2026, time.March, 25, 15, 0, 0, 0, time.UTC)

	svc, repo, cleanup := newDedupDecisionTestService(t, func() time.Time { return applyAt })
	defer cleanup()

	match, benA, benB := seedDedupMatchFixture(t, ctx, repo)

	applyResult, err := svc.ApplyDecision(ctx, ApplyDedupDecisionRequest{
		MatchID:    match.MatchID,
		Decision:   model.DedupDecisionDeleteASoft,
		ResolvedBy: "reviewer-a",
		Notes:      "duplicate confirmed",
	})
	if err != nil {
		t.Fatalf("apply delete decision: %v", err)
	}
	if applyResult.Recomputed {
		t.Fatalf("expected first decision apply to not be recompute")
	}
	if applyResult.SoftDeletedInternalUUID == nil || *applyResult.SoftDeletedInternalUUID != benA.InternalUUID {
		t.Fatalf("expected beneficiary A to be soft-deleted, got %+v", applyResult.SoftDeletedInternalUUID)
	}

	storedMatch, err := repo.GetDedupMatch(ctx, match.MatchID)
	if err != nil {
		t.Fatalf("get dedup match after apply: %v", err)
	}
	if storedMatch.DecisionStatus != matchDecisionStatusResolved {
		t.Fatalf("expected resolved match status, got %s", storedMatch.DecisionStatus)
	}

	storedDecision, err := repo.GetDedupDecisionByPairKey(ctx, match.PairKey)
	if err != nil {
		t.Fatalf("get dedup decision by pair key: %v", err)
	}
	if storedDecision.Decision != model.DedupDecisionDeleteASoft {
		t.Fatalf("expected delete A decision, got %s", storedDecision.Decision)
	}

	storedA, err := repo.GetBeneficiary(ctx, benA.InternalUUID)
	if err != nil {
		t.Fatalf("get beneficiary A: %v", err)
	}
	if storedA.RecordStatus != model.RecordStatusDeleted {
		t.Fatalf("expected beneficiary A to be DELETED, got %s", storedA.RecordStatus)
	}
	if storedA.DeletedAt == nil || *storedA.DeletedAt != applyAt.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected deleted_at for beneficiary A: %+v", storedA.DeletedAt)
	}
	if storedA.DedupStatus != model.DedupStatusResolved {
		t.Fatalf("expected beneficiary A dedup status RESOLVED, got %s", storedA.DedupStatus)
	}

	storedB, err := repo.GetBeneficiary(ctx, benB.InternalUUID)
	if err != nil {
		t.Fatalf("get beneficiary B: %v", err)
	}
	if storedB.RecordStatus == model.RecordStatusDeleted {
		t.Fatalf("expected beneficiary B to remain non-deleted")
	}
	if storedB.DedupStatus != model.DedupStatusResolved {
		t.Fatalf("expected beneficiary B dedup status RESOLVED, got %s", storedB.DedupStatus)
	}

	appliedAudits, err := repo.ListAuditLogs(ctx, repository.AuditLogQuery{Action: auditActionDecisionApplied, Limit: 10})
	if err != nil {
		t.Fatalf("list applied decision audits: %v", err)
	}
	if len(appliedAudits) != 1 {
		t.Fatalf("expected exactly one apply audit row, got %d", len(appliedAudits))
	}
	appliedDetails := mustParseAuditDetails(t, appliedAudits[0])
	if got, _ := appliedDetails["decision"].(string); got != string(model.DedupDecisionDeleteASoft) {
		t.Fatalf("unexpected decision in apply audit details: %+v", appliedDetails)
	}
	if got, _ := appliedDetails["soft_deleted_internal_uuid"].(string); got != benA.InternalUUID {
		t.Fatalf("unexpected soft-deleted uuid in apply audit details: %+v", appliedDetails)
	}

	resetAt := time.Date(2026, time.March, 25, 15, 30, 0, 0, time.UTC)
	resetSvc, err := NewDedupDecisionService(repo, WithDedupDecisionClock(func() time.Time { return resetAt }))
	if err != nil {
		t.Fatalf("new reset decision service: %v", err)
	}

	resetResult, err := resetSvc.ResetDecision(ctx, ResetDedupDecisionRequest{
		MatchID: match.MatchID,
		ResetBy: "reviewer-a",
		Notes:   "re-open for recompute",
	})
	if err != nil {
		t.Fatalf("reset decision: %v", err)
	}
	if resetResult.RestoredInternalUUID == nil || *resetResult.RestoredInternalUUID != benA.InternalUUID {
		t.Fatalf("expected reset to restore beneficiary A, got %+v", resetResult.RestoredInternalUUID)
	}
	if resetResult.Decision.DecisionID != storedDecision.DecisionID {
		t.Fatalf("expected reset to keep decision lineage, got decision %s vs %s", resetResult.Decision.DecisionID, storedDecision.DecisionID)
	}

	resetMatch, err := repo.GetDedupMatch(ctx, match.MatchID)
	if err != nil {
		t.Fatalf("get dedup match after reset: %v", err)
	}
	if resetMatch.DecisionStatus != matchDecisionStatusPending {
		t.Fatalf("expected pending match status after reset, got %s", resetMatch.DecisionStatus)
	}

	restoredA, err := repo.GetBeneficiary(ctx, benA.InternalUUID)
	if err != nil {
		t.Fatalf("get beneficiary A after reset: %v", err)
	}
	if restoredA.RecordStatus != model.RecordStatusActive {
		t.Fatalf("expected beneficiary A to be ACTIVE after reset, got %s", restoredA.RecordStatus)
	}
	if restoredA.DeletedAt != nil {
		t.Fatalf("expected beneficiary A deleted_at to be cleared, got %+v", restoredA.DeletedAt)
	}
	if restoredA.DedupStatus != model.DedupStatusPossibleDuplicate {
		t.Fatalf("expected beneficiary A dedup status POSSIBLE_DUPLICATE after reset, got %s", restoredA.DedupStatus)
	}

	resetB, err := repo.GetBeneficiary(ctx, benB.InternalUUID)
	if err != nil {
		t.Fatalf("get beneficiary B after reset: %v", err)
	}
	if resetB.DedupStatus != model.DedupStatusPossibleDuplicate {
		t.Fatalf("expected beneficiary B dedup status POSSIBLE_DUPLICATE after reset, got %s", resetB.DedupStatus)
	}

	resetAudits, err := repo.ListAuditLogs(ctx, repository.AuditLogQuery{Action: auditActionDecisionReset, Limit: 10})
	if err != nil {
		t.Fatalf("list reset decision audits: %v", err)
	}
	if len(resetAudits) != 1 {
		t.Fatalf("expected exactly one reset audit row, got %d", len(resetAudits))
	}
	resetDetails := mustParseAuditDetails(t, resetAudits[0])
	if got, _ := resetDetails["restored_internal_uuid"].(string); got != benA.InternalUUID {
		t.Fatalf("unexpected restored uuid in reset audit details: %+v", resetDetails)
	}
}

func TestDedupDecisionServiceRecomputeUpdatesDecisionInPlace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	firstAt := time.Date(2026, time.March, 25, 16, 0, 0, 0, time.UTC)

	svc, repo, cleanup := newDedupDecisionTestService(t, func() time.Time { return firstAt })
	defer cleanup()

	match, _, _ := seedDedupMatchFixture(t, ctx, repo)

	firstResult, err := svc.ApplyDecision(ctx, ApplyDedupDecisionRequest{
		MatchID:    match.MatchID,
		Decision:   model.DedupDecisionRetainBoth,
		ResolvedBy: "reviewer-1",
		Notes:      "keep both",
	})
	if err != nil {
		t.Fatalf("apply first decision: %v", err)
	}
	if firstResult.Recomputed {
		t.Fatalf("expected first apply to be non-recomputed")
	}

	secondAt := time.Date(2026, time.March, 25, 16, 10, 0, 0, time.UTC)
	recomputeSvc, err := NewDedupDecisionService(repo, WithDedupDecisionClock(func() time.Time { return secondAt }))
	if err != nil {
		t.Fatalf("new recompute service: %v", err)
	}

	secondResult, err := recomputeSvc.ApplyDecision(ctx, ApplyDedupDecisionRequest{
		MatchID:    match.MatchID,
		Decision:   model.DedupDecisionDifferent,
		ResolvedBy: "reviewer-2",
		Notes:      "different persons",
	})
	if err != nil {
		t.Fatalf("apply recompute decision: %v", err)
	}
	if !secondResult.Recomputed {
		t.Fatalf("expected second apply to be marked recomputed")
	}
	if secondResult.Decision.DecisionID != firstResult.Decision.DecisionID {
		t.Fatalf("expected recompute to keep decision id, got %s vs %s", secondResult.Decision.DecisionID, firstResult.Decision.DecisionID)
	}
	if secondResult.Decision.Decision != model.DedupDecisionDifferent {
		t.Fatalf("expected recomputed decision DIFFERENT_PERSONS, got %s", secondResult.Decision.Decision)
	}

	storedDecision, err := repo.GetDedupDecisionByPairKey(ctx, match.PairKey)
	if err != nil {
		t.Fatalf("get stored recomputed decision: %v", err)
	}
	if storedDecision.DecisionID != firstResult.Decision.DecisionID {
		t.Fatalf("expected in-place decision update, got %s vs %s", storedDecision.DecisionID, firstResult.Decision.DecisionID)
	}
	if storedDecision.Decision != model.DedupDecisionDifferent || storedDecision.ResolvedBy != "reviewer-2" {
		t.Fatalf("unexpected stored recomputed decision: %+v", storedDecision)
	}

	storedMatch, err := repo.GetDedupMatch(ctx, match.MatchID)
	if err != nil {
		t.Fatalf("get dedup match after recompute: %v", err)
	}
	if storedMatch.DecisionStatus != matchDecisionStatusResolved {
		t.Fatalf("expected resolved match status after recompute, got %s", storedMatch.DecisionStatus)
	}

	appliedAudits, err := repo.ListAuditLogs(ctx, repository.AuditLogQuery{Action: auditActionDecisionApplied, Limit: 10})
	if err != nil {
		t.Fatalf("list applied audits: %v", err)
	}
	if len(appliedAudits) != 1 {
		t.Fatalf("expected exactly one apply audit, got %d", len(appliedAudits))
	}

	recomputedAudits, err := repo.ListAuditLogs(ctx, repository.AuditLogQuery{Action: auditActionDecisionRecompute, Limit: 10})
	if err != nil {
		t.Fatalf("list recomputed audits: %v", err)
	}
	if len(recomputedAudits) != 1 {
		t.Fatalf("expected exactly one recompute audit, got %d", len(recomputedAudits))
	}
	recomputedDetails := mustParseAuditDetails(t, recomputedAudits[0])
	if got, _ := recomputedDetails["previous_decision"].(string); got != string(model.DedupDecisionRetainBoth) {
		t.Fatalf("expected previous decision marker in recompute audit, got %+v", recomputedDetails)
	}
	if got, _ := recomputedDetails["decision"].(string); got != string(model.DedupDecisionDifferent) {
		t.Fatalf("expected new decision marker in recompute audit, got %+v", recomputedDetails)
	}
}

func newDedupDecisionTestService(
	t *testing.T,
	clock func() time.Time,
) (*DedupDecisionService, *repository.Repository, func()) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "dedup-decision-service-test.db")
	handle, err := db.OpenAndMigrate(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open and migrate test db: %v", err)
	}

	repo, err := repository.New(handle.DB, handle.Writer)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new repository: %v", err)
	}

	svc, err := NewDedupDecisionService(repo, WithDedupDecisionClock(clock))
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new dedup decision service: %v", err)
	}

	cleanup := func() {
		_ = handle.DB.Close()
	}

	return svc, repo, cleanup
}

func seedDedupMatchFixture(
	t *testing.T,
	ctx context.Context,
	repo *repository.Repository,
) (*model.DedupMatch, *model.Beneficiary, *model.Beneficiary) {
	t.Helper()

	beneficiaryA := dedupDecisionBeneficiaryFixture("uuid-a", "G-000001", "Alpha", "Anne")
	beneficiaryB := dedupDecisionBeneficiaryFixture("uuid-b", "G-000002", "Beta", "Ben")
	if err := repo.CreateBeneficiary(ctx, beneficiaryA); err != nil {
		t.Fatalf("seed beneficiary A: %v", err)
	}
	if err := repo.CreateBeneficiary(ctx, beneficiaryB); err != nil {
		t.Fatalf("seed beneficiary B: %v", err)
	}

	run := &model.DedupRun{
		RunID:           "run-1",
		StartedAt:       "2026-03-25T12:00:00Z",
		Status:          "running",
		TotalCandidates: 1,
		TotalMatches:    0,
	}
	if err := repo.CreateDedupRun(ctx, run); err != nil {
		t.Fatalf("seed dedup run: %v", err)
	}

	match := &model.DedupMatch{
		MatchID:            "match-1",
		RunID:              run.RunID,
		RecordAUUID:        beneficiaryA.InternalUUID,
		RecordBUUID:        beneficiaryB.InternalUUID,
		PairKey:            beneficiaryA.InternalUUID + "|" + beneficiaryB.InternalUUID,
		FirstNameScore:     95,
		MiddleNameScore:    90,
		LastNameScore:      93,
		ExtensionNameScore: 100,
		TotalScore:         94,
		DecisionStatus:     matchDecisionStatusPending,
		CreatedAt:          "2026-03-25T12:00:00Z",
	}
	if err := repo.CreateDedupMatch(ctx, match); err != nil {
		t.Fatalf("seed dedup match: %v", err)
	}

	return match, beneficiaryA, beneficiaryB
}

func dedupDecisionBeneficiaryFixture(internalUUID, generatedID, lastName, firstName string) *model.Beneficiary {
	createdAt := "2026-03-25T12:00:00Z"
	return &model.Beneficiary{
		InternalUUID:  internalUUID,
		GeneratedID:   generatedID,
		LastName:      lastName,
		FirstName:     firstName,
		NormLastName:  lastName,
		NormFirstName: firstName,
		RegionCode:    "01",
		RegionName:    "Region One",
		ProvinceCode:  "0101",
		ProvinceName:  "Province One",
		CityCode:      "010101",
		CityName:      "City One",
		BarangayCode:  "010101001",
		BarangayName:  "Barangay One",
		Sex:           "F",
		RecordStatus:  model.RecordStatusActive,
		DedupStatus:   model.DedupStatusClear,
		SourceType:    model.BeneficiarySourceLocal,
		CreatedAt:     createdAt,
		UpdatedAt:     createdAt,
	}
}

func mustParseAuditDetails(t *testing.T, audit model.AuditLog) map[string]any {
	t.Helper()

	if audit.DetailsJSON == nil {
		t.Fatalf("audit details json is nil for audit %s", audit.AuditID)
	}

	var details map[string]any
	if err := json.Unmarshal([]byte(*audit.DetailsJSON), &details); err != nil {
		t.Fatalf("unmarshal audit details: %v", err)
	}
	return details
}
