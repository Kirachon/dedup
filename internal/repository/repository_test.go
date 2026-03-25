package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"dedup/internal/db"
	"dedup/internal/model"
)

func openTestRepository(t *testing.T) (*Repository, func()) {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "repository-test.db")
	handle, err := db.OpenAndMigrate(ctx, dbPath)
	if err != nil {
		t.Fatalf("open and migrate sqlite: %v", err)
	}

	repo, err := New(handle.DB, handle.Writer)
	if err != nil {
		_ = handle.DB.Close()
		t.Fatalf("new repository: %v", err)
	}

	return repo, func() {
		_ = handle.DB.Close()
	}
}

func TestRepositoryBeneficiarySettingsAuditTransactionsAndPaging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, cleanup := openTestRepository(t)
	defer cleanup()

	alpha1 := beneficiaryFixture("uuid-alpha-1", "G-002", "Alpha", "Bea", "0101", model.RecordStatusActive, model.DedupStatusClear)
	alpha2 := beneficiaryFixture("uuid-alpha-2", "G-003", "Alpha", "Bea", "0101", model.RecordStatusRetained, model.DedupStatusClear)
	zulu := beneficiaryFixture("uuid-zulu", "G-001", "Zulu", "Aaron", "0101", model.RecordStatusActive, model.DedupStatusClear)

	if err := repo.WithinTx(ctx, func(txRepo *Repository) error {
		if err := txRepo.CreateBeneficiary(ctx, &zulu); err != nil {
			return err
		}
		if err := txRepo.CreateBeneficiary(ctx, &alpha1); err != nil {
			return err
		}
		if err := txRepo.CreateBeneficiary(ctx, &alpha2); err != nil {
			return err
		}
		if err := txRepo.UpsertSetting(ctx, &model.AppSetting{
			SettingKey:   "ui.theme",
			SettingValue: "midnight",
			UpdatedAt:    "2026-03-25T09:00:00Z",
		}); err != nil {
			return err
		}
		if err := txRepo.CreateAuditLog(ctx, &model.AuditLog{
			AuditID:     "audit-1",
			EntityType:  "beneficiary",
			EntityID:    alpha1.InternalUUID,
			Action:      "create",
			PerformedBy: "tester",
			DetailsJSON: stringPtr(`{"ok":true}`),
			CreatedAt:   "2026-03-25T09:00:00Z",
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("commit seeded beneficiary transaction: %v", err)
	}

	updatedAlpha1 := alpha1
	updatedAlpha1.ContactNo = stringPtr("09998887777")
	updatedAlpha1.ContactNoNorm = stringPtr("09998887777")
	updatedAlpha1.UpdatedAt = "2026-03-25T09:15:00Z"
	if err := repo.UpdateBeneficiary(ctx, &updatedAlpha1); err != nil {
		t.Fatalf("update beneficiary: %v", err)
	}

	if err := repo.SoftDeleteBeneficiary(ctx, zulu.InternalUUID, "2026-03-25T09:30:00Z"); err != nil {
		t.Fatalf("soft delete beneficiary: %v", err)
	}

	if err := repo.WithinTx(ctx, func(txRepo *Repository) error {
		rollback := beneficiaryFixture("uuid-rollback", "G-999", "Rollback", "Test", "0101", model.RecordStatusActive, model.DedupStatusClear)
		rollback.CreatedAt = "2026-03-25T10:00:00Z"
		rollback.UpdatedAt = "2026-03-25T10:00:00Z"
		if err := txRepo.CreateBeneficiary(ctx, &rollback); err != nil {
			return err
		}
		if err := txRepo.UpsertSetting(ctx, &model.AppSetting{
			SettingKey:   "ui.rollback",
			SettingValue: "discard",
			UpdatedAt:    "2026-03-25T10:00:00Z",
		}); err != nil {
			return err
		}
		if err := txRepo.CreateAuditLog(ctx, &model.AuditLog{
			AuditID:     "audit-rollback",
			EntityType:  "beneficiary",
			EntityID:    "uuid-rollback",
			Action:      "create",
			PerformedBy: "tester",
			CreatedAt:   "2026-03-25T10:00:00Z",
		}); err != nil {
			return err
		}
		return fmt.Errorf("rollback requested")
	}); err == nil {
		t.Fatalf("expected rollback error, got nil")
	}

	if _, err := repo.GetBeneficiary(ctx, "uuid-rollback"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected rollback beneficiary to be absent, got %v", err)
	}
	if _, err := repo.GetSetting(ctx, "ui.rollback"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected rollback setting to be absent, got %v", err)
	}
	if _, err := repo.GetAuditLog(ctx, "audit-rollback"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected rollback audit log to be absent, got %v", err)
	}

	gotAlpha1, err := repo.GetBeneficiary(ctx, alpha1.InternalUUID)
	if err != nil {
		t.Fatalf("get alpha beneficiary: %v", err)
	}
	if gotAlpha1.ContactNo == nil || *gotAlpha1.ContactNo != "09998887777" || gotAlpha1.UpdatedAt != "2026-03-25T09:15:00Z" {
		t.Fatalf("expected updated beneficiary fields, got %+v", gotAlpha1)
	}

	gotZulu, err := repo.GetBeneficiaryByGeneratedID(ctx, zulu.GeneratedID)
	if err != nil {
		t.Fatalf("get zulu beneficiary by generated ID: %v", err)
	}
	if gotZulu.DeletedAt == nil || *gotZulu.DeletedAt != "2026-03-25T09:30:00Z" || gotZulu.RecordStatus != model.RecordStatusDeleted {
		t.Fatalf("expected soft-deleted beneficiary state, got %+v", gotZulu)
	}

	setting, err := repo.GetSetting(ctx, "ui.theme")
	if err != nil {
		t.Fatalf("get setting: %v", err)
	}
	if setting.SettingValue != "midnight" || setting.UpdatedAt != "2026-03-25T09:00:00Z" {
		t.Fatalf("unexpected setting: %+v", setting)
	}

	audit, err := repo.GetAuditLog(ctx, "audit-1")
	if err != nil {
		t.Fatalf("get audit log: %v", err)
	}
	if audit.EntityID != alpha1.InternalUUID || audit.DetailsJSON == nil || *audit.DetailsJSON != `{"ok":true}` {
		t.Fatalf("unexpected audit log: %+v", audit)
	}

	audits, err := repo.ListAuditLogs(ctx, AuditLogQuery{EntityType: "beneficiary", EntityID: alpha1.InternalUUID, Limit: 10})
	if err != nil {
		t.Fatalf("list audit logs: %v", err)
	}
	if len(audits) != 1 {
		t.Fatalf("expected 1 audit log, got %d", len(audits))
	}

	page, err := repo.ListBeneficiaries(ctx, BeneficiaryListQuery{Search: "Bea", Limit: 1, Offset: 1})
	if err != nil {
		t.Fatalf("list beneficiaries: %v", err)
	}
	if page.Total != 2 || len(page.Items) != 1 || page.Items[0].GeneratedID != alpha2.GeneratedID {
		t.Fatalf("unexpected paged beneficiaries: %+v", page)
	}

	orderedPage, err := repo.ListBeneficiaries(ctx, BeneficiaryListQuery{Limit: 10})
	if err != nil {
		t.Fatalf("list beneficiaries ordered: %v", err)
	}
	if orderedPage.Total != 2 || len(orderedPage.Items) != 2 {
		t.Fatalf("unexpected ordered beneficiaries: %+v", orderedPage)
	}
	if orderedPage.Items[0].GeneratedID != alpha1.GeneratedID || orderedPage.Items[1].GeneratedID != alpha2.GeneratedID {
		t.Fatalf("unexpected beneficiary order: %+v", orderedPage.Items)
	}

	orderedIncludingDeleted, err := repo.ListBeneficiaries(ctx, BeneficiaryListQuery{IncludeDeleted: true, Limit: 10})
	if err != nil {
		t.Fatalf("list beneficiaries with deleted rows: %v", err)
	}
	if orderedIncludingDeleted.Total != 3 || orderedIncludingDeleted.Items[2].GeneratedID != zulu.GeneratedID {
		t.Fatalf("unexpected inclusive beneficiary list: %+v", orderedIncludingDeleted)
	}

	duplicates, err := repo.FindDuplicateBeneficiaries(ctx, BeneficiaryDuplicateLookup{
		ExcludeInternalUUID: alpha1.InternalUUID,
		NormLastName:        strings.ToUpper(alpha1.LastName),
		NormFirstName:       strings.ToUpper(alpha1.FirstName),
		Limit:               10,
	})
	if err != nil {
		t.Fatalf("find duplicate beneficiaries: %v", err)
	}
	if len(duplicates) != 1 || duplicates[0].GeneratedID != alpha2.GeneratedID {
		t.Fatalf("unexpected duplicate candidates: %+v", duplicates)
	}
}

func TestRepositoryDedupAndLogsRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, cleanup := openTestRepository(t)
	defer cleanup()

	if err := repo.WithinTx(ctx, func(txRepo *Repository) error {
		beneficiaryA := beneficiaryFixture("uuid-a", "G-A", "Alpha", "Anne", "0101", model.RecordStatusActive, model.DedupStatusClear)
		beneficiaryB := beneficiaryFixture("uuid-b", "G-B", "Beta", "Ben", "0101", model.RecordStatusActive, model.DedupStatusClear)
		if err := txRepo.CreateBeneficiary(ctx, &beneficiaryA); err != nil {
			return err
		}
		if err := txRepo.CreateBeneficiary(ctx, &beneficiaryB); err != nil {
			return err
		}
		if err := txRepo.CreateDedupRun(ctx, &model.DedupRun{
			RunID:           "run-1",
			StartedAt:       "2026-03-25T10:00:00Z",
			Status:          "running",
			TotalCandidates: 1,
			TotalMatches:    0,
			Notes:           stringPtr("initial"),
		}); err != nil {
			return err
		}
		if err := txRepo.CreateDedupMatch(ctx, &model.DedupMatch{
			MatchID:            "match-1",
			RunID:              "run-1",
			RecordAUUID:        "uuid-a",
			RecordBUUID:        "uuid-b",
			PairKey:            "uuid-a|uuid-b",
			FirstNameScore:     99.5,
			MiddleNameScore:    88.0,
			LastNameScore:      77.0,
			ExtensionNameScore: 66.0,
			TotalScore:         95.0,
			BirthdateCompare:   int64Ptr(1),
			BarangayCompare:    int64Ptr(0),
			DecisionStatus:     "PENDING",
			CreatedAt:          "2026-03-25T10:00:00Z",
		}); err != nil {
			return err
		}
		if err := txRepo.CreateDedupDecision(ctx, &model.DedupDecision{
			DecisionID:  "decision-1",
			PairKey:     "uuid-a|uuid-b",
			RecordAUUID: "uuid-a",
			RecordBUUID: "uuid-b",
			Decision:    model.DedupDecisionRetainA,
			ResolvedBy:  "tester",
			ResolvedAt:  "2026-03-25T10:00:00Z",
			Notes:       stringPtr("initial"),
		}); err != nil {
			return err
		}
		if err := txRepo.CreateImportLog(ctx, &model.ImportLog{
			ImportID:        "import-1",
			SourceType:      model.ImportSourceCSV,
			SourceReference: "seed.csv",
			FileName:        stringPtr("seed.csv"),
			FileHash:        stringPtr("hash-1"),
			IdempotencyKey:  stringPtr("idem-1"),
			RowsRead:        1,
			RowsInserted:    1,
			RowsSkipped:     0,
			RowsFailed:      0,
			Status:          "RUNNING",
			StartedAt:       "2026-03-25T10:00:00Z",
		}); err != nil {
			return err
		}
		if err := txRepo.CreateExportLog(ctx, &model.ExportLog{
			ExportID:     "export-1",
			FileName:     "beneficiaries.csv",
			ExportType:   "BENEFICIARIES",
			RowsExported: 1,
			CreatedAt:    "2026-03-25T10:00:00Z",
			PerformedBy:  stringPtr("tester"),
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed dedup/log data: %v", err)
	}

	if err := repo.UpdateDedupRun(ctx, &model.DedupRun{
		RunID:           "run-1",
		StartedAt:       "2026-03-25T10:00:00Z",
		CompletedAt:     stringPtr("2026-03-25T10:05:00Z"),
		Status:          "succeeded",
		TotalCandidates: 1,
		TotalMatches:    1,
		Notes:           stringPtr("done"),
	}); err != nil {
		t.Fatalf("update dedup run: %v", err)
	}
	if err := repo.UpdateDedupMatchDecisionStatus(ctx, "match-1", "RESOLVED"); err != nil {
		t.Fatalf("update dedup match decision status: %v", err)
	}
	if err := repo.UpdateDedupDecision(ctx, &model.DedupDecision{
		DecisionID: "decision-1",
		Decision:   model.DedupDecisionDifferent,
		ResolvedBy: "reviewer",
		ResolvedAt: "2026-03-25T10:05:00Z",
		Notes:      stringPtr("changed"),
	}); err != nil {
		t.Fatalf("update dedup decision: %v", err)
	}
	if err := repo.UpdateImportLog(ctx, &model.ImportLog{
		ImportID:        "import-1",
		SourceType:      model.ImportSourceCSV,
		SourceReference: "seed.csv",
		FileName:        stringPtr("seed.csv"),
		FileHash:        stringPtr("hash-1"),
		IdempotencyKey:  stringPtr("idem-1"),
		RowsRead:        2,
		RowsInserted:    1,
		RowsSkipped:     1,
		RowsFailed:      0,
		Status:          "SUCCEEDED",
		StartedAt:       "2026-03-25T10:00:00Z",
		CompletedAt:     stringPtr("2026-03-25T10:05:00Z"),
		CheckpointToken: stringPtr("checkpoint-1"),
		OperatorName:    stringPtr("tester"),
		Remarks:         stringPtr("done"),
	}); err != nil {
		t.Fatalf("update import log: %v", err)
	}
	if err := repo.UpdateExportLog(ctx, &model.ExportLog{
		ExportID:     "export-1",
		FileName:     "beneficiaries-final.csv",
		ExportType:   "BENEFICIARIES",
		RowsExported: 2,
		CreatedAt:    "2026-03-25T10:00:00Z",
		PerformedBy:  stringPtr("reviewer"),
	}); err != nil {
		t.Fatalf("update export log: %v", err)
	}

	run, err := repo.GetDedupRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("get dedup run: %v", err)
	}
	if run.Status != "succeeded" || run.TotalMatches != 1 || run.CompletedAt == nil || *run.CompletedAt != "2026-03-25T10:05:00Z" {
		t.Fatalf("unexpected dedup run state: %+v", run)
	}

	match, err := repo.GetDedupMatch(ctx, "match-1")
	if err != nil {
		t.Fatalf("get dedup match: %v", err)
	}
	if match.DecisionStatus != "RESOLVED" || match.BirthdateCompare == nil || match.BarangayCompare == nil || *match.BirthdateCompare != 1 || *match.BarangayCompare != 0 {
		t.Fatalf("unexpected dedup match state: %+v", match)
	}

	decision, err := repo.GetDedupDecision(ctx, "decision-1")
	if err != nil {
		t.Fatalf("get dedup decision: %v", err)
	}
	if decision.Decision != model.DedupDecisionDifferent || decision.ResolvedBy != "reviewer" {
		t.Fatalf("unexpected dedup decision state: %+v", decision)
	}

	decisionByPair, err := repo.GetDedupDecisionByPairKey(ctx, "uuid-a|uuid-b")
	if err != nil {
		t.Fatalf("get dedup decision by pair key: %v", err)
	}
	if decisionByPair.DecisionID != "decision-1" {
		t.Fatalf("unexpected decision by pair key: %+v", decisionByPair)
	}

	importLog, err := repo.GetImportLog(ctx, "import-1")
	if err != nil {
		t.Fatalf("get import log: %v", err)
	}
	if importLog.Status != "SUCCEEDED" || importLog.RowsRead != 2 || importLog.CompletedAt == nil || *importLog.CompletedAt != "2026-03-25T10:05:00Z" {
		t.Fatalf("unexpected import log state: %+v", importLog)
	}

	exportLog, err := repo.GetExportLog(ctx, "export-1")
	if err != nil {
		t.Fatalf("get export log: %v", err)
	}
	if exportLog.FileName != "beneficiaries-final.csv" || exportLog.RowsExported != 2 || exportLog.PerformedBy == nil || *exportLog.PerformedBy != "reviewer" {
		t.Fatalf("unexpected export log state: %+v", exportLog)
	}

	runs, err := repo.ListDedupRuns(ctx, DedupRunListQuery{Status: "succeeded", Limit: 10})
	if err != nil {
		t.Fatalf("list dedup runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "run-1" {
		t.Fatalf("unexpected dedup runs: %+v", runs)
	}

	matches, err := repo.ListDedupMatchesByRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("list dedup matches: %v", err)
	}
	if len(matches) != 1 || matches[0].MatchID != "match-1" {
		t.Fatalf("unexpected dedup matches: %+v", matches)
	}

	decisions, err := repo.ListDedupDecisions(ctx, DedupDecisionListQuery{Limit: 10})
	if err != nil {
		t.Fatalf("list dedup decisions: %v", err)
	}
	if len(decisions) != 1 || decisions[0].DecisionID != "decision-1" {
		t.Fatalf("unexpected dedup decisions: %+v", decisions)
	}

	importLogs, err := repo.ListImportLogs(ctx, ImportLogListQuery{Status: "SUCCEEDED", Limit: 10})
	if err != nil {
		t.Fatalf("list import logs: %v", err)
	}
	if len(importLogs) != 1 || importLogs[0].ImportID != "import-1" {
		t.Fatalf("unexpected import logs: %+v", importLogs)
	}

	exportLogs, err := repo.ListExportLogs(ctx, ExportLogListQuery{ExportType: "BENEFICIARIES", Limit: 10})
	if err != nil {
		t.Fatalf("list export logs: %v", err)
	}
	if len(exportLogs) != 1 || exportLogs[0].ExportID != "export-1" {
		t.Fatalf("unexpected export logs: %+v", exportLogs)
	}
}

func TestRepositoryAuditAndLogMetadataAreNormalized(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, cleanup := openTestRepository(t)
	defer cleanup()

	if err := repo.WithinTx(ctx, func(txRepo *Repository) error {
		if err := txRepo.CreateAuditLog(ctx, &model.AuditLog{
			AuditID:     "audit-sensitive",
			EntityType:  "backup",
			EntityID:    "entity-1",
			Action:      "create",
			PerformedBy: "tester",
			DetailsJSON: stringPtr(`{"snapshot_path":"C:\\Users\\preda\\snapshots\\secret.db","manifest_path":"/var/tmp/secret.manifest.json","expected_confirmation":"RESTORE secret.db","notes":"very secret note","nested":{"db_path":"D:\\data\\live.db"}}`),
			CreatedAt:   "2026-03-25T11:00:00Z",
		}); err != nil {
			return err
		}
		if err := txRepo.CreateImportLog(ctx, &model.ImportLog{
			ImportID:        "import-sensitive",
			SourceType:      model.ImportSourceCSV,
			SourceReference: `C:\ingest\seed.csv`,
			FileName:        stringPtr(`C:\ingest\incoming\seed.csv`),
			FileHash:        stringPtr(" hash-1 "),
			IdempotencyKey:  stringPtr(" idem-1 "),
			RowsRead:        1,
			RowsInserted:    1,
			RowsSkipped:     0,
			RowsFailed:      0,
			Status:          "SUCCEEDED",
			StartedAt:       "2026-03-25T11:00:00Z",
			OperatorName:    stringPtr(" operator "),
			Remarks:         stringPtr(" needs review "),
			CheckpointToken: stringPtr(" checkpoint-1 "),
		}); err != nil {
			return err
		}
		if err := txRepo.CreateExportLog(ctx, &model.ExportLog{
			ExportID:     "export-sensitive",
			FileName:     `C:\exports\beneficiaries.csv`,
			ExportType:   "BENEFICIARIES",
			RowsExported: 1,
			CreatedAt:    "2026-03-25T11:00:00Z",
			PerformedBy:  stringPtr(" reviewer "),
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed normalized metadata transaction: %v", err)
	}

	audit, err := repo.GetAuditLog(ctx, "audit-sensitive")
	if err != nil {
		t.Fatalf("get sensitive audit log: %v", err)
	}
	if audit.DetailsJSON == nil {
		t.Fatalf("expected sanitized audit details")
	}
	var details map[string]any
	if err := json.Unmarshal([]byte(*audit.DetailsJSON), &details); err != nil {
		t.Fatalf("unmarshal sanitized audit details: %v", err)
	}

	assertAuditDetailsString(t, details, "snapshot_path", portableAuditPathBase(`C:\Users\preda\snapshots\secret.db`))
	assertAuditDetailsString(t, details, "manifest_path", portableAuditPathBase(`/var/tmp/secret.manifest.json`))
	assertAuditDetailsString(t, details, "expected_confirmation", auditDetailRedactedValue)
	assertAuditDetailsString(t, details, "notes", auditDetailRedactedValue)

	nested, ok := details["nested"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested audit details map, got %T", details["nested"])
	}
	assertAuditNestedString(t, nested, "db_path", portableAuditPathBase(`D:\data\live.db`))

	importLog, err := repo.GetImportLog(ctx, "import-sensitive")
	if err != nil {
		t.Fatalf("get sensitive import log: %v", err)
	}
	if importLog.SourceReference != "seed.csv" {
		t.Fatalf("unexpected normalized source reference: %s", importLog.SourceReference)
	}
	if importLog.FileName == nil || *importLog.FileName != "seed.csv" {
		t.Fatalf("unexpected normalized import file name: %+v", importLog.FileName)
	}
	if importLog.FileHash == nil || strings.TrimSpace(*importLog.FileHash) != "hash-1" {
		t.Fatalf("unexpected import file hash: %+v", importLog.FileHash)
	}
	if importLog.IdempotencyKey == nil || strings.TrimSpace(*importLog.IdempotencyKey) != "idem-1" {
		t.Fatalf("unexpected import idempotency key: %+v", importLog.IdempotencyKey)
	}
	if importLog.OperatorName == nil || *importLog.OperatorName != "operator" {
		t.Fatalf("unexpected import operator name: %+v", importLog.OperatorName)
	}
	if importLog.Remarks == nil || *importLog.Remarks != "needs review" {
		t.Fatalf("unexpected import remarks: %+v", importLog.Remarks)
	}
	if importLog.CheckpointToken == nil || *importLog.CheckpointToken != "checkpoint-1" {
		t.Fatalf("unexpected import checkpoint token: %+v", importLog.CheckpointToken)
	}

	exportLog, err := repo.GetExportLog(ctx, "export-sensitive")
	if err != nil {
		t.Fatalf("get sensitive export log: %v", err)
	}
	if exportLog.FileName != "beneficiaries.csv" {
		t.Fatalf("unexpected normalized export file name: %s", exportLog.FileName)
	}
	if exportLog.PerformedBy == nil || *exportLog.PerformedBy != "reviewer" {
		t.Fatalf("unexpected normalized export performed_by: %+v", exportLog.PerformedBy)
	}
}

func TestRepositoryPSGCLookups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, cleanup := openTestRepository(t)
	defer cleanup()

	if err := repo.WithinTx(ctx, func(txRepo *Repository) error {
		q, err := txRepo.queryer()
		if err != nil {
			return err
		}

		if _, err := q.ExecContext(ctx, `INSERT INTO psgc_regions (region_code, region_name) VALUES (?, ?);`, "01", "Alpha Region"); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO psgc_regions (region_code, region_name) VALUES (?, ?);`, "02", "Zulu Region"); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO psgc_provinces (province_code, region_code, province_name) VALUES (?, ?, ?);`, "0101", "01", "Alpha Province"); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO psgc_cities (city_code, region_code, province_code, city_name, city_type) VALUES (?, ?, ?, ?, ?);`, "010101", "01", "0101", "Zulu City", "Component"); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO psgc_cities (city_code, region_code, province_code, city_name, city_type) VALUES (?, ?, ?, ?, ?);`, "010102", "01", nil, "Alpha City", nil); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO psgc_barangays (barangay_code, region_code, province_code, city_code, barangay_name) VALUES (?, ?, ?, ?, ?);`, "010101001", "01", "0101", "010101", "Beta Barangay"); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO psgc_barangays (barangay_code, region_code, province_code, city_code, barangay_name) VALUES (?, ?, ?, ?, ?);`, "010101002", "01", nil, "010101", "Alpha Barangay"); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `
INSERT INTO psgc_ingest_metadata (
    id,
    source_file_name,
    source_checksum,
    rows_read,
    rows_regions,
    rows_provinces,
    rows_cities,
    rows_barangays,
    ingested_at
) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?);
`, "psgc.csv", "checksum", 5, 2, 1, 2, 2, "2026-03-25T11:00:00Z"); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("seed PSGC data: %v", err)
	}

	regions, err := repo.ListRegions(ctx)
	if err != nil {
		t.Fatalf("list regions: %v", err)
	}
	if len(regions) != 2 || regions[0].RegionName != "Alpha Region" || regions[1].RegionName != "Zulu Region" {
		t.Fatalf("unexpected region ordering: %+v", regions)
	}

	region, err := repo.GetRegion(ctx, "01")
	if err != nil {
		t.Fatalf("get region: %v", err)
	}
	if region.RegionName != "Alpha Region" {
		t.Fatalf("unexpected region: %+v", region)
	}

	provinces, err := repo.ListProvincesByRegion(ctx, "01")
	if err != nil {
		t.Fatalf("list provinces by region: %v", err)
	}
	if len(provinces) != 1 || provinces[0].ProvinceName != "Alpha Province" {
		t.Fatalf("unexpected provinces: %+v", provinces)
	}

	province, err := repo.GetProvince(ctx, "0101")
	if err != nil {
		t.Fatalf("get province: %v", err)
	}
	if province.ProvinceName != "Alpha Province" {
		t.Fatalf("unexpected province: %+v", province)
	}

	citiesByRegion, err := repo.ListCitiesByRegion(ctx, "01")
	if err != nil {
		t.Fatalf("list cities by region: %v", err)
	}
	if len(citiesByRegion) != 2 || citiesByRegion[0].CityName != "Alpha City" || citiesByRegion[1].CityName != "Zulu City" {
		t.Fatalf("unexpected city ordering by region: %+v", citiesByRegion)
	}

	citiesByProvince, err := repo.ListCitiesByProvince(ctx, "0101")
	if err != nil {
		t.Fatalf("list cities by province: %v", err)
	}
	if len(citiesByProvince) != 1 || citiesByProvince[0].CityName != "Zulu City" {
		t.Fatalf("unexpected city ordering by province: %+v", citiesByProvince)
	}

	city, err := repo.GetCity(ctx, "010102")
	if err != nil {
		t.Fatalf("get city: %v", err)
	}
	if city.CityType != nil || city.ProvinceCode != nil {
		t.Fatalf("expected nullable city fields to be nil, got %+v", city)
	}

	barangays, err := repo.ListBarangaysByCity(ctx, "010101")
	if err != nil {
		t.Fatalf("list barangays by city: %v", err)
	}
	if len(barangays) != 2 || barangays[0].BarangayName != "Alpha Barangay" || barangays[1].BarangayName != "Beta Barangay" {
		t.Fatalf("unexpected barangay ordering: %+v", barangays)
	}

	barangay, err := repo.GetBarangay(ctx, "010101002")
	if err != nil {
		t.Fatalf("get barangay: %v", err)
	}
	if barangay.ProvinceCode != nil {
		t.Fatalf("expected nil barangay province code, got %+v", barangay)
	}

	metadata, err := repo.GetIngestMetadata(ctx)
	if err != nil {
		t.Fatalf("get PSGC ingest metadata: %v", err)
	}
	if metadata.RowsRegions != 2 || metadata.SourceFileName != "psgc.csv" {
		t.Fatalf("unexpected ingest metadata: %+v", metadata)
	}
}

func beneficiaryFixture(internalUUID, generatedID, lastName, firstName, barangayCode string, status model.RecordStatus, dedupStatus model.DedupStatus) model.Beneficiary {
	return model.Beneficiary{
		InternalUUID:      internalUUID,
		GeneratedID:       generatedID,
		LastName:          lastName,
		FirstName:         firstName,
		MiddleName:        stringPtr("M"),
		ExtensionName:     nil,
		NormLastName:      strings.ToUpper(lastName),
		NormFirstName:     strings.ToUpper(firstName),
		NormMiddleName:    stringPtr("M"),
		NormExtensionName: nil,
		RegionCode:        "01",
		RegionName:        "Region One",
		ProvinceCode:      "0101",
		ProvinceName:      "Province One",
		CityCode:          "010101",
		CityName:          "City One",
		BarangayCode:      barangayCode,
		BarangayName:      "Barangay One",
		ContactNo:         stringPtr("09171234567"),
		ContactNoNorm:     stringPtr("09171234567"),
		BirthMonth:        int64Ptr(3),
		BirthDay:          int64Ptr(25),
		BirthYear:         int64Ptr(1990),
		BirthdateISO:      stringPtr("1990-03-25"),
		Sex:               "F",
		RecordStatus:      status,
		DedupStatus:       dedupStatus,
		SourceType:        model.BeneficiarySourceLocal,
		SourceReference:   nil,
		CreatedAt:         "2026-03-25T08:00:00Z",
		UpdatedAt:         "2026-03-25T08:00:00Z",
	}
}

func stringPtr(v string) *string {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func assertAuditDetailsString(t *testing.T, details map[string]any, key, want string) {
	t.Helper()

	raw, ok := details[key]
	if !ok {
		t.Fatalf("expected audit detail key %q", key)
	}
	got, ok := raw.(string)
	if !ok {
		t.Fatalf("expected audit detail key %q to be string, got %T", key, raw)
	}
	if got != want {
		t.Fatalf("unexpected audit detail %q: got=%q want=%q", key, got, want)
	}
}

func assertAuditNestedString(t *testing.T, details map[string]any, key, want string) {
	t.Helper()

	raw, ok := details[key]
	if !ok {
		t.Fatalf("expected nested audit detail key %q", key)
	}
	got, ok := raw.(string)
	if !ok {
		t.Fatalf("expected nested audit detail key %q to be string, got %T", key, raw)
	}
	if got != want {
		t.Fatalf("unexpected nested audit detail %q: got=%q want=%q", key, got, want)
	}
}
