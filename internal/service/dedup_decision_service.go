package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"dedup/internal/model"
	"dedup/internal/repository"

	"github.com/google/uuid"
)

const (
	matchDecisionStatusPending   = "PENDING"
	matchDecisionStatusResolved  = "RESOLVED"
	auditActionDecisionApplied   = "DEDUP_DECISION_APPLIED"
	auditActionDecisionReset     = "DEDUP_DECISION_RESET"
	auditActionDecisionRecompute = "DEDUP_DECISION_RECOMPUTED"
	redactedAuditNote            = "[redacted]"
)

// DedupDecisionOption configures DedupDecisionService behavior.
type DedupDecisionOption func(*DedupDecisionService)

// ApplyDedupDecisionRequest holds the decision payload for one scored pair.
type ApplyDedupDecisionRequest struct {
	MatchID    string
	Decision   model.DedupDecisionType
	ResolvedBy string
	Notes      string
}

// ApplyDedupDecisionResult returns the decision + match state after apply/recompute.
type ApplyDedupDecisionResult struct {
	Decision                model.DedupDecision
	Match                   model.DedupMatch
	Recomputed              bool
	SoftDeletedInternalUUID *string
}

// ResetDedupDecisionRequest holds the reset payload for one scored pair.
type ResetDedupDecisionRequest struct {
	MatchID string
	ResetBy string
	Notes   string
}

// ResetDedupDecisionResult returns the decision + match state after reset.
type ResetDedupDecisionResult struct {
	Decision             model.DedupDecision
	Match                model.DedupMatch
	RestoredInternalUUID *string
}

// DedupDecisionService applies/resets dedup decisions with audit lineage.
type DedupDecisionService struct {
	repo *repository.Repository
	now  func() time.Time
}

// WithDedupDecisionClock overrides the clock for deterministic tests.
func WithDedupDecisionClock(clock func() time.Time) DedupDecisionOption {
	return func(s *DedupDecisionService) {
		if clock != nil {
			s.now = clock
		}
	}
}

// NewDedupDecisionService creates a service bound to the repository transaction surface.
func NewDedupDecisionService(repo *repository.Repository, opts ...DedupDecisionOption) (*DedupDecisionService, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is nil")
	}

	svc := &DedupDecisionService{
		repo: repo,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}

	return svc, nil
}

// ApplyDecision persists a decision and updates match/beneficiary state atomically.
func (s *DedupDecisionService) ApplyDecision(ctx context.Context, req ApplyDedupDecisionRequest) (*ApplyDedupDecisionResult, error) {
	if s == nil {
		return nil, fmt.Errorf("dedup decision service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	request, err := normalizeApplyRequest(req)
	if err != nil {
		return nil, err
	}

	resolvedAt := s.now().UTC().Format(time.RFC3339Nano)
	result := &ApplyDedupDecisionResult{}

	err = s.repo.WithinTx(ctx, func(txRepo *repository.Repository) error {
		match, err := txRepo.GetDedupMatch(ctx, request.MatchID)
		if err != nil {
			return fmt.Errorf("load dedup match: %w", err)
		}

		action := auditActionDecisionApplied
		decision, recomputed, previousDecision, err := applyOrRecomputeDecision(ctx, txRepo, match, request, resolvedAt)
		if err != nil {
			return err
		}
		if recomputed {
			action = auditActionDecisionRecompute
		}
		result.Decision = *decision
		result.Recomputed = recomputed

		if err := txRepo.UpdateDedupMatchDecisionStatus(ctx, match.MatchID, matchDecisionStatusResolved); err != nil {
			return fmt.Errorf("update dedup match decision status: %w", err)
		}

		softDeletedUUID, err := applyBeneficiaryState(ctx, txRepo, match, request.Decision, resolvedAt)
		if err != nil {
			return err
		}
		result.SoftDeletedInternalUUID = softDeletedUUID

		updatedMatch, err := txRepo.GetDedupMatch(ctx, match.MatchID)
		if err != nil {
			return fmt.Errorf("reload dedup match: %w", err)
		}
		result.Match = *updatedMatch

		details := map[string]any{
			"match_id":    match.MatchID,
			"pair_key":    match.PairKey,
			"decision_id": decision.DecisionID,
			"decision":    decision.Decision,
		}
		if previousDecision != nil {
			details["previous_decision"] = *previousDecision
		}
		if softDeletedUUID != nil {
			details["soft_deleted_internal_uuid"] = *softDeletedUUID
		}
		if strings.TrimSpace(request.Notes) != "" {
			details["notes"] = request.Notes
		}

		if err := createAuditLogTx(ctx, txRepo, action, match.MatchID, request.ResolvedBy, details, resolvedAt); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ResetDecision returns a match to pending and restores soft-deleted rows when needed.
func (s *DedupDecisionService) ResetDecision(ctx context.Context, req ResetDedupDecisionRequest) (*ResetDedupDecisionResult, error) {
	if s == nil {
		return nil, fmt.Errorf("dedup decision service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	request, err := normalizeResetRequest(req)
	if err != nil {
		return nil, err
	}

	resolvedAt := s.now().UTC().Format(time.RFC3339Nano)
	result := &ResetDedupDecisionResult{}

	err = s.repo.WithinTx(ctx, func(txRepo *repository.Repository) error {
		match, err := txRepo.GetDedupMatch(ctx, request.MatchID)
		if err != nil {
			return fmt.Errorf("load dedup match: %w", err)
		}

		decision, err := txRepo.GetDedupDecisionByPairKey(ctx, match.PairKey)
		if err != nil {
			return fmt.Errorf("load dedup decision: %w", err)
		}
		result.Decision = *decision

		restoredUUID, err := maybeRestoreSoftDeletedBeneficiary(ctx, txRepo, match, decision.Decision, resolvedAt)
		if err != nil {
			return err
		}
		result.RestoredInternalUUID = restoredUUID

		if err := updateBeneficiaryDedupStatus(ctx, txRepo, match.RecordAUUID, model.DedupStatusPossibleDuplicate, resolvedAt); err != nil {
			return err
		}
		if err := updateBeneficiaryDedupStatus(ctx, txRepo, match.RecordBUUID, model.DedupStatusPossibleDuplicate, resolvedAt); err != nil {
			return err
		}

		if err := txRepo.UpdateDedupMatchDecisionStatus(ctx, match.MatchID, matchDecisionStatusPending); err != nil {
			return fmt.Errorf("reset dedup match decision status: %w", err)
		}

		updatedMatch, err := txRepo.GetDedupMatch(ctx, match.MatchID)
		if err != nil {
			return fmt.Errorf("reload dedup match: %w", err)
		}
		result.Match = *updatedMatch

		details := map[string]any{
			"match_id":    match.MatchID,
			"pair_key":    match.PairKey,
			"decision_id": decision.DecisionID,
			"decision":    decision.Decision,
		}
		if restoredUUID != nil {
			details["restored_internal_uuid"] = *restoredUUID
		}
		if strings.TrimSpace(request.Notes) != "" {
			details["notes"] = request.Notes
		}

		if err := createAuditLogTx(ctx, txRepo, auditActionDecisionReset, match.MatchID, request.ResetBy, details, resolvedAt); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func normalizeApplyRequest(req ApplyDedupDecisionRequest) (ApplyDedupDecisionRequest, error) {
	req.MatchID = strings.TrimSpace(req.MatchID)
	if req.MatchID == "" {
		return ApplyDedupDecisionRequest{}, fmt.Errorf("match id is required")
	}

	if err := validateDedupDecision(req.Decision); err != nil {
		return ApplyDedupDecisionRequest{}, err
	}

	req.ResolvedBy = strings.TrimSpace(req.ResolvedBy)
	if req.ResolvedBy == "" {
		return ApplyDedupDecisionRequest{}, fmt.Errorf("resolved_by is required")
	}

	notes := strings.TrimSpace(req.Notes)
	if notes == "" {
		req.Notes = ""
	} else {
		req.Notes = notes
	}

	return req, nil
}

func normalizeResetRequest(req ResetDedupDecisionRequest) (ResetDedupDecisionRequest, error) {
	req.MatchID = strings.TrimSpace(req.MatchID)
	if req.MatchID == "" {
		return ResetDedupDecisionRequest{}, fmt.Errorf("match id is required")
	}

	req.ResetBy = strings.TrimSpace(req.ResetBy)
	if req.ResetBy == "" {
		return ResetDedupDecisionRequest{}, fmt.Errorf("reset_by is required")
	}

	notes := strings.TrimSpace(req.Notes)
	if notes == "" {
		req.Notes = ""
	} else {
		req.Notes = notes
	}

	return req, nil
}

func validateDedupDecision(decision model.DedupDecisionType) error {
	switch decision {
	case model.DedupDecisionRetainA,
		model.DedupDecisionRetainB,
		model.DedupDecisionRetainBoth,
		model.DedupDecisionDeleteASoft,
		model.DedupDecisionDeleteBSoft,
		model.DedupDecisionDifferent:
		return nil
	default:
		return fmt.Errorf("invalid dedup decision: %s", decision)
	}
}

func applyOrRecomputeDecision(
	ctx context.Context,
	txRepo *repository.Repository,
	match *model.DedupMatch,
	req ApplyDedupDecisionRequest,
	resolvedAt string,
) (*model.DedupDecision, bool, *model.DedupDecisionType, error) {
	existing, err := txRepo.GetDedupDecisionByPairKey(ctx, match.PairKey)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil, fmt.Errorf("load existing dedup decision: %w", err)
	}

	notesPtr := optionalString(req.Notes)
	if errors.Is(err, sql.ErrNoRows) {
		created := &model.DedupDecision{
			DecisionID:  uuid.NewString(),
			PairKey:     match.PairKey,
			RecordAUUID: match.RecordAUUID,
			RecordBUUID: match.RecordBUUID,
			Decision:    req.Decision,
			ResolvedBy:  req.ResolvedBy,
			ResolvedAt:  resolvedAt,
			Notes:       notesPtr,
		}
		if err := txRepo.CreateDedupDecision(ctx, created); err != nil {
			return nil, false, nil, fmt.Errorf("create dedup decision: %w", err)
		}
		return created, false, nil, nil
	}

	previous := existing.Decision
	existing.Decision = req.Decision
	existing.ResolvedBy = req.ResolvedBy
	existing.ResolvedAt = resolvedAt
	existing.Notes = notesPtr
	if err := txRepo.UpdateDedupDecision(ctx, existing); err != nil {
		return nil, false, nil, fmt.Errorf("update dedup decision: %w", err)
	}

	return existing, true, &previous, nil
}

func applyBeneficiaryState(
	ctx context.Context,
	txRepo *repository.Repository,
	match *model.DedupMatch,
	decision model.DedupDecisionType,
	resolvedAt string,
) (*string, error) {
	var softDeleted *string
	switch decision {
	case model.DedupDecisionDeleteASoft:
		if err := txRepo.SoftDeleteBeneficiary(ctx, match.RecordAUUID, resolvedAt); err != nil {
			return nil, fmt.Errorf("soft delete beneficiary A: %w", err)
		}
		softDeleted = &match.RecordAUUID
	case model.DedupDecisionDeleteBSoft:
		if err := txRepo.SoftDeleteBeneficiary(ctx, match.RecordBUUID, resolvedAt); err != nil {
			return nil, fmt.Errorf("soft delete beneficiary B: %w", err)
		}
		softDeleted = &match.RecordBUUID
	}

	if err := updateBeneficiaryDedupStatus(ctx, txRepo, match.RecordAUUID, model.DedupStatusResolved, resolvedAt); err != nil {
		return nil, err
	}
	if err := updateBeneficiaryDedupStatus(ctx, txRepo, match.RecordBUUID, model.DedupStatusResolved, resolvedAt); err != nil {
		return nil, err
	}

	return softDeleted, nil
}

func maybeRestoreSoftDeletedBeneficiary(
	ctx context.Context,
	txRepo *repository.Repository,
	match *model.DedupMatch,
	decision model.DedupDecisionType,
	resolvedAt string,
) (*string, error) {
	var targetUUID string
	switch decision {
	case model.DedupDecisionDeleteASoft:
		targetUUID = match.RecordAUUID
	case model.DedupDecisionDeleteBSoft:
		targetUUID = match.RecordBUUID
	default:
		return nil, nil
	}

	beneficiary, err := txRepo.GetBeneficiary(ctx, targetUUID)
	if err != nil {
		return nil, fmt.Errorf("load beneficiary for restore: %w", err)
	}
	if beneficiary.RecordStatus != model.RecordStatusDeleted {
		return nil, nil
	}

	beneficiary.RecordStatus = model.RecordStatusActive
	beneficiary.DeletedAt = nil
	beneficiary.UpdatedAt = resolvedAt
	if err := txRepo.UpdateBeneficiary(ctx, beneficiary); err != nil {
		return nil, fmt.Errorf("restore soft-deleted beneficiary: %w", err)
	}

	return &targetUUID, nil
}

func updateBeneficiaryDedupStatus(
	ctx context.Context,
	txRepo *repository.Repository,
	internalUUID string,
	status model.DedupStatus,
	updatedAt string,
) error {
	beneficiary, err := txRepo.GetBeneficiary(ctx, internalUUID)
	if err != nil {
		return fmt.Errorf("load beneficiary %s: %w", internalUUID, err)
	}

	beneficiary.DedupStatus = status
	beneficiary.UpdatedAt = updatedAt
	if err := txRepo.UpdateBeneficiary(ctx, beneficiary); err != nil {
		return fmt.Errorf("update beneficiary %s dedup status: %w", internalUUID, err)
	}

	return nil
}

func createAuditLogTx(
	ctx context.Context,
	txRepo *repository.Repository,
	action string,
	entityID string,
	performedBy string,
	details map[string]any,
	createdAt string,
) error {
	safeDetails := sanitizeDedupAuditDetails(details)
	detailsJSON, err := json.Marshal(safeDetails)
	if err != nil {
		return fmt.Errorf("marshal audit details: %w", err)
	}
	detailsText := string(detailsJSON)

	audit := &model.AuditLog{
		AuditID:     uuid.NewString(),
		EntityType:  "dedup_match",
		EntityID:    entityID,
		Action:      action,
		PerformedBy: performedBy,
		DetailsJSON: &detailsText,
		CreatedAt:   createdAt,
	}
	if err := txRepo.CreateAuditLog(ctx, audit); err != nil {
		return fmt.Errorf("create audit log: %w", err)
	}

	return nil
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func sanitizeDedupAuditDetails(details map[string]any) map[string]any {
	if len(details) == 0 {
		return details
	}

	safeDetails := make(map[string]any, len(details))
	for key, value := range details {
		safeDetails[key] = value
	}

	rawNotes, hasNotes := safeDetails["notes"]
	if !hasNotes {
		return safeDetails
	}

	trimmed := strings.TrimSpace(fmt.Sprint(rawNotes))
	if trimmed == "" {
		delete(safeDetails, "notes")
		return safeDetails
	}

	safeDetails["notes"] = redactedAuditNote
	return safeDetails
}
