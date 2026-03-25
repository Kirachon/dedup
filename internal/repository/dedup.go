package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"dedup/internal/model"
)

// CreateDedupRun inserts a new dedup run row.
func (r *Repository) CreateDedupRun(ctx context.Context, run *model.DedupRun) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createDedupRun(background(ctx), run)
	})
}

// UpdateDedupRun updates the mutable fields of a dedup run row.
func (r *Repository) UpdateDedupRun(ctx context.Context, run *model.DedupRun) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.updateDedupRun(background(ctx), run)
	})
}

// GetDedupRun loads a run by ID.
func (r *Repository) GetDedupRun(ctx context.Context, runID string) (*model.DedupRun, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT run_id, started_at, completed_at, status, total_candidates, total_matches, notes
FROM dedup_runs
WHERE run_id = ?
LIMIT 1;
`, runID)

	return scanDedupRun(row)
}

// ListDedupRuns returns deterministic run history.
func (r *Repository) ListDedupRuns(ctx context.Context, query DedupRunListQuery) ([]model.DedupRun, error) {
	if err := normalizeOffset(query.Offset); err != nil {
		return nil, err
	}
	limit := normalizeLimit(query.Limit, defaultGenericLimit)

	builder := sqlBuilder{}
	builder.addEquals("status", query.Status)

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT run_id, started_at, completed_at, status, total_candidates, total_matches, notes
FROM dedup_runs
WHERE `+builder.where()+`
ORDER BY started_at DESC, run_id ASC
LIMIT ? OFFSET ?;
`, append(builder.args, limit, query.Offset)...)
	if err != nil {
		return nil, fmt.Errorf("list dedup runs: %w", err)
	}
	defer rows.Close()

	items := make([]model.DedupRun, 0)
	for rows.Next() {
		item, err := scanDedupRun(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dedup runs: %w", err)
	}

	return items, nil
}

// CreateDedupMatch inserts a new dedup match row.
func (r *Repository) CreateDedupMatch(ctx context.Context, match *model.DedupMatch) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createDedupMatch(background(ctx), match)
	})
}

// UpdateDedupMatchDecisionStatus updates the mutable decision status for a dedup match.
func (r *Repository) UpdateDedupMatchDecisionStatus(ctx context.Context, matchID, decisionStatus string) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.updateDedupMatchDecisionStatus(background(ctx), matchID, decisionStatus)
	})
}

// GetDedupMatch loads a dedup match by match ID.
func (r *Repository) GetDedupMatch(ctx context.Context, matchID string) (*model.DedupMatch, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT match_id, run_id, record_a_uuid, record_b_uuid, pair_key, first_name_score, middle_name_score, last_name_score, extension_name_score, total_score, birthdate_compare, barangay_compare, decision_status, created_at
FROM dedup_matches
WHERE match_id = ?
LIMIT 1;
`, matchID)

	return scanDedupMatch(row)
}

// ListDedupMatchesByRun returns matches for one run ordered by score and pair key.
func (r *Repository) ListDedupMatchesByRun(ctx context.Context, runID string) ([]model.DedupMatch, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT match_id, run_id, record_a_uuid, record_b_uuid, pair_key, first_name_score, middle_name_score, last_name_score, extension_name_score, total_score, birthdate_compare, barangay_compare, decision_status, created_at
FROM dedup_matches
WHERE run_id = ?
ORDER BY total_score DESC, pair_key ASC, match_id ASC;
`, runID)
	if err != nil {
		return nil, fmt.Errorf("list dedup matches: %w", err)
	}
	defer rows.Close()

	items := make([]model.DedupMatch, 0)
	for rows.Next() {
		item, err := scanDedupMatch(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dedup matches: %w", err)
	}

	return items, nil
}

// CreateDedupDecision inserts a new dedup decision row.
func (r *Repository) CreateDedupDecision(ctx context.Context, decision *model.DedupDecision) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.createDedupDecision(background(ctx), decision)
	})
}

// UpdateDedupDecision updates the mutable decision fields by decision ID.
func (r *Repository) UpdateDedupDecision(ctx context.Context, decision *model.DedupDecision) error {
	return r.write(background(ctx), func(txRepo *Repository) error {
		return txRepo.updateDedupDecision(background(ctx), decision)
	})
}

// GetDedupDecision loads a decision by decision ID.
func (r *Repository) GetDedupDecision(ctx context.Context, decisionID string) (*model.DedupDecision, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT decision_id, pair_key, record_a_uuid, record_b_uuid, decision, resolved_by, resolved_at, notes
FROM dedup_decisions
WHERE decision_id = ?
LIMIT 1;
`, decisionID)

	return scanDedupDecision(row)
}

// GetDedupDecisionByPairKey loads a decision by pair key.
func (r *Repository) GetDedupDecisionByPairKey(ctx context.Context, pairKey string) (*model.DedupDecision, error) {
	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	row := q.QueryRowContext(background(ctx), `
SELECT decision_id, pair_key, record_a_uuid, record_b_uuid, decision, resolved_by, resolved_at, notes
FROM dedup_decisions
WHERE pair_key = ?
LIMIT 1;
`, pairKey)

	return scanDedupDecision(row)
}

// ListDedupDecisions returns deterministic decision history.
func (r *Repository) ListDedupDecisions(ctx context.Context, query DedupDecisionListQuery) ([]model.DedupDecision, error) {
	if err := normalizeOffset(query.Offset); err != nil {
		return nil, err
	}
	limit := normalizeLimit(query.Limit, defaultGenericLimit)

	q, err := r.queryer()
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(background(ctx), `
SELECT decision_id, pair_key, record_a_uuid, record_b_uuid, decision, resolved_by, resolved_at, notes
FROM dedup_decisions
ORDER BY resolved_at DESC, pair_key ASC, decision_id ASC
LIMIT ? OFFSET ?;
`, limit, query.Offset)
	if err != nil {
		return nil, fmt.Errorf("list dedup decisions: %w", err)
	}
	defer rows.Close()

	items := make([]model.DedupDecision, 0)
	for rows.Next() {
		item, err := scanDedupDecision(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dedup decisions: %w", err)
	}

	return items, nil
}

func (r *Repository) createDedupRun(ctx context.Context, run *model.DedupRun) error {
	if run == nil {
		return fmt.Errorf("dedup run is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO dedup_runs (run_id, started_at, completed_at, status, total_candidates, total_matches, notes)
VALUES (?, ?, ?, ?, ?, ?, ?);
`, run.RunID, run.StartedAt, stringValue(run.CompletedAt), run.Status, run.TotalCandidates, run.TotalMatches, stringValue(run.Notes))
	if err != nil {
		return fmt.Errorf("insert dedup run: %w", err)
	}

	return nil
}

func (r *Repository) updateDedupRun(ctx context.Context, run *model.DedupRun) error {
	if run == nil {
		return fmt.Errorf("dedup run is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE dedup_runs SET
    completed_at = ?,
    status = ?,
    total_candidates = ?,
    total_matches = ?,
    notes = ?
WHERE run_id = ?;
`, stringValue(run.CompletedAt), run.Status, run.TotalCandidates, run.TotalMatches, stringValue(run.Notes), run.RunID)
	if err != nil {
		return fmt.Errorf("update dedup run: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update dedup run rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Repository) createDedupMatch(ctx context.Context, match *model.DedupMatch) error {
	if match == nil {
		return fmt.Errorf("dedup match is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	decisionStatus := strings.TrimSpace(match.DecisionStatus)
	if decisionStatus == "" {
		decisionStatus = "PENDING"
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO dedup_matches (
    match_id,
    run_id,
    record_a_uuid,
    record_b_uuid,
    pair_key,
    first_name_score,
    middle_name_score,
    last_name_score,
    extension_name_score,
    total_score,
    birthdate_compare,
    barangay_compare,
    decision_status,
    created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`, match.MatchID, match.RunID, match.RecordAUUID, match.RecordBUUID, match.PairKey, match.FirstNameScore, match.MiddleNameScore, match.LastNameScore, match.ExtensionNameScore, match.TotalScore, int64Value(match.BirthdateCompare), int64Value(match.BarangayCompare), decisionStatus, match.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert dedup match: %w", err)
	}

	return nil
}

func (r *Repository) updateDedupMatchDecisionStatus(ctx context.Context, matchID, decisionStatus string) error {
	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE dedup_matches SET decision_status = ?
WHERE match_id = ?;
`, decisionStatus, matchID)
	if err != nil {
		return fmt.Errorf("update dedup match decision status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update dedup match rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Repository) createDedupDecision(ctx context.Context, decision *model.DedupDecision) error {
	if decision == nil {
		return fmt.Errorf("dedup decision is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	_, err = q.ExecContext(ctx, `
INSERT INTO dedup_decisions (
    decision_id,
    pair_key,
    record_a_uuid,
    record_b_uuid,
    decision,
    resolved_by,
    resolved_at,
    notes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
`, decision.DecisionID, decision.PairKey, decision.RecordAUUID, decision.RecordBUUID, decision.Decision, decision.ResolvedBy, decision.ResolvedAt, stringValue(decision.Notes))
	if err != nil {
		return fmt.Errorf("insert dedup decision: %w", err)
	}

	return nil
}

func (r *Repository) updateDedupDecision(ctx context.Context, decision *model.DedupDecision) error {
	if decision == nil {
		return fmt.Errorf("dedup decision is nil")
	}

	q, err := r.queryer()
	if err != nil {
		return err
	}

	result, err := q.ExecContext(ctx, `
UPDATE dedup_decisions SET
    decision = ?,
    resolved_by = ?,
    resolved_at = ?,
    notes = ?
WHERE decision_id = ?;
`, decision.Decision, decision.ResolvedBy, decision.ResolvedAt, stringValue(decision.Notes), decision.DecisionID)
	if err != nil {
		return fmt.Errorf("update dedup decision: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update dedup decision rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func scanDedupRun(scanner rowScanner) (*model.DedupRun, error) {
	var (
		item        model.DedupRun
		completedAt sql.NullString
		notes       sql.NullString
	)

	if err := scanner.Scan(&item.RunID, &item.StartedAt, &completedAt, &item.Status, &item.TotalCandidates, &item.TotalMatches, &notes); err != nil {
		return nil, err
	}

	item.CompletedAt = stringPtrFromNullString(completedAt)
	item.Notes = stringPtrFromNullString(notes)
	return &item, nil
}

func scanDedupMatch(scanner rowScanner) (*model.DedupMatch, error) {
	var (
		item             model.DedupMatch
		birthdateCompare sql.NullInt64
		barangayCompare  sql.NullInt64
	)

	if err := scanner.Scan(&item.MatchID, &item.RunID, &item.RecordAUUID, &item.RecordBUUID, &item.PairKey, &item.FirstNameScore, &item.MiddleNameScore, &item.LastNameScore, &item.ExtensionNameScore, &item.TotalScore, &birthdateCompare, &barangayCompare, &item.DecisionStatus, &item.CreatedAt); err != nil {
		return nil, err
	}

	item.BirthdateCompare = int64PtrFromNullInt64(birthdateCompare)
	item.BarangayCompare = int64PtrFromNullInt64(barangayCompare)
	return &item, nil
}

func scanDedupDecision(scanner rowScanner) (*model.DedupDecision, error) {
	var (
		item  model.DedupDecision
		notes sql.NullString
	)

	if err := scanner.Scan(&item.DecisionID, &item.PairKey, &item.RecordAUUID, &item.RecordBUUID, &item.Decision, &item.ResolvedBy, &item.ResolvedAt, &notes); err != nil {
		return nil, err
	}

	item.Notes = stringPtrFromNullString(notes)
	return &item, nil
}
