package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"dedup/internal/db"
)

// State names are frozen by docs/contracts/jobstate_v1.md.
type State string

const (
	StateQueued          State = "queued"
	StateRunning         State = "running"
	StateCancelRequested State = "cancel_requested"
	StateCancelled       State = "cancelled"
	StateSucceeded       State = "succeeded"
	StateFailed          State = "failed"
	StateRecoverable     State = "recoverable"
)

var (
	// ErrInvalidJobID means the caller passed an empty or whitespace-only job ID.
	ErrInvalidJobID = errors.New("job id is required")
	// ErrInvalidState means the requested state is not part of the frozen contract.
	ErrInvalidState = errors.New("invalid job state")
	// ErrInvalidProgress means progress was outside the 0..100 range or not finite.
	ErrInvalidProgress = errors.New("progress percent must be between 0 and 100")
	// ErrJobAlreadyExists means enqueue was attempted for an existing job id.
	ErrJobAlreadyExists = errors.New("job already exists")
	// ErrJobNotFound means the requested job id is not present in job_states.
	ErrJobNotFound = errors.New("job not found")
	// ErrInvalidTransition means the requested state transition is not allowed.
	ErrInvalidTransition = errors.New("invalid job transition")
)

var allowedTransitions = map[State]map[State]struct{}{
	StateQueued: {
		StateRunning:   {},
		StateCancelled: {},
	},
	StateRunning: {
		StateSucceeded:       {},
		StateFailed:          {},
		StateCancelRequested: {},
		StateRecoverable:     {},
	},
	StateCancelRequested: {
		StateCancelled: {},
	},
	StateRecoverable: {
		StateRunning: {},
		StateFailed:  {},
	},
}

// JobState mirrors the durable row stored in job_states.
type JobState struct {
	JobID           string
	State           State
	UpdatedAtUTC    time.Time
	Attempt         int
	ProgressPercent *float64
	Message         string
	ErrorCode       *string
}

// TransitionOptions carries the durable journal fields that can change on update.
type TransitionOptions struct {
	ProgressPercent *float64
	Message         string
	ErrorCode       *string
}

// Manager is a small serialized queue/runner surface for durable job state.
type Manager struct {
	db     *sql.DB
	writer *db.WriterGuard
	now    func() time.Time
}

// NewManager wires a job manager to the SQLite handle and optional writer guard.
func NewManager(database *sql.DB, writer *db.WriterGuard) *Manager {
	if writer == nil {
		writer = db.NewWriterGuard()
	}
	return &Manager{
		db:     database,
		writer: writer,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func newManagerWithClock(database *sql.DB, writer *db.WriterGuard, now func() time.Time) *Manager {
	if writer == nil {
		writer = db.NewWriterGuard()
	}
	if now == nil {
		now = func() time.Time {
			return time.Now().UTC()
		}
	}
	return &Manager{db: database, writer: writer, now: now}
}

// Enqueue creates a queued job row in job_states.
func (m *Manager) Enqueue(ctx context.Context, jobID string, opts TransitionOptions) (JobState, error) {
	if err := m.validateReady(); err != nil {
		return JobState{}, err
	}
	ctx = normalizeContext(ctx)
	jobID = normalizeJobID(jobID)
	if jobID == "" {
		return JobState{}, ErrInvalidJobID
	}

	progress, err := normalizeProgress(opts.ProgressPercent)
	if err != nil {
		return JobState{}, err
	}

	message := normalizeMessage(opts.Message)
	errorCode := normalizeErrorCode(opts.ErrorCode)
	record := JobState{
		JobID:           jobID,
		State:           StateQueued,
		UpdatedAtUTC:    m.now(),
		Attempt:         0,
		ProgressPercent: progress,
		Message:         message,
		ErrorCode:       errorCode,
	}

	err = m.withWriteTx(ctx, func(tx *sql.Tx) error {
		result, execErr := tx.ExecContext(ctx, `
INSERT INTO job_states (
    job_id,
    state,
    updated_at_utc,
    attempt,
    progress_percent,
    message,
    error_code
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO NOTHING;
`,
			record.JobID,
			string(record.State),
			record.UpdatedAtUTC.Format(time.RFC3339Nano),
			record.Attempt,
			nullFloatValue(record.ProgressPercent),
			record.Message,
			nullStringValue(record.ErrorCode),
		)
		if execErr != nil {
			return fmt.Errorf("insert job state: %w", execErr)
		}

		affected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("inspect inserted job state: %w", rowsErr)
		}
		if affected == 0 {
			return ErrJobAlreadyExists
		}

		return nil
	})
	if err != nil {
		return JobState{}, err
	}

	return record, nil
}

// Transition applies one allowed state transition and persists the new snapshot.
func (m *Manager) Transition(ctx context.Context, jobID string, next State, opts TransitionOptions) (JobState, error) {
	if err := m.validateReady(); err != nil {
		return JobState{}, err
	}
	ctx = normalizeContext(ctx)
	jobID = normalizeJobID(jobID)
	if jobID == "" {
		return JobState{}, ErrInvalidJobID
	}
	if err := validateState(next); err != nil {
		return JobState{}, err
	}
	if next == StateQueued {
		return JobState{}, fmt.Errorf("%w: use Enqueue to create queued jobs", ErrInvalidTransition)
	}

	var updated JobState
	err := m.withWriteTx(ctx, func(tx *sql.Tx) error {
		current, loadErr := loadJobStateTx(ctx, tx, jobID)
		if loadErr != nil {
			return loadErr
		}

		if !isAllowedTransition(current.State, next) {
			return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current.State, next)
		}

		updatedState, composeErr := composeTransition(current, next, opts, m.now)
		if composeErr != nil {
			return composeErr
		}
		updated = updatedState

		result, execErr := tx.ExecContext(ctx, `
UPDATE job_states
SET state = ?,
    updated_at_utc = ?,
    attempt = ?,
    progress_percent = ?,
    message = ?,
    error_code = ?
WHERE job_id = ?;
`,
			string(updated.State),
			updated.UpdatedAtUTC.Format(time.RFC3339Nano),
			updated.Attempt,
			nullFloatValue(updated.ProgressPercent),
			updated.Message,
			nullStringValue(updated.ErrorCode),
			updated.JobID,
		)
		if execErr != nil {
			return fmt.Errorf("update job state: %w", execErr)
		}

		affected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("inspect updated job state: %w", rowsErr)
		}
		if affected == 0 {
			return ErrJobNotFound
		}

		return nil
	})
	if err != nil {
		return JobState{}, err
	}

	return updated, nil
}

// Get returns the current durable snapshot for a job id.
func (m *Manager) Get(ctx context.Context, jobID string) (JobState, error) {
	if err := m.validateReady(); err != nil {
		return JobState{}, err
	}
	ctx = normalizeContext(ctx)
	jobID = normalizeJobID(jobID)
	if jobID == "" {
		return JobState{}, ErrInvalidJobID
	}

	return loadJobState(ctx, m.db, jobID)
}

// RecoverStartup scans incomplete jobs and marks running jobs recoverable.
func (m *Manager) RecoverStartup(ctx context.Context) (int, error) {
	if err := m.validateReady(); err != nil {
		return 0, err
	}
	ctx = normalizeContext(ctx)

	recovered := 0
	err := m.withWriteTx(ctx, func(tx *sql.Tx) error {
		rows, queryErr := tx.QueryContext(ctx, `
SELECT job_id, state, updated_at_utc, attempt, progress_percent, message, error_code
FROM job_states
WHERE state IN (?, ?)
ORDER BY updated_at_utc ASC, job_id ASC;
`, string(StateRunning), string(StateCancelRequested))
		if queryErr != nil {
			return fmt.Errorf("scan incomplete jobs: %w", queryErr)
		}
		defer rows.Close()

		for rows.Next() {
			current, scanErr := scanJobState(rows)
			if scanErr != nil {
				return scanErr
			}
			if current.State != StateRunning {
				continue
			}

			nextMessage := current.Message
			if nextMessage == "" {
				nextMessage = "Recovered after restart"
			}

			updated, composeErr := composeTransition(current, StateRecoverable, TransitionOptions{
				ProgressPercent: current.ProgressPercent,
				Message:         nextMessage,
			}, m.now)
			if composeErr != nil {
				return composeErr
			}

			result, execErr := tx.ExecContext(ctx, `
UPDATE job_states
SET state = ?,
    updated_at_utc = ?,
    attempt = ?,
    progress_percent = ?,
    message = ?,
    error_code = ?
WHERE job_id = ?;
`,
				string(updated.State),
				updated.UpdatedAtUTC.Format(time.RFC3339Nano),
				updated.Attempt,
				nullFloatValue(updated.ProgressPercent),
				updated.Message,
				nullStringValue(updated.ErrorCode),
				updated.JobID,
			)
			if execErr != nil {
				return fmt.Errorf("recover job %s: %w", current.JobID, execErr)
			}

			affected, rowsErr := result.RowsAffected()
			if rowsErr != nil {
				return fmt.Errorf("inspect recovered job %s: %w", current.JobID, rowsErr)
			}
			if affected == 0 {
				return ErrJobNotFound
			}
			recovered++
		}

		if err := rows.Err(); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return recovered, nil
}

// AllowedTransition reports whether from -> to is part of the frozen contract.
func AllowedTransition(from, to State) bool {
	return isAllowedTransition(from, to)
}

func (m *Manager) validateReady() error {
	if m == nil {
		return fmt.Errorf("manager is nil")
	}
	if m.db == nil {
		return fmt.Errorf("db is nil")
	}
	if m.writer == nil {
		return fmt.Errorf("writer guard is nil")
	}
	if m.now == nil {
		return fmt.Errorf("clock is nil")
	}
	return nil
}

func (m *Manager) withWriteTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return m.writer.WithWriteTx(ctx, m.db, fn)
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func normalizeJobID(jobID string) string {
	return strings.TrimSpace(jobID)
}

func normalizeMessage(message string) string {
	return strings.TrimSpace(message)
}

func normalizeErrorCode(code *string) *string {
	if code == nil {
		return nil
	}
	value := strings.TrimSpace(*code)
	if value == "" {
		return nil
	}
	return &value
}

func normalizeProgress(progress *float64) (*float64, error) {
	if progress == nil {
		zero := 0.0
		return &zero, nil
	}
	if math.IsNaN(*progress) || math.IsInf(*progress, 0) {
		return nil, ErrInvalidProgress
	}
	if *progress < 0 || *progress > 100 {
		return nil, ErrInvalidProgress
	}
	value := *progress
	return &value, nil
}

func normalizeProgressOrPreserve(progress *float64, current *float64, fallback float64) (*float64, error) {
	if progress != nil {
		return normalizeProgress(progress)
	}
	if current != nil {
		value := *current
		return &value, nil
	}
	value := fallback
	return &value, nil
}

func validateState(state State) error {
	switch state {
	case StateQueued, StateRunning, StateCancelRequested, StateCancelled, StateSucceeded, StateFailed, StateRecoverable:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrInvalidState, state)
	}
}

func isAllowedTransition(from, to State) bool {
	nextStates, ok := allowedTransitions[from]
	if !ok {
		return false
	}
	_, ok = nextStates[to]
	return ok
}

func composeTransition(current JobState, next State, opts TransitionOptions, clock func() time.Time) (JobState, error) {
	if err := validateState(current.State); err != nil {
		return JobState{}, err
	}
	if err := validateState(next); err != nil {
		return JobState{}, err
	}

	message := current.Message
	if trimmed := normalizeMessage(opts.Message); trimmed != "" {
		message = trimmed
	}

	errorCode := current.ErrorCode
	if opts.ErrorCode != nil {
		errorCode = normalizeErrorCode(opts.ErrorCode)
	} else if next != StateFailed {
		errorCode = nil
	}

	progress, err := resolveProgress(current, next, opts.ProgressPercent)
	if err != nil {
		return JobState{}, err
	}

	attempt := current.Attempt
	if next == StateRunning && (current.State == StateQueued || current.State == StateRecoverable) {
		attempt++
	}

	return JobState{
		JobID:           current.JobID,
		State:           next,
		UpdatedAtUTC:    clock().UTC(),
		Attempt:         attempt,
		ProgressPercent: progress,
		Message:         message,
		ErrorCode:       errorCode,
	}, nil
}

func resolveProgress(current JobState, next State, requested *float64) (*float64, error) {
	if requested != nil {
		return normalizeProgress(requested)
	}

	switch next {
	case StateRunning:
		if current.State == StateRecoverable {
			return normalizeProgressOrPreserve(nil, current.ProgressPercent, 0)
		}
		return normalizeProgressOrPreserve(nil, nil, 0)
	case StateSucceeded:
		return normalizeProgressOrPreserve(nil, nil, 100)
	default:
		return cloneFloatPtr(current.ProgressPercent), nil
	}
}

func loadJobState(ctx context.Context, database *sql.DB, jobID string) (JobState, error) {
	if database == nil {
		return JobState{}, fmt.Errorf("db is nil")
	}
	row := database.QueryRowContext(ctx, `
SELECT job_id, state, updated_at_utc, attempt, progress_percent, message, error_code
FROM job_states
WHERE job_id = ?;
`, jobID)
	return scanJobState(row)
}

func loadJobStateTx(ctx context.Context, tx *sql.Tx, jobID string) (JobState, error) {
	if tx == nil {
		return JobState{}, fmt.Errorf("tx is nil")
	}
	row := tx.QueryRowContext(ctx, `
SELECT job_id, state, updated_at_utc, attempt, progress_percent, message, error_code
FROM job_states
WHERE job_id = ?;
`, jobID)
	return scanJobState(row)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJobState(row rowScanner) (JobState, error) {
	var (
		jobID      string
		stateValue string
		updatedRaw string
		attempt    int
		progress   sql.NullFloat64
		message    sql.NullString
		errorCode  sql.NullString
	)

	if err := row.Scan(&jobID, &stateValue, &updatedRaw, &attempt, &progress, &message, &errorCode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return JobState{}, ErrJobNotFound
		}
		return JobState{}, err
	}

	state, err := parseState(stateValue)
	if err != nil {
		return JobState{}, err
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, updatedRaw)
	if err != nil {
		return JobState{}, fmt.Errorf("parse updated_at_utc: %w", err)
	}

	result := JobState{
		JobID:        jobID,
		State:        state,
		UpdatedAtUTC: updatedAt.UTC(),
		Attempt:      attempt,
		Message:      message.String,
	}
	if progress.Valid {
		value := progress.Float64
		result.ProgressPercent = &value
	}
	if errorCode.Valid {
		value := errorCode.String
		result.ErrorCode = &value
	}

	return result, nil
}

func parseState(raw string) (State, error) {
	state := State(strings.TrimSpace(raw))
	if err := validateState(state); err != nil {
		return State(""), err
	}
	return state, nil
}

func cloneFloatPtr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func nullFloatValue(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullStringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
