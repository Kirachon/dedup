package jobs

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dedup/internal/db"
)

func TestManagerAllowedTransitions(t *testing.T) {
	manager, database, cleanup := newTestManager(t)
	defer cleanup()

	baseTime := time.Date(2026, time.March, 25, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		initialState   State
		initialAttempt int
		initialProg    *float64
		initialMessage string
		nextState      State
		opts           TransitionOptions
		wantState      State
		wantAttempt    int
		wantProgress   *float64
		wantMessage    string
		wantErrorCode  *string
	}{
		{
			name:           "queued to running",
			initialState:   StateQueued,
			initialAttempt: 0,
			initialProg:    floatPtr(0),
			initialMessage: "queued",
			nextState:      StateRunning,
			opts:           TransitionOptions{Message: "running"},
			wantState:      StateRunning,
			wantAttempt:    1,
			wantProgress:   floatPtr(0),
			wantMessage:    "running",
		},
		{
			name:           "running to succeeded",
			initialState:   StateRunning,
			initialAttempt: 1,
			initialProg:    floatPtr(35),
			initialMessage: "working",
			nextState:      StateSucceeded,
			opts:           TransitionOptions{Message: "done"},
			wantState:      StateSucceeded,
			wantAttempt:    1,
			wantProgress:   floatPtr(100),
			wantMessage:    "done",
		},
		{
			name:           "running to failed",
			initialState:   StateRunning,
			initialAttempt: 2,
			initialProg:    floatPtr(55),
			initialMessage: "working",
			nextState:      StateFailed,
			opts:           TransitionOptions{Message: "failed", ErrorCode: stringPtr("E_IMPORT")},
			wantState:      StateFailed,
			wantAttempt:    2,
			wantProgress:   floatPtr(55),
			wantMessage:    "failed",
			wantErrorCode:  stringPtr("E_IMPORT"),
		},
		{
			name:           "running to cancel requested",
			initialState:   StateRunning,
			initialAttempt: 1,
			initialProg:    floatPtr(40),
			initialMessage: "working",
			nextState:      StateCancelRequested,
			opts:           TransitionOptions{Message: "cancel requested"},
			wantState:      StateCancelRequested,
			wantAttempt:    1,
			wantProgress:   floatPtr(40),
			wantMessage:    "cancel requested",
		},
		{
			name:           "cancel requested to cancelled",
			initialState:   StateCancelRequested,
			initialAttempt: 1,
			initialProg:    floatPtr(40),
			initialMessage: "cancel requested",
			nextState:      StateCancelled,
			opts:           TransitionOptions{Message: "cancelled"},
			wantState:      StateCancelled,
			wantAttempt:    1,
			wantProgress:   floatPtr(40),
			wantMessage:    "cancelled",
		},
		{
			name:           "running to recoverable",
			initialState:   StateRunning,
			initialAttempt: 3,
			initialProg:    floatPtr(72),
			initialMessage: "working",
			nextState:      StateRecoverable,
			opts:           TransitionOptions{Message: "recoverable"},
			wantState:      StateRecoverable,
			wantAttempt:    3,
			wantProgress:   floatPtr(72),
			wantMessage:    "recoverable",
		},
		{
			name:           "recoverable to running increments attempt",
			initialState:   StateRecoverable,
			initialAttempt: 2,
			initialProg:    floatPtr(72),
			initialMessage: "recoverable",
			nextState:      StateRunning,
			opts:           TransitionOptions{Message: "resumed"},
			wantState:      StateRunning,
			wantAttempt:    3,
			wantProgress:   floatPtr(72),
			wantMessage:    "resumed",
		},
		{
			name:           "recoverable to failed",
			initialState:   StateRecoverable,
			initialAttempt: 4,
			initialProg:    floatPtr(81),
			initialMessage: "recoverable",
			nextState:      StateFailed,
			opts:           TransitionOptions{Message: "fatal", ErrorCode: stringPtr("E_FATAL")},
			wantState:      StateFailed,
			wantAttempt:    4,
			wantProgress:   floatPtr(81),
			wantMessage:    "fatal",
			wantErrorCode:  stringPtr("E_FATAL"),
		},
		{
			name:           "queued to cancelled",
			initialState:   StateQueued,
			initialAttempt: 0,
			initialProg:    floatPtr(0),
			initialMessage: "queued",
			nextState:      StateCancelled,
			opts:           TransitionOptions{Message: "cancelled before start"},
			wantState:      StateCancelled,
			wantAttempt:    0,
			wantProgress:   floatPtr(0),
			wantMessage:    "cancelled before start",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobID := "job-" + tt.name
			seedJobState(t, database, jobID, tt.initialState, tt.initialAttempt, tt.initialProg, tt.initialMessage, nil, baseTime.Add(time.Duration(i)*time.Second))

			got, err := manager.Transition(context.Background(), jobID, tt.nextState, tt.opts)
			if err != nil {
				t.Fatalf("transition: %v", err)
			}

			assertJobState(t, got, jobID, tt.wantState, tt.wantAttempt, tt.wantProgress, tt.wantMessage, tt.wantErrorCode)

			stored, err := manager.Get(context.Background(), jobID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			assertJobState(t, stored, jobID, tt.wantState, tt.wantAttempt, tt.wantProgress, tt.wantMessage, tt.wantErrorCode)
		})
	}
}

func TestManagerRejectsInvalidTransitions(t *testing.T) {
	manager, database, cleanup := newTestManager(t)
	defer cleanup()

	baseTime := time.Date(2026, time.March, 25, 9, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		initialState State
		nextState    State
	}{
		{name: "queued to failed", initialState: StateQueued, nextState: StateFailed},
		{name: "running to queued", initialState: StateRunning, nextState: StateQueued},
		{name: "cancel requested to recoverable", initialState: StateCancelRequested, nextState: StateRecoverable},
		{name: "succeeded to running", initialState: StateSucceeded, nextState: StateRunning},
		{name: "failed to running", initialState: StateFailed, nextState: StateRunning},
		{name: "recoverable to cancelled", initialState: StateRecoverable, nextState: StateCancelled},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobID := "invalid-" + tt.name
			seedJobState(t, database, jobID, tt.initialState, 1, floatPtr(20), "seeded", nil, baseTime.Add(time.Duration(i)*time.Second))

			_, err := manager.Transition(context.Background(), jobID, tt.nextState, TransitionOptions{Message: "attempt"})
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition, got %v", err)
			}

			stored, getErr := manager.Get(context.Background(), jobID)
			if getErr != nil {
				t.Fatalf("get after invalid transition: %v", getErr)
			}
			if stored.State != tt.initialState {
				t.Fatalf("expected state %s to remain, got %s", tt.initialState, stored.State)
			}
		})
	}
}

func TestManagerRecoverStartupMarksRunningJobsRecoverable(t *testing.T) {
	t.Parallel()

	manager, database, cleanup := newTestManager(t)
	defer cleanup()

	baseTime := time.Date(2026, time.March, 25, 10, 0, 0, 0, time.UTC)
	seedJobState(t, database, "running-job", StateRunning, 3, floatPtr(65), "in progress", nil, baseTime)
	seedJobState(t, database, "cancel-requested-job", StateCancelRequested, 2, floatPtr(15), "waiting cancel", nil, baseTime.Add(time.Second))
	seedJobState(t, database, "succeeded-job", StateSucceeded, 1, floatPtr(100), "done", nil, baseTime.Add(2*time.Second))

	recovered, err := manager.RecoverStartup(context.Background())
	if err != nil {
		t.Fatalf("recover startup: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered job, got %d", recovered)
	}

	runningJob, err := manager.Get(context.Background(), "running-job")
	if err != nil {
		t.Fatalf("get running job: %v", err)
	}
	assertJobState(t, runningJob, "running-job", StateRecoverable, 3, floatPtr(65), "in progress", nil)

	cancelRequestedJob, err := manager.Get(context.Background(), "cancel-requested-job")
	if err != nil {
		t.Fatalf("get cancel requested job: %v", err)
	}
	assertJobState(t, cancelRequestedJob, "cancel-requested-job", StateCancelRequested, 2, floatPtr(15), "waiting cancel", nil)
}

func TestManagerConcurrentEnqueueIsDeterministic(t *testing.T) {
	t.Parallel()

	manager, _, cleanup := newTestManager(t)
	defer cleanup()

	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := manager.Enqueue(context.Background(), "shared-job", TransitionOptions{Message: "queued"})
			results <- err
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, ErrJobAlreadyExists) {
			t.Fatalf("unexpected enqueue error: %v", err)
		}
	}

	if successes != 1 {
		t.Fatalf("expected exactly one successful enqueue, got %d", successes)
	}

	stored, err := manager.Get(context.Background(), "shared-job")
	if err != nil {
		t.Fatalf("get shared job: %v", err)
	}
	assertJobState(t, stored, "shared-job", StateQueued, 0, floatPtr(0), "queued", nil)
}

func newTestManager(t *testing.T) (*Manager, *sql.DB, func()) {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "jobs-test.db")

	handle, err := db.OpenAndMigrate(ctx, dbPath)
	if err != nil {
		t.Fatalf("open and migrate: %v", err)
	}

	baseTime := time.Date(2026, time.March, 25, 11, 0, 0, 0, time.UTC)
	manager := newManagerWithClock(handle.DB, handle.Writer, func() time.Time {
		current := baseTime
		baseTime = baseTime.Add(time.Second)
		return current
	})

	return manager, handle.DB, func() {
		_ = handle.DB.Close()
	}
}

func seedJobState(t *testing.T, database *sql.DB, jobID string, state State, attempt int, progress *float64, message string, errorCode *string, updatedAt time.Time) {
	t.Helper()

	if database == nil {
		t.Fatalf("db is nil")
	}
	if _, err := database.ExecContext(context.Background(), `
INSERT INTO job_states (
    job_id,
    state,
    updated_at_utc,
    attempt,
    progress_percent,
    message,
    error_code
) VALUES (?, ?, ?, ?, ?, ?, ?);
`,
		jobID,
		string(state),
		updatedAt.UTC().Format(time.RFC3339Nano),
		attempt,
		floatValue(progress),
		message,
		stringValue(errorCode),
	); err != nil {
		t.Fatalf("seed job state: %v", err)
	}
}

func assertJobState(t *testing.T, got JobState, wantJobID string, wantState State, wantAttempt int, wantProgress *float64, wantMessage string, wantErrorCode *string) {
	t.Helper()

	if got.JobID != wantJobID {
		t.Fatalf("job id: want %q, got %q", wantJobID, got.JobID)
	}
	if got.State != wantState {
		t.Fatalf("state: want %s, got %s", wantState, got.State)
	}
	if got.Attempt != wantAttempt {
		t.Fatalf("attempt: want %d, got %d", wantAttempt, got.Attempt)
	}
	if got.Message != wantMessage {
		t.Fatalf("message: want %q, got %q", wantMessage, got.Message)
	}
	if !sameFloatPtr(got.ProgressPercent, wantProgress) {
		t.Fatalf("progress: want %v, got %v", wantProgress, got.ProgressPercent)
	}
	if !sameStringPtr(got.ErrorCode, wantErrorCode) {
		t.Fatalf("error code: want %v, got %v", wantErrorCode, got.ErrorCode)
	}
}

func floatPtr(v float64) *float64 {
	return &v
}

func stringPtr(v string) *string {
	return &v
}

func floatValue(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func stringValue(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func sameFloatPtr(got, want *float64) bool {
	switch {
	case got == nil && want == nil:
		return true
	case got == nil || want == nil:
		return false
	default:
		return *got == *want
	}
}

func sameStringPtr(got, want *string) bool {
	switch {
	case got == nil && want == nil:
		return true
	case got == nil || want == nil:
		return false
	default:
		return *got == *want
	}
}
