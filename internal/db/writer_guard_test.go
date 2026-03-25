package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
)

func TestWriterGuardWithWriteTxSerializesMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "writer-guard-test.db")

	database, err := OpenSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer database.Close()

	if _, err := database.ExecContext(ctx, `
CREATE TABLE write_guard_test (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    value INTEGER NOT NULL
);
`); err != nil {
		t.Fatalf("create test table: %v", err)
	}

	guard := NewWriterGuard()

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		value := i
		go func() {
			defer wg.Done()
			err := guard.WithWriteTx(ctx, database, func(tx *sql.Tx) error {
				_, execErr := tx.ExecContext(ctx, "INSERT INTO write_guard_test (value) VALUES (?);", value)
				return execErr
			})
			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("writer guard transaction failed: %v", err)
		}
	}

	var rowCount int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM write_guard_test;").Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != workers {
		t.Fatalf("expected %d rows, got %d", workers, rowCount)
	}
}
