package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

const (
	// DefaultBusyTimeoutMS avoids immediate SQLITE_BUSY failures during contention.
	DefaultBusyTimeoutMS = 5000
)

// RuntimePolicy captures the pragma values we enforce at startup.
type RuntimePolicy struct {
	JournalMode   string
	ForeignKeysOn bool
	BusyTimeoutMS int
}

// OpenSQLite opens a SQLite handle and applies runtime policy pragmas.
func OpenSQLite(ctx context.Context, dbPath string) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if dbPath == "" {
		return nil, fmt.Errorf("db path is required")
	}

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Keep one underlying connection for deterministic pragma behavior.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	if err := EnforceRuntimePolicy(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// EnforceRuntimePolicy applies required SQLite pragmas for safe local operation.
func EnforceRuntimePolicy(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL;").Scan(&journalMode); err != nil {
		return fmt.Errorf("set journal_mode WAL: %w", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		return fmt.Errorf("journal_mode is %q, expected wal", journalMode)
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d;", DefaultBusyTimeoutMS)); err != nil {
		return fmt.Errorf("set busy_timeout: %w", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON;"); err != nil {
		return fmt.Errorf("set foreign_keys ON: %w", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA synchronous = NORMAL;"); err != nil {
		return fmt.Errorf("set synchronous NORMAL: %w", err)
	}

	return nil
}

// ReadRuntimePolicy reads the active pragma values for validation/testing.
func ReadRuntimePolicy(ctx context.Context, db *sql.DB) (RuntimePolicy, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return RuntimePolicy{}, fmt.Errorf("db is nil")
	}

	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		return RuntimePolicy{}, fmt.Errorf("read journal_mode: %w", err)
	}

	var foreignKeys int
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys;").Scan(&foreignKeys); err != nil {
		return RuntimePolicy{}, fmt.Errorf("read foreign_keys: %w", err)
	}

	var busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		return RuntimePolicy{}, fmt.Errorf("read busy_timeout: %w", err)
	}

	return RuntimePolicy{
		JournalMode:   strings.ToLower(journalMode),
		ForeignKeysOn: foreignKeys == 1,
		BusyTimeoutMS: busyTimeout,
	}, nil
}
