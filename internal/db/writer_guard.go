package db

import (
	"context"
	"database/sql"
	"fmt"
)

// WriterGuard serializes write operations so callers can queue mutations safely.
type WriterGuard struct {
	lock chan struct{}
}

// NewWriterGuard creates a single-writer guard.
func NewWriterGuard() *WriterGuard {
	return &WriterGuard{
		lock: make(chan struct{}, 1),
	}
}

// WithWriteLock executes fn under the single-writer lock.
func (g *WriterGuard) WithWriteLock(ctx context.Context, fn func() error) error {
	if g == nil {
		return fmt.Errorf("writer guard is nil")
	}
	if fn == nil {
		return fmt.Errorf("write callback is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.lock <- struct{}{}:
	}

	defer func() {
		<-g.lock
	}()

	return fn()
}

// WithWriteTx executes fn inside a transaction under the single-writer lock.
func (g *WriterGuard) WithWriteTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if fn == nil {
		return fmt.Errorf("write transaction callback is nil")
	}

	return g.WithWriteLock(ctx, func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}

		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("commit tx: %w", err)
		}

		return nil
	})
}
