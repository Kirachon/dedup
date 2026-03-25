package db

import (
	"context"
	"database/sql"
	"fmt"
)

// Handle holds initialized database resources for app wiring.
type Handle struct {
	DB     *sql.DB
	DBPath string
	Writer *WriterGuard
}

// OpenAndMigrate opens SQLite, applies runtime policy, and runs migrations.
func OpenAndMigrate(ctx context.Context, dbPath string) (*Handle, error) {
	db, err := OpenSQLite(ctx, dbPath)
	if err != nil {
		return nil, err
	}

	if err := MigrateUp(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate up: %w", err)
	}

	return &Handle{
		DB:     db,
		DBPath: dbPath,
		Writer: NewWriterGuard(),
	}, nil
}
