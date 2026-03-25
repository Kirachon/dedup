package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

var migrationFilePattern = regexp.MustCompile(`^(\d+)_([a-zA-Z0-9_]+)\.(up|down)\.sql$`)

type migration struct {
	Version int
	Name    string
	UpSQL   string
	DownSQL string
}

// MigrateUp applies all pending migrations in version order.
func MigrateUp(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	if err := ensureMigrationJournal(ctx, db); err != nil {
		return err
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	for _, m := range migrations {
		applied, err := isVersionApplied(ctx, db, m.Version)
		if err != nil {
			return fmt.Errorf("check migration %d: %w", m.Version, err)
		}
		if applied {
			continue
		}

		if err := applyUpMigration(ctx, db, m); err != nil {
			return fmt.Errorf("apply migration %d_%s: %w", m.Version, m.Name, err)
		}
	}

	return nil
}

// MigrateDown rolls migrations back to targetVersion (inclusive).
func MigrateDown(ctx context.Context, db *sql.DB, targetVersion int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if targetVersion < 0 {
		return fmt.Errorf("target version must be >= 0")
	}

	if err := ensureMigrationJournal(ctx, db); err != nil {
		return err
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	byVersion := make(map[int]migration, len(migrations))
	for _, m := range migrations {
		byVersion[m.Version] = m
	}

	rows, err := db.QueryContext(ctx, `
SELECT version
FROM schema_migrations
WHERE version > ?
ORDER BY version DESC;
`, targetVersion)
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("scan applied migration version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate applied migrations: %w", err)
	}

	for _, version := range versions {
		m, ok := byVersion[version]
		if !ok {
			return fmt.Errorf("no down migration registered for version %d", version)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin down migration tx %d: %w", version, err)
		}

		if _, err := tx.ExecContext(ctx, m.DownSQL); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("execute down migration %d_%s: %w", m.Version, m.Name, err)
		}

		if _, err := tx.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version = ?;", version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("delete migration journal row %d: %w", version, err)
		}

		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("commit down migration %d: %w", version, err)
		}
	}

	return nil
}

// CurrentVersion returns the highest applied migration version, or 0 when empty.
func CurrentVersion(ctx context.Context, db *sql.DB) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}

	if err := ensureMigrationJournal(ctx, db); err != nil {
		return 0, err
	}

	var maxVersion sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations;").Scan(&maxVersion); err != nil {
		return 0, fmt.Errorf("query current migration version: %w", err)
	}
	if !maxVersion.Valid {
		return 0, nil
	}

	return int(maxVersion.Int64), nil
}

func applyUpMigration(ctx context.Context, db *sql.DB, m migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if _, err := tx.ExecContext(ctx, m.UpSQL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("execute up sql: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO schema_migrations (version, name, applied_at_utc)
VALUES (?, ?, ?);
`, m.Version, m.Name, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert migration journal row: %w", err)
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func ensureMigrationJournal(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at_utc TEXT NOT NULL
);
`)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	return nil
}

func isVersionApplied(ctx context.Context, db *sql.DB, version int) (bool, error) {
	var found int
	err := db.QueryRowContext(ctx, `
SELECT 1
FROM schema_migrations
WHERE version = ?;
`, version).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations directory: %w", err)
	}

	loaded := map[int]*migration{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		base := filepath.Base(entry.Name())
		match := migrationFilePattern.FindStringSubmatch(base)
		if len(match) != 4 {
			continue
		}

		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("parse migration version from %q: %w", base, err)
		}

		name := match[2]
		direction := match[3]
		content, err := migrationFS.ReadFile(filepath.ToSlash(filepath.Join("migrations", base)))
		if err != nil {
			return nil, fmt.Errorf("read migration file %q: %w", base, err)
		}

		current, ok := loaded[version]
		if !ok {
			current = &migration{
				Version: version,
				Name:    name,
			}
			loaded[version] = current
		}

		if current.Name != name {
			return nil, fmt.Errorf("inconsistent migration names for version %d", version)
		}

		switch direction {
		case "up":
			current.UpSQL = strings.TrimSpace(string(content))
		case "down":
			current.DownSQL = strings.TrimSpace(string(content))
		}
	}

	var versions []int
	for version := range loaded {
		versions = append(versions, version)
	}
	sort.Ints(versions)

	migrations := make([]migration, 0, len(versions))
	for _, version := range versions {
		m := loaded[version]
		if m.UpSQL == "" {
			return nil, fmt.Errorf("migration %d is missing up sql", version)
		}
		if m.DownSQL == "" {
			return nil, fmt.Errorf("migration %d is missing down sql", version)
		}
		migrations = append(migrations, *m)
	}

	return migrations, nil
}
