package appcore

import (
	"context"
	"database/sql"
	"fmt"

	"dedup/internal/config"
	"dedup/internal/db"
	"dedup/internal/psgc"
)

// BootstrapResult provides startup database artifacts for the app shell.
type BootstrapResult struct {
	DB         *sql.DB
	DBPath     string
	Writer     *db.WriterGuard
	PSGCReport *psgc.Report
}

// BootstrapDatabase initializes SQLite policy and applies schema migrations.
func BootstrapDatabase(ctx context.Context, cfg config.Config) (*BootstrapResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	handle, err := db.OpenAndMigrate(ctx, cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("bootstrap sqlite: %w", err)
	}

	psgcReport, err := psgc.IngestCSV(ctx, handle.DB, cfg.PSGCCSVPath)
	if err != nil {
		_ = handle.DB.Close()
		return nil, fmt.Errorf("bootstrap psgc: %w", err)
	}

	return &BootstrapResult{
		DB:         handle.DB,
		DBPath:     handle.DBPath,
		Writer:     handle.Writer,
		PSGCReport: &psgcReport,
	}, nil
}
