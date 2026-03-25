package appcore

import (
	"context"
	"fmt"

	"dedup/internal/config"
	"dedup/internal/dedup"
	"dedup/internal/exporter"
	"dedup/internal/importer"
	"dedup/internal/jobs"
	appui "dedup/internal/ui"

	"dedup/internal/repository"
	"dedup/internal/service"
)

// Run launches the desktop UI shell after verifying DB bootstrap.
func Run(ctx context.Context, cfg config.Config) error {
	bootstrap, err := BootstrapDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	defer bootstrap.DB.Close()

	repo, err := repository.New(bootstrap.DB, bootstrap.Writer)
	if err != nil {
		return fmt.Errorf("build repository: %w", err)
	}

	beneficiaryService, err := service.NewBeneficiaryService(bootstrap.DB, bootstrap.Writer, repo)
	if err != nil {
		return fmt.Errorf("build beneficiary service: %w", err)
	}
	importerSvc, err := importer.New(repo, beneficiaryService)
	if err != nil {
		return fmt.Errorf("build importer: %w", err)
	}
	exporterSvc, err := exporter.New(repo)
	if err != nil {
		return fmt.Errorf("build exporter: %w", err)
	}
	dedupEngine := dedup.NewEngine()
	dedupDecisionSvc, err := service.NewDedupDecisionService(repo)
	if err != nil {
		return fmt.Errorf("build dedup decision service: %w", err)
	}
	backupSvc, err := service.NewBackupService(bootstrap.DB, bootstrap.DBPath, bootstrap.Writer)
	if err != nil {
		return fmt.Errorf("build backup service: %w", err)
	}
	jobManager := jobs.NewManager(bootstrap.DB, bootstrap.Writer)

	deps, err := appui.NewDependencies(
		cfg,
		bootstrap.DBPath,
		bootstrap.PSGCReport,
		repo,
		beneficiaryService,
		importerSvc,
		exporterSvc,
		dedupEngine,
		dedupDecisionSvc,
		backupSvc,
		jobManager,
	)
	if err != nil {
		return fmt.Errorf("build ui dependencies: %w", err)
	}

	return appui.Launch(ctx, deps)
}
