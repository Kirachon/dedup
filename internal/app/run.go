package appcore

import (
	"context"
	"fmt"

	"dedup/internal/config"
	"dedup/internal/psgc"

	fyne "fyne.io/fyne/v2"
	fyneapp "fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// Run launches a minimal UI shell and verifies DB bootstrap.
func Run(ctx context.Context, cfg config.Config) error {
	bootstrap, err := BootstrapDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	defer bootstrap.DB.Close()

	app := fyneapp.NewWithID(cfg.AppID)
	window := app.NewWindow(cfg.WindowTitle)
	window.Resize(fyne.NewSize(920, 580))

	window.SetContent(container.NewVBox(
		widget.NewLabel("Offline Beneficiary Tool"),
		widget.NewLabel("Wave 1 scaffold is active."),
		widget.NewLabel(fmt.Sprintf("SQLite bootstrap successful: %s", bootstrap.DBPath)),
		widget.NewLabel(psgcStatusLabel(bootstrap.PSGCReport)),
		widget.NewLabel("Frozen interfaces are documented in docs/contracts/*.md."),
	))

	window.ShowAndRun()
	return nil
}

func psgcStatusLabel(report *psgc.Report) string {
	if report == nil {
		return "PSGC ingest: not attempted"
	}
	if report.RowsRead == 0 {
		return "PSGC ingest: no rows processed"
	}
	if report.Skipped {
		return fmt.Sprintf("PSGC ingest: skipped, checksum already current (%s)", report.SourceChecksum)
	}
	return fmt.Sprintf(
		"PSGC ingest: %d rows read, %d barangays loaded, checksum %s",
		report.RowsRead,
		report.BarangaysInserted,
		report.SourceChecksum,
	)
}
