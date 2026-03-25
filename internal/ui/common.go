package ui

import (
	"fmt"
	"strings"

	"dedup/internal/psgc"
)

func psgcStatusSummary(report *psgc.Report) string {
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

func psgcChecksum(report *psgc.Report) string {
	if report == nil {
		return "unknown"
	}
	checksum := strings.TrimSpace(report.SourceChecksum)
	if checksum == "" {
		return "unknown"
	}
	return checksum
}
