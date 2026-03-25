package ui

import (
	"fmt"
	"strings"

	"dedup/internal/psgc"
)

func psgcStatusSummary(report *psgc.Report) string {
	if report == nil {
		return "PSGC: not attempted"
	}
	if report.RowsRead == 0 {
		return "PSGC: no rows processed"
	}
	if report.Skipped {
		return "PSGC: up to date"
	}
	return fmt.Sprintf("PSGC: %d rows, %d barangays", report.RowsRead, report.BarangaysInserted)
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
