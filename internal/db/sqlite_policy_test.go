package db

import (
	"context"
	"path/filepath"
	"testing"
)

func TestOpenSQLiteAppliesRuntimePolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "policy-test.db")

	database, err := OpenSQLite(ctx, dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer database.Close()

	policy, err := ReadRuntimePolicy(ctx, database)
	if err != nil {
		t.Fatalf("read runtime policy: %v", err)
	}

	if policy.JournalMode != "wal" {
		t.Fatalf("expected journal_mode wal, got %q", policy.JournalMode)
	}
	if !policy.ForeignKeysOn {
		t.Fatalf("expected foreign_keys ON")
	}
	if policy.BusyTimeoutMS != DefaultBusyTimeoutMS {
		t.Fatalf("expected busy_timeout %dms, got %dms", DefaultBusyTimeoutMS, policy.BusyTimeoutMS)
	}
}
