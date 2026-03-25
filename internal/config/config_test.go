package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolvePSGCCSVPathPrefersWorkingDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	execDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(execDir, 0o755); err != nil {
		t.Fatalf("mkdir exec dir: %v", err)
	}
	workingDir := filepath.Join(root, "work")
	if err := os.MkdirAll(workingDir, 0o755); err != nil {
		t.Fatalf("mkdir working dir: %v", err)
	}
	csvName := "lib_geo_map_2025_202603251312.csv"
	workingCSV := filepath.Join(workingDir, csvName)
	if err := os.WriteFile(workingCSV, []byte("working"), 0o644); err != nil {
		t.Fatalf("write working csv: %v", err)
	}
	execCSV := filepath.Join(execDir, csvName)
	if err := os.WriteFile(execCSV, []byte("exec"), 0o644); err != nil {
		t.Fatalf("write exec csv: %v", err)
	}

	resolved := resolvePSGCCSVPath(csvName, workingDir, execDir)
	if resolved != workingCSV {
		t.Fatalf("expected working dir csv %q, got %q", workingCSV, resolved)
	}
}

func TestResolvePSGCCSVPathFallsBackToExecutableDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	execDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(execDir, 0o755); err != nil {
		t.Fatalf("mkdir exec dir: %v", err)
	}
	workingDir := filepath.Join(root, "work")
	if err := os.MkdirAll(workingDir, 0o755); err != nil {
		t.Fatalf("mkdir working dir: %v", err)
	}
	csvName := "lib_geo_map_2025_202603251312.csv"
	execCSV := filepath.Join(execDir, csvName)
	if err := os.WriteFile(execCSV, []byte("exec"), 0o644); err != nil {
		t.Fatalf("write exec csv: %v", err)
	}

	resolved := resolvePSGCCSVPath(csvName, workingDir, execDir)
	if resolved != execCSV {
		t.Fatalf("expected executable dir csv %q, got %q", execCSV, resolved)
	}
}

func TestResolvePSGCCSVPathKeepsAbsolutePaths(t *testing.T) {
	t.Parallel()

	absolute := filepath.Join(t.TempDir(), "custom.csv")
	resolved := resolvePSGCCSVPath(absolute, "ignored", "ignored")
	if resolved != absolute {
		t.Fatalf("expected absolute path %q, got %q", absolute, resolved)
	}
}
