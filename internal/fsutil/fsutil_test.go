package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomicReplacesAndCleansUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "target")
	if err := WriteFileAtomic(path, []byte("v1")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := WriteFileAtomic(path, []byte("v2")); err != nil {
		t.Fatalf("second write: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "v2" {
		t.Fatalf("read back %q, err=%v; want v2", data, err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file left behind")
	}
}

func TestWriteFileAtomicCreateFailure(t *testing.T) {
	// The temp file lands next to the target, so a nonexistent parent
	// directory fails the create step.
	path := filepath.Join(t.TempDir(), "no-such-dir", "target")
	if err := WriteFileAtomic(path, []byte("x")); err == nil {
		t.Fatal("expected create error for missing parent directory")
	}
}

func TestWriteFileAtomicRenameFailure(t *testing.T) {
	// Renaming a file over an existing non-empty directory fails; the temp
	// file must be cleaned up.
	dir := t.TempDir()
	path := filepath.Join(dir, "occupied")
	if err := os.MkdirAll(filepath.Join(path, "child"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := WriteFileAtomic(path, []byte("x")); err == nil {
		t.Fatal("expected rename error over a non-empty directory")
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file left behind after failed rename")
	}
}

func TestSyncDirMissing(t *testing.T) {
	if err := SyncDir(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("expected error for missing directory")
	}
}
