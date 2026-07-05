package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// openRestored opens a fresh store on a backup directory, mimicking restore
// through the normal recovery path (OpenTables reads any dir with a manifest).
func openRestored(t *testing.T, backupDir string) *Store {
	t.Helper()
	r := New()
	if _, err := r.OpenTables(backupDir); err != nil {
		t.Fatalf("OpenTables(%s): %v", backupDir, err)
	}
	t.Cleanup(func() { _ = r.CloseTables() })
	return r
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	keys := []string{"user:1", "user:2", "user:3", "acct:1"}
	for i, k := range keys {
		if err := s.Put(k, "v-"+k, vclock("w", uint64(i+1))); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	dest := filepath.Join(t.TempDir(), "backup")
	info, err := s.Backup(dest)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if info.Tables != 1 {
		t.Fatalf("BackupInfo.Tables = %d, want 1", info.Tables)
	}
	if info.MaxSeq == 0 {
		t.Fatalf("BackupInfo.MaxSeq = 0, want the flushed sequence")
	}

	restored := openRestored(t, dest)
	for _, k := range keys {
		e, ok := mustGet(t, restored, k)
		if !ok {
			t.Fatalf("key %q missing after restore", k)
		}
		if got := e.Siblings[0].Value; got != "v-"+k {
			t.Fatalf("restored %q = %q, want %q", k, got, "v-"+k)
		}
	}

	// A prefix scan over the restored store returns the ordered matches.
	items, err := restored.Scan("user:", "", 10)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("restored scan returned %d keys, want 3", len(items))
	}
}

// TestBackupFlushesMemtable: keys that live only in the memtable (never
// manually flushed) must still be captured, because Backup flushes first.
func TestBackupFlushesMemtable(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("only-in-mem", "v", vclock("w", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n := s.TableCount(); n != 0 {
		t.Fatalf("expected 0 tables before backup, got %d", n)
	}

	dest := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(dest); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	restored := openRestored(t, dest)
	if _, ok := mustGet(t, restored, "only-in-mem"); !ok {
		t.Fatal("memtable-only key not captured by backup")
	}
}

// TestBackupEmptyStore: backing up a store with no data produces a valid,
// empty, restorable backup.
func TestBackupEmptyStore(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	dest := filepath.Join(t.TempDir(), "backup")
	info, err := s.Backup(dest)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if info.Tables != 0 {
		t.Fatalf("BackupInfo.Tables = %d, want 0", info.Tables)
	}
	restored := openRestored(t, dest)
	if _, ok := mustGet(t, restored, "anything"); ok {
		t.Fatal("empty backup restored a phantom key")
	}
}

func TestBackupEmptyDestination(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if _, err := s.Backup(""); err == nil || !strings.Contains(err.Error(), "destination is empty") {
		t.Fatalf("Backup(\"\") err = %v, want destination-empty error", err)
	}
}

func TestBackupRequiresFlushing(t *testing.T) {
	s := New() // memory-only: no flush policy
	if _, err := s.Backup(t.TempDir()); err == nil || !strings.Contains(err.Error(), "requires flushing") {
		t.Fatalf("Backup err = %v, want requires-flushing error", err)
	}
}

func TestBackupRefusesExistingBackup(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("k", "v", vclock("w", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(dest); err != nil {
		t.Fatalf("first Backup: %v", err)
	}
	if _, err := s.Backup(dest); err == nil || !strings.Contains(err.Error(), "already holds a backup") {
		t.Fatalf("second Backup err = %v, want already-holds-a-backup error", err)
	}
}

func TestBackupStatError(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	// Point the destination at a regular file: stat of destDir/MANIFEST then
	// fails with a non-"not exist" error (the path component is not a dir).
	destFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(destFile, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := s.Backup(destFile); err == nil || !strings.Contains(err.Error(), "stat backup dir") {
		t.Fatalf("Backup err = %v, want stat error", err)
	}
}

func TestBackupLinkError(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("k", "v", vclock("w", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// The first flush allocates file number 1. Pre-create a file with that name
	// in the destination so os.Link fails with EEXIST.
	dest := filepath.Join(t.TempDir(), "backup")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dest, tableName(1)), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := s.Backup(dest); err == nil || !strings.Contains(err.Error(), "link table") {
		t.Fatalf("Backup err = %v, want link error", err)
	}
}
