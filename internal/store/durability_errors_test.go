package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
)

// failingWAL is a configurable fake satisfying the WAL interface. Tests
// flip appendErr or syncErr to assert the store leaves memory unmodified
// when the log fails.
type failingWAL struct {
	appendErr error
	syncErr   error
	appended  [][]byte
	synced    int
}

func (f *failingWAL) Append(payload []byte) (uint64, error) {
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	f.appended = append(f.appended, append([]byte(nil), payload...))
	return uint64(len(f.appended)), nil
}

func (f *failingWAL) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	f.synced++
	return nil
}

func TestStore_Put_WALAppendError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{appendErr: errors.New("append failed")})
	err := s.Put("k", "v", newTestVC(map[string]uint64{"a": 1}))
	if err == nil || !strings.Contains(err.Error(), "append failed") {
		t.Fatalf("got %v", err)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatalf("memory must not be modified on Append failure")
	}
}

func TestStore_Put_WALSyncError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{syncErr: errors.New("sync failed")})
	if err := s.Put("k", "v", newTestVC(map[string]uint64{"a": 1})); err == nil {
		t.Fatalf("expected sync error")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatalf("memory must not be modified on Sync failure")
	}
}

func TestStore_Delete_WALAppendError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{appendErr: errors.New("boom")})
	if err := s.Delete("k", newTestVC(map[string]uint64{"a": 1})); err == nil {
		t.Fatalf("expected error")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatalf("memory must not be modified")
	}
}

func TestStore_Delete_WALSyncError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{syncErr: errors.New("boom")})
	if err := s.Delete("k", newTestVC(map[string]uint64{"a": 1})); err == nil {
		t.Fatalf("expected error")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatalf("memory must not be modified")
	}
}

func TestStore_Evict_WALAppendError(t *testing.T) {
	s := New()
	// Seed a key so Evict has something to remove.
	s.Put("k", "v", newTestVC(map[string]uint64{"a": 1}))
	s.SetWAL(&failingWAL{appendErr: errors.New("boom")})
	if err := s.Evict("k"); err == nil {
		t.Fatalf("expected error")
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatalf("memory must not be modified on Evict failure")
	}
}

func TestStore_Evict_WALSyncError(t *testing.T) {
	s := New()
	s.Put("k", "v", newTestVC(map[string]uint64{"a": 1}))
	s.SetWAL(&failingWAL{syncErr: errors.New("boom")})
	if err := s.Evict("k"); err == nil {
		t.Fatalf("expected error")
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatalf("memory must not be modified")
	}
}

func TestStore_GCTombstones_WALAppendError(t *testing.T) {
	s := New()
	s.Delete("k", newTestVC(map[string]uint64{"a": 1}))
	s.tombstoneAges["k"] = time.Now().Add(-2 * time.Hour)
	s.SetWAL(&failingWAL{appendErr: errors.New("boom")})
	purged, err := s.GCTombstones(time.Hour)
	if err == nil {
		t.Fatalf("expected error")
	}
	if purged != nil {
		t.Fatalf("expected nil purged on error, got %v", purged)
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatalf("tombstone must remain when WAL fails")
	}
}

func TestStore_GCTombstones_WALSyncError(t *testing.T) {
	s := New()
	s.Delete("k", newTestVC(map[string]uint64{"a": 1}))
	s.tombstoneAges["k"] = time.Now().Add(-2 * time.Hour)
	s.SetWAL(&failingWAL{syncErr: errors.New("boom")})
	if _, err := s.GCTombstones(time.Hour); err == nil {
		t.Fatalf("expected error")
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatalf("tombstone must remain")
	}
}

func TestStore_WriteCheckpoint_WALAppendError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{appendErr: errors.New("boom")})
	if err := s.WriteCheckpoint(99); err == nil {
		t.Fatalf("expected error")
	}
}

func TestStore_WriteCheckpoint_WALSyncError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{syncErr: errors.New("boom")})
	if err := s.WriteCheckpoint(99); err == nil {
		t.Fatalf("expected error")
	}
}

func TestStore_SetWAL_NilDisables(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{appendErr: errors.New("boom")})
	s.SetWAL(nil)
	if err := s.Put("k", "v", newTestVC(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put with nil WAL should not error, got %v", err)
	}
}

// Long-input encode failures are propagated by the mutating store methods.

func TestStore_Put_EncodeError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{})
	long := strings.Repeat("k", 1<<16)
	if err := s.Put(long, "v", newTestVC(map[string]uint64{"a": 1})); err == nil {
		t.Fatalf("expected encode error for over-long key")
	}
}

func TestStore_Delete_EncodeError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{})
	long := strings.Repeat("k", 1<<16)
	if err := s.Delete(long, newTestVC(map[string]uint64{"a": 1})); err == nil {
		t.Fatalf("expected encode error")
	}
}

func TestStore_Evict_EncodeError(t *testing.T) {
	s := New()
	s.SetWAL(&failingWAL{})
	long := strings.Repeat("k", 1<<16)
	if err := s.Evict(long); err == nil {
		t.Fatalf("expected encode error")
	}
}

func TestStore_Replay_DecodeError(t *testing.T) {
	s := New()
	s.mu.Lock()
	defer s.mu.Unlock()
	// PUT with truncated payload.
	if err := s.applyWALRecord([]byte{recPut, 0, 5, 'a'}); err == nil {
		t.Fatalf("expected decode error")
	}
	if err := s.applyWALRecord([]byte{recDelete, 0, 5, 'a'}); err == nil {
		t.Fatalf("expected decode error")
	}
	if err := s.applyWALRecord([]byte{recEvict, 0, 5, 'a'}); err == nil {
		t.Fatalf("expected decode error")
	}
	if err := s.applyWALRecord([]byte{recGC, 0}); err == nil {
		t.Fatalf("expected decode error")
	}
	if err := s.applyWALRecord([]byte{recCheckpoint, 1, 2}); err == nil {
		t.Fatalf("expected decode error")
	}
}

func TestStore_Replay_ApplyError(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append([]byte{0xFF}); err != nil { // unknown record type
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := wal.NewReader(walDir)
	if err != nil {
		t.Fatal(err)
	}
	s := New()
	if _, err := s.Replay(r, 0); err == nil {
		t.Fatal("expected Replay to return error for unknown record type")
	}
	r.Close()
}

func TestStore_SetTombstoneAge(t *testing.T) {
	s := New()
	if err := s.Delete("k", newTestVC(map[string]uint64{"a": 1})); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-2 * time.Hour)
	s.SetTombstoneAge("k", past)
	purged, err := s.GCTombstones(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(purged) != 1 || purged[0] != "k" {
		t.Fatalf("expected key purged after SetTombstoneAge, got %v", purged)
	}
}
