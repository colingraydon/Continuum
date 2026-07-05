package store

import (
	"errors"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
)

func openTestWAL(t *testing.T, dir string) *wal.Writer {
	t.Helper()
	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	return w
}

func TestStore_WALDurabilityRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// First lifecycle: write through the WAL.
	s := New()
	w := openTestWAL(t, dir)
	s.SetWAL(w)

	if err := s.Put("alpha", "one", newTestVC(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put alpha: %v", err)
	}
	if err := s.Put("beta", "two", newTestVC(map[string]uint64{"a": 2})); err != nil {
		t.Fatalf("Put beta: %v", err)
	}
	if err := s.Delete("gamma", newTestVC(map[string]uint64{"a": 3})); err != nil {
		t.Fatalf("Delete gamma: %v", err)
	}
	gammaVal, _ := s.mem.get("gamma")
	beforeAge := gammaVal.age
	if err := w.Close(); err != nil {
		t.Fatalf("Close wal: %v", err)
	}

	// Second lifecycle: replay into a fresh store.
	s2 := New()
	r, err := wal.NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	lastSeq, err := s2.Replay(r, 0)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if lastSeq != 3 {
		t.Fatalf("lastSeq = %d, want 3", lastSeq)
	}
	assertStoresEqual(t, s, s2)
	gamma2Val, _ := s2.mem.get("gamma")
	if !gamma2Val.age.Equal(beforeAge) {
		t.Fatalf("tombstone age changed across replay: %v vs %v", beforeAge, gamma2Val.age)
	}
}

func TestStore_WALEvictAndGCReplay(t *testing.T) {
	dir := t.TempDir()
	s := New()
	w := openTestWAL(t, dir)
	s.SetWAL(w)

	// Put then Evict — replay should leave the key absent.
	if err := s.Put("evict-me", "v", newTestVC(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Evict("evict-me"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	// Delete + GC — replay should leave the tombstone gone.
	if err := s.Delete("gc-me", newTestVC(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	s.SetTombstoneAge("gc-me", time.Now().Add(-2*time.Hour))
	purged, err := s.GCTombstones(time.Hour)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(purged) != 1 || purged[0] != "gc-me" {
		t.Fatalf("GC purged = %v, want [gc-me]", purged)
	}
	w.Close()

	s2 := New()
	r, _ := wal.NewReader(dir)
	defer r.Close()
	if _, err := s2.Replay(r, 0); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, ok := memData(s2)["evict-me"]; ok {
		t.Fatalf("evicted key still present after replay")
	}
	if _, ok := memData(s2)["gc-me"]; ok {
		t.Fatalf("GC'd key still present after replay")
	}
}

func TestStore_ReplaySkipsBelow(t *testing.T) {
	dir := t.TempDir()
	s := New()
	w := openTestWAL(t, dir)
	s.SetWAL(w)
	s.Put("a", "1", newTestVC(map[string]uint64{"x": 1})) // seq 1
	s.Put("b", "2", newTestVC(map[string]uint64{"x": 2})) // seq 2
	s.Put("c", "3", newTestVC(map[string]uint64{"x": 3})) // seq 3
	w.Close()

	s2 := New()
	r, _ := wal.NewReader(dir)
	defer r.Close()
	if _, err := s2.Replay(r, 2); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, ok := memData(s2)["a"]; ok {
		t.Fatalf("a should be skipped")
	}
	if _, ok := memData(s2)["b"]; ok {
		t.Fatalf("b should be skipped")
	}
	if _, ok := memData(s2)["c"]; !ok {
		t.Fatalf("c should be applied")
	}
}

func TestStore_WriteCheckpointDuringReplay(t *testing.T) {
	dir := t.TempDir()
	s := New()
	w := openTestWAL(t, dir)
	s.SetWAL(w)
	s.Put("a", "1", newTestVC(map[string]uint64{"x": 1}))
	if err := s.WriteCheckpoint(1); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	s.Put("b", "2", newTestVC(map[string]uint64{"x": 2}))
	w.Close()

	s2 := New()
	r, _ := wal.NewReader(dir)
	defer r.Close()
	if _, err := s2.Replay(r, 0); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, ok := memData(s2)["a"]; !ok {
		t.Fatalf("a missing after replay")
	}
	if _, ok := memData(s2)["b"]; !ok {
		t.Fatalf("b missing after replay")
	}
}

func TestApplyWALRecord_UnknownType(t *testing.T) {
	s := New()
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.applyWALRecord([]byte{0xff, 1, 2})
	if err == nil || !errors.Is(err, errUnknownRecordType) {
		t.Fatalf("expected unknown record type error, got %v", err)
	}
}

func TestApplyWALRecord_EmptyPayload(t *testing.T) {
	s := New()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.applyWALRecord(nil); err == nil {
		t.Fatalf("expected error on empty payload")
	}
}

func TestStore_WALOff_NoFiles(t *testing.T) {
	// Sanity: with no WAL installed, Put/Delete don't error and don't touch disk.
	s := New()
	if err := s.Put("k", "v", newTestVC(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete("k2", newTestVC(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Evict("k3"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if _, err := s.GCTombstones(time.Hour); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if err := s.WriteCheckpoint(99); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
}
