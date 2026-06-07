package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/store"
)

func vc(clocks map[string]uint64) store.VectorClockVersion {
	return store.VectorClockVersion{Clocks: clocks}
}

func TestRecoverStore_EmptyDataDir(t *testing.T) {
	s, p, err := recoverStore("", "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if p != nil {
		t.Fatalf("expected nil persistence for empty data dir")
	}
	if s == nil {
		t.Fatalf("expected non-nil store")
	}
}

func TestRecoverStore_FreshDir(t *testing.T) {
	dir := t.TempDir()
	s, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if p == nil {
		t.Fatalf("expected non-nil persistence")
	}
	if s == nil {
		t.Fatalf("expected non-nil store")
	}
	// snap and wal dirs should exist.
	if _, err := os.Stat(filepath.Join(dir, snapDirName)); err != nil {
		t.Fatalf("snap dir missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, walDirName)); err != nil {
		t.Fatalf("wal dir missing: %v", err)
	}
	if err := p.finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	// meta.json should now exist.
	if _, err := os.Stat(filepath.Join(dir, metaFile)); err != nil {
		t.Fatalf("meta missing after finalize: %v", err)
	}
}

func TestRecoverStore_RoundTripPreservesData(t *testing.T) {
	dir := t.TempDir()
	s, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if err := s.Put("k1", "v1", vc(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put("k2", "v2", vc(map[string]uint64{"a": 2})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete("k3", vc(map[string]uint64{"a": 3})); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := p.finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// Reopen and verify.
	s2, p2, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore reopen: %v", err)
	}
	defer p2.finalize()
	if e, ok := s2.Get("k1"); !ok || e.Siblings[0].Value != "v1" {
		t.Fatalf("k1 missing or wrong after reopen: %v", e)
	}
	if e, ok := s2.Get("k2"); !ok || e.Siblings[0].Value != "v2" {
		t.Fatalf("k2 missing or wrong after reopen: %v", e)
	}
	if e, ok := s2.Get("k3"); !ok || !e.Siblings[0].Deleted {
		t.Fatalf("k3 should be tombstoned: %v", e)
	}
}

func TestRecoverStore_RoundTripWithoutSnapshot(t *testing.T) {
	// Skip finalize on the first lifecycle so recovery has to replay the WAL
	// from scratch with no snapshot to skip ahead from. The WAL records are
	// still on disk because each Put fsyncs them.
	dir := t.TempDir()
	s, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if err := s.Put("k", "v", vc(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Write meta manually with a recent last_clean_shutdown so the downtime
	// gate doesn't fire on the next open.
	meta := persistMeta{NodeID: "node-A", LastCleanShutdown: time.Now()}
	if err := writePersistMeta(filepath.Join(dir, metaFile), meta); err != nil {
		t.Fatalf("writeMeta: %v", err)
	}
	// Close the writer without taking a snapshot.
	if err := p.w.Close(); err != nil {
		t.Fatalf("wal close: %v", err)
	}

	s2, p2, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore reopen: %v", err)
	}
	defer p2.finalize()
	if e, ok := s2.Get("k"); !ok || e.Siblings[0].Value != "v" {
		t.Fatalf("k missing or wrong after WAL-only recovery: %v", e)
	}
}

func TestRecoverStore_DowntimeGateDiscardsData(t *testing.T) {
	dir := t.TempDir()
	s, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if err := s.Put("k", "v", vc(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := p.finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// Backdate last_clean_shutdown well past the TTL.
	prior, _, _ := readPersistMeta(filepath.Join(dir, metaFile))
	prior.LastCleanShutdown = time.Now().Add(-25 * time.Hour)
	if err := writePersistMeta(filepath.Join(dir, metaFile), prior); err != nil {
		t.Fatalf("writeMeta: %v", err)
	}

	s2, p2, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore reopen: %v", err)
	}
	defer p2.finalize()
	if _, ok := s2.Get("k"); ok {
		t.Fatalf("key should be discarded after downtime gate fires")
	}
}

func TestRecoverStore_IdentityMismatchRefused(t *testing.T) {
	dir := t.TempDir()
	s, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if err := s.Put("k", "v", vc(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := p.finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	_, _, err = recoverStore(dir, "node-B", time.Hour)
	if err == nil || !strings.Contains(err.Error(), "data dir owned by") {
		t.Fatalf("expected identity mismatch, got %v", err)
	}
}

func TestRecoverStore_CleansSnapTmp(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, snapDirName), 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	junk := filepath.Join(dir, snapDirName, "00000000000000000001.snap.tmp")
	if err := os.WriteFile(junk, []byte("garbage"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	defer p.finalize()
	if _, err := os.Stat(junk); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snap.tmp should be cleaned up: %v", err)
	}
}

func TestFinalize_NilReceiverNoOp(t *testing.T) {
	var p *persistence
	if err := p.finalize(); err != nil {
		t.Fatalf("nil finalize: %v", err)
	}
}

func TestRecoverStore_CrashWithoutFinalizeDiscardsData(t *testing.T) {
	// Simulate: writes go through the WAL, process crashes before finalize.
	// Recovery has no meta → downtime gate fires → data is discarded.
	dir := t.TempDir()
	s, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if err := s.Put("k", "v", vc(map[string]uint64{"a": 1})); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Simulate SIGKILL: close the WAL handle (mimics kernel reaping the fd)
	// but do not call finalize. No meta.json gets written.
	if err := p.w.Close(); err != nil {
		t.Fatalf("wal close: %v", err)
	}

	s2, p2, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore reopen: %v", err)
	}
	defer p2.finalize()
	if _, ok := s2.Get("k"); ok {
		t.Fatalf("expected store empty after gate fires, found key")
	}
}

func TestRecoverStore_TruncatesCoveredWALAfterFinalize(t *testing.T) {
	dir := t.TempDir()
	s, p, err := recoverStore(dir, "node-A", time.Hour)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	for i := range 3 {
		if err := s.Put("k", "v", vc(map[string]uint64{"a": uint64(i + 1)})); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := p.finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// The single segment containing seqs 1..3 is the current segment and is
	// preserved by TruncateThrough — only segments fully covered by a later
	// segment are eligible. There should still be exactly one .wal file.
	entries, err := os.ReadDir(filepath.Join(dir, walDirName))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	count := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 wal segment after finalize, got %d", count)
	}
}
