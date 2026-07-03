package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIncarnationStore_LoadMissingReturnsZero(t *testing.T) {
	s := newIncarnationStore(t.TempDir())
	if got := s.load(); got != 0 {
		t.Errorf("expected 0 for missing file, got %d", got)
	}
}

func TestIncarnationStore_RoundTrip(t *testing.T) {
	s := newIncarnationStore(t.TempDir())
	if err := s.store(7); err != nil {
		t.Fatalf("store: %v", err)
	}
	if got := s.load(); got != 7 {
		t.Errorf("expected 7, got %d", got)
	}
}

// A fresh store reopened over the same dir sees the persisted value, and the
// load()+1 pattern advances monotonically across restarts.
func TestIncarnationStore_MonotonicAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	first := newIncarnationStore(dir)
	next := first.load() + 1
	if err := first.store(next); err != nil {
		t.Fatalf("store: %v", err)
	}

	second := newIncarnationStore(dir)
	if got := second.load(); got != 1 {
		t.Fatalf("expected reopened store to load 1, got %d", got)
	}
	if again := second.load() + 1; again != 2 {
		t.Errorf("expected next incarnation 2, got %d", again)
	}
}

func TestIncarnationStore_CorruptReturnsZero(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, incarnationFile), []byte("not-a-number"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := newIncarnationStore(dir).load(); got != 0 {
		t.Errorf("expected 0 for corrupt file, got %d", got)
	}
}

func TestIncarnationStore_OverwriteKeepsLatest(t *testing.T) {
	s := newIncarnationStore(t.TempDir())
	for _, v := range []uint64{1, 2, 3, 10} {
		if err := s.store(v); err != nil {
			t.Fatalf("store(%d): %v", v, err)
		}
	}
	if got := s.load(); got != 10 {
		t.Errorf("expected latest value 10, got %d", got)
	}
}

func TestIncarnationStore_NoTmpLeftBehind(t *testing.T) {
	dir := t.TempDir()
	s := newIncarnationStore(dir)
	if err := s.store(5); err != nil {
		t.Fatalf("store: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, incarnationFile+".tmp")); !os.IsNotExist(err) {
		t.Errorf("expected temp file to be renamed away, stat err = %v", err)
	}
}
