package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func mustScan(t *testing.T, s *Store, prefix, after string, limit int) []KeyItem {
	t.Helper()
	items, err := s.Scan(prefix, after, limit)
	if err != nil {
		t.Fatalf("Scan(%q, %q, %d): %v", prefix, after, limit, err)
	}
	return items
}

func scanKeys(items []KeyItem) []string {
	keys := make([]string, len(items))
	for i, it := range items {
		keys[i] = it.Key
	}
	return keys
}

func assertKeys(t *testing.T, items []KeyItem, want ...string) {
	t.Helper()
	got := scanKeys(items)
	if len(got) != len(want) {
		t.Fatalf("scan returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan returned %v, want %v", got, want)
		}
	}
}

// TestScanAcrossGenerations proves the merged view: keys only in tables, keys
// only in the memtable, and a table key overwritten in the memtable (the
// memtable version must win), with the prefix excluding unrelated keys.
func TestScanAcrossGenerations(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	for i, k := range []string{"sc-a", "sc-b", "sc-c", "other-x"} {
		if err := s.Put(k, "table-"+k, vclock("w", uint64(i+1))); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Overwrite one table key and add a memtable-only key.
	if err := s.Put("sc-b", "mem-sc-b", vclock("w", 10)); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if err := s.Put("sc-d", "mem-sc-d", vclock("w", 11)); err != nil {
		t.Fatalf("mem put: %v", err)
	}

	items := mustScan(t, s, "sc-", "", 100)
	assertKeys(t, items, "sc-a", "sc-b", "sc-c", "sc-d")
	if v := items[1].Entry.Siblings[0].Value; v != "mem-sc-b" {
		t.Errorf("sc-b = %q, want the memtable overwrite to win", v)
	}
	if v := items[0].Entry.Siblings[0].Value; v != "table-sc-a" {
		t.Errorf("sc-a = %q, want the table value", v)
	}
}

// TestScanEvictShadowing proves evictions hide keys at both stages: while the
// marker sits in the memtable's evicted set, and after a flush moves it into
// a newer table as an evict marker shadowing the older table's copy.
func TestScanEvictShadowing(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	for i, k := range []string{"ev-a", "ev-b", "ev-c"} {
		if err := s.Put(k, "v", vclock("w", uint64(i+1))); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Evict("ev-b"); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	assertKeys(t, mustScan(t, s, "ev-", "", 100), "ev-a", "ev-c")

	// Second flush writes the evict marker into a newer table; the older
	// table's copy must stay hidden via the marker-delete path in scanTables.
	if err := s.Flush(); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	assertKeys(t, mustScan(t, s, "ev-", "", 100), "ev-a", "ev-c")
}

// TestScanIncludesTombstones: deleted keys stay in scan output with their
// tombstone siblings, so the scatter-gather coordinator can let a tombstone
// dominate a stale live value from another node. Callers filter for display.
func TestScanIncludesTombstones(t *testing.T) {
	s := New()
	if err := s.Put("ts-a", "v", vclock("w", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete("ts-a", vclock("w", 2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	items := mustScan(t, s, "ts-", "", 100)
	assertKeys(t, items, "ts-a")
	if sib := items[0].Entry.Siblings; len(sib) != 1 || !sib[0].Deleted {
		t.Errorf("expected a lone tombstone sibling, got %+v", sib)
	}
}

// TestScanPagination walks a keyspace with limit+after pages and asserts each
// key appears exactly once in order.
func TestScanPagination(t *testing.T) {
	s := New()
	const n = 10
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("pg-%02d", i)
		if err := s.Put(key, "v", vclock("w", uint64(i+1))); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}

	var got []string
	after := ""
	for {
		items := mustScan(t, s, "pg-", after, 3)
		if len(items) == 0 {
			break
		}
		got = append(got, scanKeys(items)...)
		after = items[len(items)-1].Key
	}
	if len(got) != n {
		t.Fatalf("pagination visited %d keys, want %d: %v", len(got), n, got)
	}
	for i, k := range got {
		if want := fmt.Sprintf("pg-%02d", i); k != want {
			t.Fatalf("page order broken at %d: got %q want %q", i, k, want)
		}
	}
}

// TestScanFrozenOverlay covers the frozen-memtable generation: its data must
// override tables, its evictions must hide table keys, and the active
// memtable must override it in turn.
func TestScanFrozenOverlay(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	for i, k := range []string{"fz-a", "fz-b", "fz-c"} {
		if err := s.Put(k, "table", vclock("w", uint64(i+1))); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Install a frozen generation directly (white-box): overrides fz-a,
	// evicts fz-b.
	fz := newMemtable()
	fz.putEntry("fz-a", Entry{Siblings: []Sibling{{Value: "frozen", Version: vclock("w", 5)}}}, time.Time{}, len("frozen"))
	fz.evict("fz-b")
	s.mu.Lock()
	s.frozen = fz
	s.mu.Unlock()

	items := mustScan(t, s, "fz-", "", 100)
	assertKeys(t, items, "fz-a", "fz-c")
	if v := items[0].Entry.Siblings[0].Value; v != "frozen" {
		t.Errorf("fz-a = %q, want frozen generation to override the table", v)
	}

	// The active memtable overrides frozen in turn.
	if err := s.Put("fz-a", "active", vclock("w", 9)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	items = mustScan(t, s, "fz-", "", 100)
	if v := items[0].Entry.Siblings[0].Value; v != "active" {
		t.Errorf("fz-a = %q, want active memtable to override frozen", v)
	}
}

// TestScanSiblingConflicts: concurrent writes surface as multiple siblings in
// scan results, same as Get.
func TestScanSiblingConflicts(t *testing.T) {
	s := New()
	if err := s.Put("cf-a", "v1", vclock("n1", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put("cf-a", "v2", vclock("n2", 1)); err != nil {
		t.Fatalf("Put concurrent: %v", err)
	}
	items := mustScan(t, s, "cf-", "", 100)
	assertKeys(t, items, "cf-a")
	if len(items[0].Entry.Siblings) != 2 {
		t.Errorf("expected 2 concurrent siblings, got %d", len(items[0].Entry.Siblings))
	}
}

// TestScanTableReadError proves a corrupted table surfaces as a scan error
// instead of silently missing keys: the reader's in-memory index still points
// at the old block offsets, so the scan's block read fails its CRC check.
func TestScanTableReadError(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	if err := s.Put("cr-a", "v", vclock("w", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(tablesDir, "*.sst"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no table files found: %v", err)
	}
	info, err := os.Stat(files[0])
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.WriteFile(files[0], make([]byte, info.Size()), 0o644); err != nil {
		t.Fatalf("corrupt table: %v", err)
	}

	if _, err := s.Scan("cr-", "", 10); err == nil {
		t.Fatal("expected scan error from corrupted table, got nil")
	}
}

func TestScanEmptyAndLimits(t *testing.T) {
	s := New()
	if items := mustScan(t, s, "x-", "", 100); len(items) != 0 {
		t.Errorf("empty store scan returned %v", items)
	}
	if err := s.Put("lm-a", "v", vclock("w", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if items := mustScan(t, s, "lm-", "", 0); items != nil {
		t.Errorf("limit 0 returned %v", items)
	}
	if items := mustScan(t, s, "zz-", "", 10); len(items) != 0 {
		t.Errorf("non-matching prefix returned %v", items)
	}
}
