package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/sstable"
	"github.com/colingraydon/continuum/internal/wal"
)

// openRaw writes a table with arbitrary bytes and returns an open reader,
// closed at test end.
func openRaw(t *testing.T, dir, name string, kv [][2][]byte) *sstable.Reader {
	t.Helper()
	path := filepath.Join(dir, name)
	writeRawTable(t, path, kv)
	r, err := sstable.Open(path)
	if err != nil {
		t.Fatalf("open raw table: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// writeRawTable writes an SSTable with arbitrary key/value bytes, bypassing the
// store's value encoding so tests can inject malformed table entries.
func writeRawTable(t *testing.T, path string, kv [][2][]byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create raw table: %v", err)
	}
	w := sstable.NewWriter(f, sstable.Options{})
	for _, e := range kv {
		if err := w.Add(e[0], e[1]); err != nil {
			t.Fatalf("raw table add: %v", err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("raw table finish: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("raw table close: %v", err)
	}
}

// blockManifestWrites makes every subsequent writeManifest in dir fail by
// occupying its temp path with a directory.
func blockManifestWrites(t *testing.T, dir string) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(dir, manifestFile+".tmp"), 0o755); err != nil {
		t.Fatalf("block manifest writes: %v", err)
	}
}

func TestCompactMemoryOnlyNoop(t *testing.T) {
	s := New()
	merged, err := s.Compact(time.Hour)
	if merged || err != nil {
		t.Fatalf("Compact on memory-only store = (%v, %v), want (false, nil)", merged, err)
	}
}

func TestOpenTablesMissingTableInManifest(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, manifest{Tables: []string{tableName(1)}, NextFileNum: 2}); err != nil {
		t.Fatal(err)
	}
	s := New()
	if _, err := s.OpenTables(dir); err == nil {
		t.Fatal("OpenTables should error when the manifest names a missing table")
	}
}

func TestOpenTablesGarbageManifest(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, manifestFile), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := New()
	if _, err := s.OpenTables(dir); err == nil {
		t.Fatal("OpenTables should propagate a manifest parse error")
	}
}

func TestOpenTablesBadLegacyName(t *testing.T) {
	dir := t.TempDir()
	// Legacy dir (no manifest) with a non-numeric table name.
	if err := os.WriteFile(filepath.Join(dir, "bogus"+tableSuffix), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := New()
	if _, err := s.OpenTables(dir); err == nil {
		t.Fatal("OpenTables should error on a non-numeric legacy table name")
	}
}

func TestFinishFlushManifestError(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	blockManifestWrites(t, tablesDir)

	if err := s.Put("k", "v", vclock("c", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err == nil {
		t.Fatal("Flush should fail when the manifest cannot be written")
	}
	if n := s.TableCount(); n != 0 {
		t.Fatalf("TableCount = %d, want 0 (table must not attach)", n)
	}
	if n := countSST(t, tablesDir); n != 0 {
		t.Fatalf("%d orphan .sst left on disk, want 0", n)
	}
}

func TestCompactCommitManifestErrorDiscardsMerged(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 1e9)
	for i, k := range []string{"a", "b", "c", "d"} {
		putFlush(t, s, k, "v", uint64(i+1))
	}
	blockManifestWrites(t, tablesDir)

	if _, err := s.Compact(time.Hour); err == nil {
		t.Fatal("Compact should fail when the commit manifest write fails")
	}
	if n := s.TableCount(); n != 4 {
		t.Fatalf("TableCount = %d, want 4 (sources must remain)", n)
	}
	if n := countSST(t, tablesDir); n != 4 {
		t.Fatalf("%d .sst on disk, want 4 (merged table must be discarded)", n)
	}
}

func TestCompactTableSizesError(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 1e9)
	for i, k := range []string{"a", "b", "c", "d"} {
		putFlush(t, s, k, "v", uint64(i+1))
	}
	// Remove a live table file out from under the store; sizing it fails.
	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tableSuffix) && !strings.HasSuffix(e.Name(), tableTmpSuffix) {
			if err := os.Remove(filepath.Join(tablesDir, e.Name())); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if _, err := s.Compact(time.Hour); err == nil {
		t.Fatal("Compact should fail when a source table cannot be stat'd")
	}
}

func TestDroppableAtBottom(t *testing.T) {
	cutoff := time.Now().Add(-24 * time.Hour)
	tomb := func(age time.Time) []byte {
		v, err := encodeTableEntry("k", Entry{Siblings: []Sibling{{Deleted: true, Version: vclock("c", 1)}}},
			map[string]time.Time{"k": age})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	live, err := encodeTableEntry("k", Entry{Siblings: []Sibling{{Value: "v", Version: vclock("c", 1)}}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		val     []byte
		want    bool
		wantErr bool
	}{
		{"evict marker", tableEvictValue, true, false},
		{"unknown kind", []byte{0x7f}, false, true},
		{"aged tombstone", tomb(time.Now().Add(-48 * time.Hour)), true, false},
		{"fresh tombstone", tomb(time.Now()), false, false},
		{"live entry", live, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drop, err := droppableAtBottom(tc.val, cutoff)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && drop != tc.want {
				t.Fatalf("drop = %v, want %v", drop, tc.want)
			}
		})
	}
}

func TestDecodeTableEntryErrors(t *testing.T) {
	if _, _, err := decodeTableEntry(nil); err == nil {
		t.Fatal("empty value should error")
	}
	if _, _, err := decodeTableEntry([]byte{0x7f}); err == nil {
		t.Fatal("unknown kind should error")
	}
	// kind entry, sibling count 1, but no sibling bytes follow.
	if _, _, err := decodeTableEntry([]byte{tableKindEntry, 0x00, 0x01}); err == nil {
		t.Fatal("truncated sibling should error")
	}
	if _, evicted, err := decodeTableEntry(tableEvictValue); err != nil || !evicted {
		t.Fatalf("evict marker = (evicted=%v, err=%v)", evicted, err)
	}
}

func TestEncodeDecodeTableEntryRoundTrip(t *testing.T) {
	ages := map[string]time.Time{"k": time.Unix(1234567890, 0)}
	e := Entry{Siblings: []Sibling{{Value: "hello", Version: vclock("c", 3), Hash: 42}}}
	val, err := encodeTableEntry("k", e, ages)
	if err != nil {
		t.Fatalf("encodeTableEntry: %v", err)
	}
	got, evicted, tombAt, hasTomb, err := decodeTableEntryFull(val)
	if err != nil || evicted {
		t.Fatalf("decode = (evicted=%v, err=%v)", evicted, err)
	}
	if !hasTomb || !tombAt.Equal(time.Unix(1234567890, 0)) {
		t.Fatalf("tombAt = %v, hasTomb = %v", tombAt, hasTomb)
	}
	if len(got.Siblings) != 1 || got.Siblings[0].Value != "hello" {
		t.Fatalf("round-trip siblings = %+v", got.Siblings)
	}
}

func TestTableDecodeErrorSurfacesOnRead(t *testing.T) {
	dir := t.TempDir()
	writeRawTable(t, filepath.Join(dir, tableName(1)), [][2][]byte{{[]byte("k"), {0x7f}}})
	if err := writeManifest(dir, manifest{Tables: []string{tableName(1)}, MaxSeq: 1, NextFileNum: 2}); err != nil {
		t.Fatal(err)
	}
	s := New()
	if _, err := s.OpenTables(dir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	t.Cleanup(func() { _ = s.CloseTables() })

	if _, _, err := s.Get("k"); err == nil {
		t.Fatal("Get should surface a table decode error")
	}
	if _, err := s.KeyHashes(); err == nil {
		t.Fatal("KeyHashes should surface a table decode error")
	}
}

func TestFlushWriteTableCreateError(t *testing.T) {
	dir := t.TempDir()
	notDir := filepath.Join(dir, "tables-is-a-file")
	if err := os.WriteFile(notDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })

	s := New()
	s.SetWAL(w)
	s.SetFlushPolicy(notDir, 0) // flushDir is a regular file -> os.Create fails
	if err := s.Put("k", "v", vclock("c", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err == nil {
		t.Fatal("Flush should fail when the table file cannot be created")
	}
}

func TestCompactionBottomDecodeError(t *testing.T) {
	dir := t.TempDir()
	// Two tables with malformed store values; merging them at the bottom must
	// surface the decode error rather than corrupting the merged table.
	writeRawTable(t, filepath.Join(dir, tableName(1)), [][2][]byte{{[]byte("a"), {0x7f}}})
	writeRawTable(t, filepath.Join(dir, tableName(2)), [][2][]byte{{[]byte("b"), {0x7f}}})
	if err := writeManifest(dir, manifest{Tables: []string{tableName(2), tableName(1)}, MaxSeq: 2, NextFileNum: 3}); err != nil {
		t.Fatal(err)
	}
	s := New()
	if _, err := s.OpenTables(dir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	t.Cleanup(func() { _ = s.CloseTables() })
	s.SetFlushPolicy(dir, 0) // enable compaction
	s.SetCompactionPolicy(2, 32, 1e9)

	if _, err := s.Compact(time.Hour); err == nil {
		t.Fatal("Compact should surface a bottom-level decode error")
	}
}

func TestSpliceTablesNotFound(t *testing.T) {
	dir := t.TempDir()
	r1 := openRaw(t, dir, tableName(1), [][2][]byte{{[]byte("a"), tableEvictValue}})
	r2 := openRaw(t, dir, tableName(2), [][2][]byte{{[]byte("b"), tableEvictValue}})

	cur := []liveTable{{r: r1, file: tableName(1)}}
	if _, _, ok := spliceTables(cur, []liveTable{{r: r2, file: tableName(2)}}, liveTable{}, false); ok {
		t.Fatal("ok should be false when the source run is absent")
	}
	if _, _, ok := spliceTables(cur, nil, liveTable{}, false); ok {
		t.Fatal("ok should be false for an empty source run")
	}
	// A source run extending past the end of cur is also rejected.
	if _, _, ok := spliceTables(cur, []liveTable{{r: r1}, {r: r2}}, liveTable{}, false); ok {
		t.Fatal("ok should be false when the source run overflows cur")
	}
}

func TestOpenTablesRemovesTmpFile(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	putFlush(t, s, "a", "1", 1)

	tmp := filepath.Join(tablesDir, tableName(99)+".tmp")
	if err := os.WriteFile(tmp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2 := New()
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	t.Cleanup(func() { _ = s2.CloseTables() })
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf(".sst.tmp not removed (stat err=%v)", err)
	}
}

func TestFsyncDirError(t *testing.T) {
	if err := fsyncDir(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Fatal("fsyncDir should error on a missing path")
	}
}

// TestFrozenMemtableReadPaths exercises the frozen-memtable branches of the
// read/evict path. A flush whose manifest write fails leaves the frozen
// memtable in place, so subsequent lookups must consult it.
func TestFrozenMemtableReadPaths(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	blockManifestWrites(t, tablesDir)

	if err := s.Put("k", "v1", vclock("c", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err == nil {
		t.Fatal("Flush should fail (manifest blocked), leaving a frozen memtable")
	}

	// k now lives only in the frozen memtable.
	if e, ok := mustGet(t, s, "k"); !ok || e.Siblings[0].Value != "v1" {
		t.Fatalf("Get from frozen = (%v, %v)", e, ok)
	}
	if _, ok := mustGet(t, s, "absent"); ok {
		t.Fatal("absent key should not be found in frozen memtable")
	}

	// A dominating write folds frozen state up into the active memtable
	// (lookupLocked consults the frozen generation).
	if err := s.Put("k", "v2", vclock("c", 2)); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if e, ok := mustGet(t, s, "k"); !ok || e.Siblings[0].Value != "v2" {
		t.Fatalf("Get after fold = (%v, %v)", e, ok)
	}

	// Evict must leave a marker because k is still shadowed in the frozen
	// memtable (shadowedBelowLocked consults it).
	if err := s.Evict("k"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if _, ok := mustGet(t, s, "k"); ok {
		t.Fatal("evicted key should read as absent")
	}
}
