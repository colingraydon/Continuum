package hintstore

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
)

func reopen(t *testing.T, dir string, maxPerNode int, ttl time.Duration) *HintStore {
	t.Helper()
	hs, err := NewPersistent(dir, maxPerNode, ttl)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	return hs
}

func TestPersistent_ReplaysStoredHints(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	hs := reopen(t, dir, 100, time.Hour)
	hs.Store("nodeA", makeHint("k1"))
	hs.Store("nodeA", makeHint("k2"))
	hs.Store("nodeB", makeHint("k3"))
	if err := hs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	hs2 := reopen(t, dir, 100, time.Hour)
	defer hs2.Close()
	a := hs2.Drain("nodeA")
	if len(a) != 2 {
		t.Fatalf("nodeA: want 2 hints after reopen, got %d", len(a))
	}
	if b := hs2.Drain("nodeB"); len(b) != 1 {
		t.Fatalf("nodeB: want 1 hint after reopen, got %d", len(b))
	}
}

func TestPersistent_DrainSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	hs := reopen(t, dir, 100, time.Hour)
	hs.Store("nodeA", makeHint("k1"))
	hs.Store("nodeA", makeHint("k2"))
	if got := hs.Drain("nodeA"); len(got) != 2 {
		t.Fatalf("drain: want 2, got %d", len(got))
	}
	if err := hs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Drained hints must not resurrect after a restart.
	hs2 := reopen(t, dir, 100, time.Hour)
	defer hs2.Close()
	if got := hs2.Drain("nodeA"); got != nil {
		t.Fatalf("drained hints resurrected after restart: %v", got)
	}
	if nodes := hs2.PendingNodes(); len(nodes) != 0 {
		t.Fatalf("want no pending nodes, got %v", nodes)
	}
}

func TestPersistent_CapEvictionSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	const cap = 3
	hs := reopen(t, dir, cap, time.Hour)
	for i := 0; i < cap+2; i++ {
		hs.Store("nodeA", Hint{Key: string(rune('a' + i)), At: time.Now()})
	}
	if err := hs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	hs2 := reopen(t, dir, cap, time.Hour)
	defer hs2.Close()
	got := hs2.Drain("nodeA")
	if len(got) != cap {
		t.Fatalf("want %d hints after eviction + restart, got %d", cap, len(got))
	}
	// The two oldest ('a','b') were evicted; the survivors are c,d,e in order.
	want := []string{"c", "d", "e"}
	for i, h := range got {
		if h.Key != want[i] {
			t.Fatalf("survivor %d: want %q, got %q", i, want[i], h.Key)
		}
	}
}

func TestPersistent_TTLPrunedOnReplay(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	hs := reopen(t, dir, 100, time.Hour)
	hs.Store("nodeA", Hint{Key: "old", At: time.Now().Add(-2 * time.Hour)})
	hs.Store("nodeA", makeHint("fresh"))
	if err := hs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	hs2 := reopen(t, dir, 100, time.Hour)
	defer hs2.Close()
	got := hs2.Drain("nodeA")
	if len(got) != 1 || got[0].Key != "fresh" {
		t.Fatalf("want only the fresh hint after TTL prune, got %v", got)
	}
}

func TestPersistent_RoundTripPreservesFields(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	hs := reopen(t, dir, 100, time.Hour)
	want := Hint{
		Key:     "user:42",
		Value:   "alice",
		Clocks:  map[string]uint64{"n1": 3, "n2": 7},
		Deleted: true,
		At:      time.Unix(0, time.Now().UnixNano()), // ns precision survives
	}
	hs.Store("nodeA", want)
	if err := hs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	hs2 := reopen(t, dir, 100, time.Hour)
	defer hs2.Close()
	got := hs2.Drain("nodeA")
	if len(got) != 1 {
		t.Fatalf("want 1 hint, got %d", len(got))
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got[0], want)
	}
}

func TestPersistent_CompactionBoundsLogAndPreservesHints(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	hs := reopen(t, dir, 100, time.Hour)

	// Churn well past the compaction threshold: store then drain repeatedly so
	// nearly every record is superseded.
	for i := 0; i < compactThreshold*2; i++ {
		hs.Store("nodeA", makeHint("k"))
		hs.Drain("nodeA")
	}
	// Leave a couple of live hints behind.
	hs.Store("nodeA", makeHint("live1"))
	hs.Store("nodeB", makeHint("live2"))
	hs.ExpireOld() // triggers maybeCompact
	if err := hs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	size, err := dirSize(dir)
	if err != nil {
		t.Fatalf("dirSize: %v", err)
	}
	// After compaction the log should be tiny (a couple of records), nowhere
	// near the ~4000 records that were appended.
	if size > 64*1024 {
		t.Fatalf("compacted log unexpectedly large: %d bytes", size)
	}

	hs2 := reopen(t, dir, 100, time.Hour)
	defer hs2.Close()
	if got := hs2.Drain("nodeA"); len(got) != 1 || got[0].Key != "live1" {
		t.Fatalf("nodeA live hint lost after compaction: %v", got)
	}
	if got := hs2.Drain("nodeB"); len(got) != 1 || got[0].Key != "live2" {
		t.Fatalf("nodeB live hint lost after compaction: %v", got)
	}
}

func TestPersistent_RemoveSeqsFilters(t *testing.T) {
	hints := []storedHint{
		{Hint: Hint{Key: "a"}, seq: 1},
		{Hint: Hint{Key: "b"}, seq: 2},
		{Hint: Hint{Key: "c"}, seq: 3},
	}
	got := removeSeqs(hints, []uint64{2})
	if len(got) != 2 || got[0].Key != "a" || got[1].Key != "c" {
		t.Fatalf("removeSeqs dropped wrong entries: %v", got)
	}
}

func dirSize(dir string) (int64, error) {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// TestPersistent_CrashReplaysRemoveRecords simulates a kill -9: the store is
// abandoned without Close, so the on-disk log keeps its STORE and REMOVE
// records instead of being compacted to STORE-only. Replay must apply the
// removals. This is the crash-recovery path that Close() normally compacts away
// — and the only path that exercises REMOVE-record decoding end to end.
func TestPersistent_CrashReplaysRemoveRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	hs := reopen(t, dir, 100, time.Hour)
	hs.Store("nodeA", makeHint("a1"))
	hs.Store("nodeA", makeHint("a2"))
	hs.Store("nodeB", makeHint("b1"))
	if got := hs.Drain("nodeA"); len(got) != 2 { // appends a REMOVE record
		t.Fatalf("drain nodeA: want 2, got %d", len(got))
	}
	// Intentionally do NOT Close hs — emulate a crash so the REMOVE record
	// stays on disk for the next open to replay.

	hs2 := reopen(t, dir, 100, time.Hour)
	defer hs2.Close()
	if got := hs2.Drain("nodeA"); got != nil {
		t.Fatalf("REMOVE record not replayed: nodeA resurrected: %v", got)
	}
	if got := hs2.Drain("nodeB"); len(got) != 1 {
		t.Fatalf("nodeB should survive crash replay, got %d", len(got))
	}
}

// writeRawHintLog writes raw payloads as WAL records under dir, bypassing the
// hint codec so tests can plant malformed records.
func writeRawHintLog(t *testing.T, dir string, payloads ...[]byte) {
	t.Helper()
	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	for _, p := range payloads {
		if _, err := w.Append(p); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPersistent_ReplayRejectsMalformedRecords(t *testing.T) {
	cases := map[string][]byte{
		"unknown op":       {0xFF},
		"empty record":     {},
		"truncated store":  {opStore, 0x00, 0x05, 'n'},                          // promises 5-byte node, supplies 1
		"truncated remove": {opRemove, 0x00, 0x01, 'n', 0x00, 0x00, 0x00, 0x01}, // promises 1 seq, supplies none
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "hints")
			writeRawHintLog(t, dir, payload)
			if _, err := NewPersistent(dir, 100, time.Hour); err == nil {
				t.Fatalf("expected replay error for %q record", name)
			}
		})
	}
}

// TestPersistent_DecodeStoreTruncated truncates a valid STORE record at many
// offsets, exercising the decode error path for each field in turn.
func TestPersistent_DecodeStoreTruncated(t *testing.T) {
	full := encodeStore("nA", storedHint{
		Hint: Hint{Key: "k1", Value: "v", Clocks: map[string]uint64{"x": 1}},
		seq:  9,
	})
	for n := 1; n < len(full); n++ {
		if _, _, err := decodeStore(full[1:n]); err == nil {
			t.Fatalf("decodeStore truncated to %d bytes: expected error", n)
		}
	}
	// The full record decodes cleanly.
	node, sh, err := decodeStore(full[1:])
	if err != nil {
		t.Fatalf("decodeStore full: %v", err)
	}
	if node != "nA" || sh.Key != "k1" || sh.seq != 9 {
		t.Fatalf("decodeStore round-trip mismatch: %s %+v", node, sh)
	}
}

func TestPersistent_CloseMemoryOnlyAndNil(t *testing.T) {
	if err := New(10, time.Hour).Close(); err != nil {
		t.Fatalf("memory-only Close should be a no-op: %v", err)
	}
	var nilHS *HintStore
	if err := nilHS.Close(); err != nil {
		t.Fatalf("nil-receiver Close should be a no-op: %v", err)
	}
}

// TestPersistent_ExpireOldPersistsRemoval covers the persistent branch of
// ExpireOld: an aged-out hint appends a REMOVE record that survives a crash.
func TestPersistent_ExpireOldPersistsRemoval(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	hs := reopen(t, dir, 100, time.Hour)
	hs.Store("nodeA", Hint{Key: "old", At: time.Now().Add(-2 * time.Hour)})
	hs.Store("nodeA", makeHint("fresh"))
	hs.ExpireOld() // aged hint expires -> REMOVE appended + synced
	// Abandon without Close so the REMOVE record replays rather than compacting.

	hs2 := reopen(t, dir, 100, time.Hour)
	defer hs2.Close()
	got := hs2.Drain("nodeA")
	if len(got) != 1 || got[0].Key != "fresh" {
		t.Fatalf("want only fresh after ExpireOld persisted removal, got %v", got)
	}
}
