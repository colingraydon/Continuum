package antientropy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
	"github.com/colingraydon/continuum/internal/wal"
)

// newSnapshotFixture returns a manager over a 2-node ring with n keys, plus
// its ring and store — the state a SaveSnapshot would capture at shutdown.
func newSnapshotFixture(t *testing.T, n int) (*Manager, *ring.Ring, *store.Store) {
	t.Helper()
	r := ring.NewRing(8)
	r.AddNode("self", "127.0.0.1:1")
	r.AddNode("peer", "127.0.0.1:2")
	s := store.New()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("sn-k%03d", i)
		if err := s.Put(key, "v", store.VectorClockVersion{Clocks: map[string]uint64{"w": uint64(i + 1)}}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	m := New(r, s, "self", 2, time.Second)
	return m, r, s
}

func snapshotPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "merkle.json")
}

// assertSameSyncState compares two managers' served state for every vnode in
// ranges — the externally observable definition of "the same trees".
func assertSameSyncState(t *testing.T, want, got *Manager, ranges []ring.VnodeRange) {
	t.Helper()
	for _, vr := range ranges {
		wr, wb, wok := want.SyncState(vr.End)
		gr, gb, gok := got.SyncState(vr.End)
		if wok != gok {
			t.Fatalf("vnode %d: tree presence differs (want %v, got %v)", vr.End, wok, gok)
		}
		if !wok {
			continue
		}
		if wr != gr || !slices.Equal(wb, gb) {
			t.Errorf("vnode %d: restored sync state differs", vr.End)
		}
	}
}

// TestSnapshotRoundTrip: save, restore into a fresh manager, identical served
// state for every replicated vnode, plus order/ranges installed.
func TestSnapshotRoundTrip(t *testing.T) {
	m, r, s := newSnapshotFixture(t, 60)
	path := snapshotPath(t)
	if err := m.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file left behind after atomic write")
	}

	restored := NewWithSnapshot(r, s, "self", 2, time.Second, path)
	assertSameSyncState(t, m, restored, r.GetReplicaVnodeRanges("self", 2))
	if !slices.Equal(m.order, restored.order) {
		t.Errorf("sync order not restored: %v vs %v", m.order, restored.order)
	}
}

// TestSnapshotRestoreDoesNotScan is the point of the feature: the restored
// manager's trees come from the file, not from a store scan. A key written
// after the save (memory-only store, so LastSeq stays 0 and the seq check
// still passes) must be absent from the restored trees, and present after a
// fallback rebuild.
func TestSnapshotRestoreDoesNotScan(t *testing.T) {
	m, r, s := newSnapshotFixture(t, 20)
	path := snapshotPath(t)
	if err := m.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Mutate the store after the snapshot; a scan would see this key.
	if err := s.Put("sn-post-snapshot", "v", store.VectorClockVersion{Clocks: map[string]uint64{"w": 999}}); err != nil {
		t.Fatalf("post-snapshot put: %v", err)
	}

	restored := NewWithSnapshot(r, s, "self", 2, time.Second, path)
	if managerHoldsKey(restored, "sn-post-snapshot") {
		t.Error("restored trees contain a post-snapshot key: restore scanned the store")
	}

	rebuilt := New(r, s, "self", 2, time.Second)
	if !managerHoldsKey(rebuilt, "sn-post-snapshot") {
		t.Error("fallback rebuild missed a stored key")
	}
}

func managerHoldsKey(m *Manager, key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, tree := range m.trees {
		if _, ok := tree.Dump()[key]; ok {
			return true
		}
	}
	return false
}

// TestSnapshotRejectedOnSeqMismatch: a WAL-backed store that advanced past
// the snapshot must reject it — WAL replay does not fire tree callbacks, so
// restoring would silently miss the tail.
func TestSnapshotRejectedOnSeqMismatch(t *testing.T) {
	r := ring.NewRing(8)
	r.AddNode("self", "127.0.0.1:1")
	s := store.New()
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatalf("wal open: %v", err)
	}
	defer func() { _ = w.Close() }()
	s.SetWAL(w)

	if err := s.Put("sq-k1", "v", store.VectorClockVersion{Clocks: map[string]uint64{"w": 1}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	m := New(r, s, "self", 2, time.Second)
	path := snapshotPath(t)
	if err := m.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Advance the store past the snapshot (bumps LastSeq via the WAL).
	if err := s.Put("sq-k2", "v", store.VectorClockVersion{Clocks: map[string]uint64{"w": 2}}); err != nil {
		t.Fatalf("put: %v", err)
	}

	stale := newBare(r, s, "self", 2, time.Second)
	if stale.RestoreSnapshot(path) {
		t.Fatal("stale snapshot (seq behind store) was accepted")
	}
	// The public constructor must fall back to a rebuild that sees everything.
	rebuilt := NewWithSnapshot(r, s, "self", 2, time.Second, path)
	if !managerHoldsKey(rebuilt, "sq-k2") {
		t.Error("fallback rebuild after seq mismatch missed the tail key")
	}
}

func TestSnapshotMissingOrCorrupt(t *testing.T) {
	m, r, s := newSnapshotFixture(t, 10)
	_ = m

	if newBare(r, s, "self", 2, time.Second).RestoreSnapshot(filepath.Join(t.TempDir(), "absent.json")) {
		t.Error("missing snapshot accepted")
	}

	corrupt := snapshotPath(t)
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	if newBare(r, s, "self", 2, time.Second).RestoreSnapshot(corrupt) {
		t.Error("corrupt snapshot accepted")
	}
	// Constructor falls back cleanly.
	rebuilt := NewWithSnapshot(r, s, "self", 2, time.Second, corrupt)
	if !managerHoldsKey(rebuilt, "sn-k000") {
		t.Error("fallback rebuild after corrupt snapshot missed stored keys")
	}
}

// TestSnapshotThenMembershipChange: a restored manager still adapts — an
// unchanged ring is a no-op, a changed ring triggers the usual full rebuild.
func TestSnapshotThenMembershipChange(t *testing.T) {
	m, r, s := newSnapshotFixture(t, 20)
	path := snapshotPath(t)
	if err := m.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	restored := NewWithSnapshot(r, s, "self", 2, time.Second, path)

	// Unchanged ring: maybeRebuild must keep the restored trees.
	before := restored.trees
	restored.maybeRebuild()
	restored.mu.RLock()
	same := len(before) == len(restored.trees)
	for end, tr := range restored.trees {
		same = same && before[end] == tr
	}
	restored.mu.RUnlock()
	if !same {
		t.Error("maybeRebuild rebuilt despite unchanged membership")
	}

	// Changed ring: must rebuild to the new range set.
	r.AddNode("late-joiner", "127.0.0.1:3")
	restored.maybeRebuild()
	fresh := r.GetReplicaVnodeRanges("self", 2)
	restored.mu.RLock()
	matches := rangesEqual(restored.ranges, fresh)
	restored.mu.RUnlock()
	if !matches {
		t.Error("maybeRebuild did not adapt restored trees to membership change")
	}
}

func TestSaveSnapshotWriteError(t *testing.T) {
	m, _, _ := newSnapshotFixture(t, 5)
	// Parent directory does not exist: the atomic write must fail cleanly.
	if err := m.SaveSnapshot(filepath.Join(t.TempDir(), "no-such-dir", "merkle.json")); err == nil {
		t.Fatal("expected error saving into a missing directory")
	}
}

// TestSnapshotMissingTreeForRange: a snapshot whose ranges reference a tree
// that is not in the file is internally inconsistent and must be rejected.
func TestSnapshotMissingTreeForRange(t *testing.T) {
	m, r, s := newSnapshotFixture(t, 5)
	path := snapshotPath(t)
	if err := m.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Strip the trees map but keep the ranges.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var snap treeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	snap.Trees = map[string]map[string]uint32{}
	mangled, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, mangled, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if newBare(r, s, "self", 2, time.Second).RestoreSnapshot(path) {
		t.Fatal("snapshot with ranges but no trees was accepted")
	}
}
