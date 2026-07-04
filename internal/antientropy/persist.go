package antientropy

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// treeSnapshot is the on-disk form of the manager's Merkle state: every
// replicated range's key→entryHash pairs, the range set and primary sync
// order they were built against, and the WAL sequence the store had applied
// when the snapshot was taken. Trees are keyed by the vnode end hash
// (stringified for JSON map keys); bucket layout is not persisted because it
// is derivable from each key.
type treeSnapshot struct {
	LastSeq uint64                       `json:"last_seq"`
	Ranges  []ring.VnodeRange            `json:"ranges"`
	Order   []uint32                     `json:"order"`
	Trees   map[string]map[string]uint32 `json:"trees"`
}

// NewWithSnapshot is New with startup-scan avoidance: when snapshotPath names
// a snapshot whose WAL sequence matches the store's recovered state exactly,
// the trees are restored from it instead of scanning every table. Any
// mismatch, parse failure, or missing file falls back to the full rebuild —
// the worst case is New's exact behavior. Pass "" to always rebuild.
func NewWithSnapshot(r *ring.Ring, s *store.Store, selfID string, replicationFactor int, timeout time.Duration, snapshotPath string) *Manager {
	m := newBare(r, s, selfID, replicationFactor, timeout)
	if snapshotPath != "" && m.RestoreSnapshot(snapshotPath) {
		log.Printf("antientropy: restored Merkle trees from snapshot (seq %d)", s.LastSeq())
		return m
	}
	m.rebuild(r.GetReplicaVnodeRanges(selfID, replicationFactor), vnodeEnds(r.GetPrimaryVnodeRanges(selfID)))
	return m
}

// SaveSnapshot atomically writes the manager's current trees to path. Call at
// graceful shutdown, after writes have drained, so the store's LastSeq is
// final and the trees are quiescent.
func (m *Manager) SaveSnapshot(path string) error {
	snap := treeSnapshot{
		LastSeq: m.s.LastSeq(),
		Trees:   make(map[string]map[string]uint32),
	}
	m.mu.RLock()
	for end, vr := range m.ranges {
		snap.Ranges = append(snap.Ranges, vr)
		snap.Trees[strconv.FormatUint(uint64(end), 10)] = m.trees[end].Dump()
	}
	snap.Order = append(snap.Order, m.order...)
	m.mu.RUnlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("antientropy: encode snapshot: %w", err)
	}
	return atomicWrite(path, data)
}

// RestoreSnapshot installs the trees, ranges, and sync order from the
// snapshot at path, reporting whether it was usable. A snapshot is rejected
// (returning false, state untouched) when it is missing, unreadable, or was
// taken at a different WAL sequence than the store recovered to — in that
// case the trees would silently miss whatever the WAL tail replayed, because
// replay suppresses the onUpdate callbacks that keep trees current.
func (m *Manager) RestoreSnapshot(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var snap treeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		log.Printf("antientropy: snapshot unreadable, rebuilding: %v", err)
		return false
	}
	if snap.LastSeq != m.s.LastSeq() {
		log.Printf("antientropy: snapshot at seq %d but store recovered to %d; rebuilding", snap.LastSeq, m.s.LastSeq())
		return false
	}

	trees := make(map[uint32]*merkle.Tree, len(snap.Trees))
	ranges := make(map[uint32]ring.VnodeRange, len(snap.Ranges))
	for _, vr := range snap.Ranges {
		entries, ok := snap.Trees[strconv.FormatUint(uint64(vr.End), 10)]
		if !ok {
			log.Printf("antientropy: snapshot missing tree for vnode %d; rebuilding", vr.End)
			return false
		}
		t := merkle.New()
		for key, hash := range entries {
			t.Update(key, hash)
		}
		trees[vr.End] = t
		ranges[vr.End] = vr
	}

	m.mu.Lock()
	m.trees = trees
	m.ranges = ranges
	m.order = append([]uint32(nil), snap.Order...)
	m.cursor = 0
	m.mu.Unlock()
	return true
}

// atomicWrite replaces path with data crash-safely: temp file, fsync, rename,
// directory fsync — the same commit shape the store manifest uses. A crash
// leaves either the old snapshot or the new one, never a partial file.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("antientropy: create snapshot tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("antientropy: write snapshot tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("antientropy: sync snapshot tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("antientropy: close snapshot tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("antientropy: rename snapshot: %w", err)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("antientropy: open snapshot dir: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("antientropy: sync snapshot dir: %w", err)
	}
	return nil
}
