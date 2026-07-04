package antientropy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

const (
	syncInterval = 30 * time.Second
	gcInterval   = 5 * time.Minute
	// GCTTL is the minimum age a tombstone must reach before it is eligible for
	// garbage collection AND the maximum downtime a single node can tolerate
	// while persisting state. The recovery driver in cmd/continuum uses the
	// same constant as the downtime gate: a node whose last clean shutdown was
	// longer than GCTTL ago discards local data and re-bootstraps, ensuring
	// it can never resurrect a tombstone that other replicas have already
	// purged. See docs/persistence.md for the full safety argument.
	GCTTL = 24 * time.Hour
)

// Manager maintains one Merkle tree per vnode this node replicates and drives
// anti-entropy syncs from primary to replicas (Dynamo-style: primary initiates,
// replicas are passive). It keeps trees for the full replica set — not just the
// primary ranges it drives sync for — so it can serve sync state (GET /sync,
// /sync/bucket-keys) straight from a tree instead of rescanning the whole store
// on every request. The round-robin initiation order stays the primary subset:
// a node only drives sync for vnodes it is primary for.
type Manager struct {
	mu                sync.RWMutex
	trees             map[uint32]*merkle.Tree    // replicated vnode end hash → tree
	ranges            map[uint32]ring.VnodeRange // replicated vnode end hash → range
	order             []uint32                   // sorted primary vnode end hashes; deterministic sync order
	cursor            int                        // next index into order
	r                 *ring.Ring
	s                 *store.Store
	selfID            string
	replicationFactor int
	client            *http.Client
	syncEvery         time.Duration
}

func New(r *ring.Ring, s *store.Store, selfID string, replicationFactor int, timeout time.Duration) *Manager {
	return NewWithSnapshot(r, s, selfID, replicationFactor, timeout, "")
}

// newBare constructs a Manager without building any trees; callers must
// follow with RestoreSnapshot or rebuild.
func newBare(r *ring.Ring, s *store.Store, selfID string, replicationFactor int, timeout time.Duration) *Manager {
	return &Manager{
		r:                 r,
		s:                 s,
		selfID:            selfID,
		replicationFactor: replicationFactor,
		client:            &http.Client{Timeout: timeout},
		syncEvery:         syncInterval,
	}
}

// SetSyncInterval overrides how often the primary-driven sync round runs.
// Non-positive values are ignored. Call before Start.
func (m *Manager) SetSyncInterval(d time.Duration) {
	if d > 0 {
		m.syncEvery = d
	}
}

// rebuild replaces the trees and ranges for the given replicated ranges, sets
// the sync order to the primary subset, and repopulates the trees from the
// current store state (one full scan). Called at startup and whenever a sync
// round observes that membership changed the replicated range set.
func (m *Manager) rebuild(replicaRanges []ring.VnodeRange, primaryEnds []uint32) {
	m.mu.Lock()
	m.trees = make(map[uint32]*merkle.Tree, len(replicaRanges))
	m.ranges = make(map[uint32]ring.VnodeRange, len(replicaRanges))
	for _, vr := range replicaRanges {
		m.trees[vr.End] = merkle.New()
		m.ranges[vr.End] = vr
	}
	m.order = make([]uint32, len(primaryEnds))
	copy(m.order, primaryEnds)
	sort.Slice(m.order, func(i, j int) bool { return m.order[i] < m.order[j] })
	m.cursor = 0
	m.mu.Unlock()

	hashes, err := m.s.KeyHashes()
	if err != nil {
		log.Printf("antientropy: rebuild scan failed: %v", err)
		return
	}
	for key, hash := range hashes {
		m.Update(key, hash)
	}
}

// maybeRebuild recomputes the replicated ranges from the current ring and
// rebuilds the trees when membership has changed them. Membership events are
// rare, so the full store scan a rebuild costs is acceptable; every other
// round this is a cheap comparison. The primary subset that drives sync
// initiation can only change when the replicated set does (a node is primary
// of a vnode exactly when it owns that vnode, which also puts the vnode in its
// replica set), so an unchanged range set leaves the order — and its cursor —
// intact.
func (m *Manager) maybeRebuild() {
	replicaRanges := m.r.GetReplicaVnodeRanges(m.selfID, m.replicationFactor)
	m.mu.RLock()
	same := rangesEqual(m.ranges, replicaRanges)
	m.mu.RUnlock()
	if same {
		return
	}
	log.Printf("antientropy: replicated ranges changed (%d vnodes); rebuilding trees", len(replicaRanges))
	m.rebuild(replicaRanges, vnodeEnds(m.r.GetPrimaryVnodeRanges(m.selfID)))
}

// vnodeEnds extracts the End hash of each range.
func vnodeEnds(ranges []ring.VnodeRange) []uint32 {
	ends := make([]uint32, len(ranges))
	for i, vr := range ranges {
		ends[i] = vr.End
	}
	return ends
}

// SyncState returns the root and per-bucket hashes of the maintained tree for
// vnodeHash, or ok=false when this node keeps no tree for it (it is not a
// replica of that vnode, or a membership change has not been observed yet). The
// root is derived from the returned bucket hashes so the two are always mutually
// consistent and identical to the on-the-fly scan path.
func (m *Manager) SyncState(vnodeHash uint32) (root uint32, buckets []uint32, ok bool) {
	m.mu.RLock()
	tree, ok := m.trees[vnodeHash]
	m.mu.RUnlock()
	if !ok {
		return 0, nil, false
	}
	buckets = make([]uint32, merkle.BucketCount)
	for i := range buckets {
		buckets[i] = tree.BucketHash(i)
	}
	return merkle.ComputeRootHash(buckets), buckets, true
}

// BucketKeys returns the sorted keys in bucket of the maintained tree for
// vnodeHash, or ok=false when this node keeps no tree for it.
func (m *Manager) BucketKeys(vnodeHash uint32, bucket int) (keys []string, ok bool) {
	m.mu.RLock()
	tree, ok := m.trees[vnodeHash]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return tree.BucketKeys(bucket), true
}

// rangesEqual reports whether the current range map matches the freshly
// computed primary ranges exactly (same vnodes, same bounds).
func rangesEqual(cur map[uint32]ring.VnodeRange, next []ring.VnodeRange) bool {
	if len(cur) != len(next) {
		return false
	}
	for _, vr := range next {
		if got, ok := cur[vr.End]; !ok || got != vr {
			return false
		}
	}
	return true
}

// Update routes a store write to the correct primary vnode's Merkle tree.
// Keys not belonging to any primary range are ignored (replicas don't maintain
// trees; they serve sync state on-the-fly from the store).
func (m *Manager) Update(key string, hash uint32) {
	keyHash := merkle.HashKey(key)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for end, vr := range m.ranges {
		if vr.Contains(keyHash) {
			m.trees[end].Update(key, hash)
			return
		}
	}
}

// Start launches the background sync loop. Cancelled when ctx is done.
func (m *Manager) Start(ctx context.Context) {
	go m.syncLoop(ctx)
}

func (m *Manager) syncLoop(ctx context.Context) {
	syncTicker := time.NewTicker(m.syncEvery)
	gcTicker := time.NewTicker(gcInterval)
	defer syncTicker.Stop()
	defer gcTicker.Stop()
	for {
		select {
		case <-syncTicker.C:
			m.syncRound()
		case <-gcTicker.C:
			m.runGC()
		case <-ctx.Done():
			return
		}
	}
}

// runGC removes tombstones older than GCTTL and evicts them from the primary's
// Merkle trees so future syncs reflect the purged state.
func (m *Manager) runGC() {
	purged, err := m.s.GCTombstones(GCTTL)
	if err != nil {
		log.Printf("antientropy: GC failed: %v", err)
		return
	}
	for _, key := range purged {
		m.RemoveFromTrees(key)
	}
	if len(purged) > 0 {
		log.Printf("antientropy: GC purged %d tombstones", len(purged))
	}
}

// RemoveFromTrees removes key from whichever primary vnode tree owns it.
// Called by the store's onEvict callback when a key is evicted during cleanup.
func (m *Manager) RemoveFromTrees(key string) {
	keyHash := merkle.HashKey(key)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for end, vr := range m.ranges {
		if vr.Contains(keyHash) {
			m.trees[end].Remove(key)
			return
		}
	}
}

// syncRound refreshes the primary ranges if membership changed, then syncs
// the next vnode in the deterministic round-robin order. Cycling (rather
// than sampling randomly) bounds full-keyspace repair at exactly
// len(order) rounds; random selection had an unbounded worst case and a
// coupon-collector expected time.
func (m *Manager) syncRound() {
	m.maybeRebuild()

	end, ok := m.nextVnode()
	if !ok {
		return
	}

	m.mu.RLock()
	tree := m.trees[end]
	m.mu.RUnlock()

	nodes := m.r.GetReplicationNodesForHash(end, m.replicationFactor)
	for _, node := range nodes {
		if node.ID == m.selfID {
			continue
		}
		if err := m.syncWithReplica(node.Address, end, tree); err != nil {
			log.Printf("antientropy: sync with %s vnode %d: %v", node.ID, end, err)
		}
	}
}

// nextVnode returns the next vnode end hash in the round-robin order, or
// false when there are no primary ranges.
func (m *Manager) nextVnode() (uint32, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.order) == 0 {
		return 0, false
	}
	end := m.order[m.cursor%len(m.order)]
	m.cursor = (m.cursor + 1) % len(m.order)
	return end, true
}

// snapshotBucketEntries captures the current siblings for each key in localKeys
// so we push the pre-merge state — there is no point sending back data the
// replica just gave us.
func (m *Manager) snapshotBucketEntries(localKeys []string) map[string][]syncSibling {
	snap := make(map[string][]syncSibling)
	for _, key := range localKeys {
		entry, ok, err := m.s.Get(key)
		if err != nil {
			log.Printf("antientropy: snapshot read %s: %v", key, err)
			continue
		}
		if ok {
			sibs := make([]syncSibling, len(entry.Siblings))
			for j, sib := range entry.Siblings {
				sibs[j] = syncSibling{Value: sib.Value, Deleted: sib.Deleted, Clocks: sib.Version.Clocks}
			}
			snap[key] = sibs
		}
	}
	return snap
}

// applyBucketEntries merges a set of key→siblings from a replica into the
// primary's local store using the standard vector clock path. Per-key write
// failures are logged and skipped so a single bad write doesn't abort the
// whole sync round.
func (m *Manager) applyBucketEntries(entries map[string][]syncSibling) {
	for key, sibs := range entries {
		for _, sib := range sibs {
			v := store.VectorClockVersion{Clocks: sib.Clocks}
			var err error
			if sib.Deleted {
				err = m.s.Delete(key, v)
			} else {
				err = m.s.Put(key, sib.Value, v)
			}
			if err != nil {
				log.Printf("antientropy: apply %s: %v", key, err)
			}
		}
	}
}

// syncBucket reconciles a single divergent bucket between the primary and a
// replica: it snapshots primary entries for the push batch, pulls remote
// entries and applies them locally, and returns any fetch error.
func (m *Manager) syncBucket(addr string, vnodeHash uint32, local *merkle.Tree, bucket int, toPush map[string][]syncSibling) error {
	localKeys := local.BucketKeys(bucket)
	remoteKeys, err := m.fetchBucketKeys(addr, vnodeHash, bucket)
	if err != nil {
		return fmt.Errorf("bucket %d keys: %w", bucket, err)
	}
	allKeys := union(localKeys, remoteKeys)
	if len(allKeys) == 0 {
		return nil
	}

	for k, v := range m.snapshotBucketEntries(localKeys) {
		toPush[k] = v
	}

	entries, err := m.fetchSyncKeys(addr, allKeys)
	if err != nil {
		return fmt.Errorf("bucket %d: %w", bucket, err)
	}
	m.applyBucketEntries(entries)
	return nil
}

// syncWithReplica bidirectionally syncs the primary's vnode against the replica
// at addr. For each divergent bucket it:
//   - pulls entries the replica has that are newer than or absent from the primary
//   - pushes entries the primary has that are newer than or absent from the replica
func (m *Manager) syncWithReplica(addr string, vnodeHash uint32, local *merkle.Tree) error {
	state, err := m.fetchSyncState(addr, vnodeHash)
	if err != nil {
		return err
	}
	if state.Root == local.RootHash() {
		return nil
	}

	// Collect all entries to push at the end so a single HTTP call carries the
	// full batch rather than one call per key.
	toPush := make(map[string][]syncSibling)

	for i, replicaBucketHash := range state.Buckets {
		if replicaBucketHash == local.BucketHash(i) {
			continue
		}
		if err := m.syncBucket(addr, vnodeHash, local, i, toPush); err != nil {
			return err
		}
	}

	// Push: send the primary's entries to the replica in one batch.
	if len(toPush) > 0 {
		if err := m.pushSyncEntries(addr, toPush); err != nil {
			return fmt.Errorf("push: %w", err)
		}
	}
	return nil
}

type syncStateResponse struct {
	Root    uint32   `json:"root"`
	Buckets []uint32 `json:"buckets"`
}

type syncKeysRequest struct {
	Keys []string `json:"keys"`
}

type syncSibling struct {
	Value   string            `json:"value,omitempty"`
	Deleted bool              `json:"deleted,omitempty"`
	Clocks  map[string]uint64 `json:"clocks"`
}

type syncKeysResponse struct {
	Entries map[string][]syncSibling `json:"entries"`
}

func (m *Manager) fetchSyncState(addr string, vnodeHash uint32) (syncStateResponse, error) {
	resp, err := m.client.Get(fmt.Sprintf("http://%s/sync?vnode=%d", addr, vnodeHash))
	if err != nil {
		return syncStateResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var state syncStateResponse
	return state, json.NewDecoder(resp.Body).Decode(&state)
}

func (m *Manager) fetchSyncKeys(addr string, keys []string) (map[string][]syncSibling, error) {
	body, err := json.Marshal(syncKeysRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	resp, err := m.client.Post("http://"+addr+"/sync/keys", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var result syncKeysResponse
	return result.Entries, json.NewDecoder(resp.Body).Decode(&result)
}

// fetchBucketKeys returns the key names in a specific bucket of a vnode range
// from the replica. Used to discover keys the replica has that the primary
// does not, enabling the pull side of bidirectional sync.
func (m *Manager) fetchBucketKeys(addr string, vnodeHash uint32, bucket int) ([]string, error) {
	resp, err := m.client.Get(fmt.Sprintf("http://%s/sync/bucket-keys?vnode=%d&bucket=%d", addr, vnodeHash, bucket))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		Keys []string `json:"keys"`
	}
	return result.Keys, json.NewDecoder(resp.Body).Decode(&result)
}

// pushSyncEntries sends a batch of primary entries to the replica via a single
// HTTP call so the replica can apply any it is missing or behind on.
func (m *Manager) pushSyncEntries(addr string, entries map[string][]syncSibling) error {
	body, err := json.Marshal(syncKeysResponse{Entries: entries})
	if err != nil {
		return err
	}
	resp, err := m.client.Post("http://"+addr+"/sync/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("push: replica returned %d", resp.StatusCode)
	}
	return nil
}

// union returns a deduplicated slice containing every string in a or b.
func union(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		seen[s] = struct{}{}
	}
	for _, s := range b {
		seen[s] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	return out
}
