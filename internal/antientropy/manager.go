package antientropy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
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

// Manager maintains one Merkle tree per primary vnode and drives anti-entropy
// syncs from primary to replicas (Dynamo-style: primary initiates, replicas are
// passive). Replicas serve sync state on-the-fly from their local store.
type Manager struct {
	mu                sync.RWMutex
	trees             map[uint32]*merkle.Tree    // vnode end hash → tree
	ranges            map[uint32]ring.VnodeRange // vnode end hash → range
	r                 *ring.Ring
	s                 *store.Store
	selfID            string
	replicationFactor int
	client            *http.Client
	syncEvery         time.Duration
}

func New(r *ring.Ring, s *store.Store, selfID string, replicationFactor int, timeout time.Duration) *Manager {
	m := &Manager{
		r:                 r,
		s:                 s,
		selfID:            selfID,
		replicationFactor: replicationFactor,
		client:            &http.Client{Timeout: timeout},
		syncEvery:         syncInterval,
	}
	m.rebuild()
	return m
}

// SetSyncInterval overrides how often the primary-driven sync round runs.
// Non-positive values are ignored. Call before Start.
func (m *Manager) SetSyncInterval(d time.Duration) {
	if d > 0 {
		m.syncEvery = d
	}
}

// rebuild initializes trees for all primary vnode ranges and populates them
// from the current store state. Called once on startup.
func (m *Manager) rebuild() {
	ranges := m.r.GetPrimaryVnodeRanges(m.selfID)

	m.mu.Lock()
	m.trees = make(map[uint32]*merkle.Tree, len(ranges))
	m.ranges = make(map[uint32]ring.VnodeRange, len(ranges))
	for _, vr := range ranges {
		m.trees[vr.End] = merkle.New()
		m.ranges[vr.End] = vr
	}
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

func (m *Manager) syncRound() {
	m.mu.RLock()
	ends := make([]uint32, 0, len(m.trees))
	for end := range m.trees {
		ends = append(ends, end)
	}
	m.mu.RUnlock()

	if len(ends) == 0 {
		return
	}

	end := ends[rand.Intn(len(ends))]

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
