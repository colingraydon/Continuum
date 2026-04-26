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

const syncInterval = 30 * time.Second

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
}

func New(r *ring.Ring, s *store.Store, selfID string, replicationFactor int, timeout time.Duration) *Manager {
	m := &Manager{
		r:                 r,
		s:                 s,
		selfID:            selfID,
		replicationFactor: replicationFactor,
		client:            &http.Client{Timeout: timeout},
	}
	m.rebuild()
	return m
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

	for key, hash := range m.s.KeyHashes() {
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
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.syncRound()
		case <-ctx.Done():
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

// syncWithReplica compares the primary's Merkle tree for vnodeHash against the
// replica at addr. For each divergent bucket, it fetches the replica's entries
// for the primary's bucket keys and applies any that are newer.
func (m *Manager) syncWithReplica(addr string, vnodeHash uint32, local *merkle.Tree) error {
	state, err := m.fetchSyncState(addr, vnodeHash)
	if err != nil {
		return err
	}
	if state.Root == local.RootHash() {
		return nil
	}

	for i, replicaBucketHash := range state.Buckets {
		if replicaBucketHash == local.BucketHash(i) {
			continue
		}
		keys := local.BucketKeys(i)
		if len(keys) == 0 {
			continue
		}
		entries, err := m.fetchSyncKeys(addr, keys)
		if err != nil {
			return fmt.Errorf("bucket %d: %w", i, err)
		}
		for key, sibs := range entries {
			for _, sib := range sibs {
				v := store.VectorClockVersion{Clocks: sib.Clocks}
				if sib.Deleted {
					m.s.Delete(key, v)
				} else {
					m.s.Put(key, sib.Value, v)
				}
			}
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
