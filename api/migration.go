package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/store"
)

// Bootstrap pulls all key ranges for which this node is the primary replica
// from the nodes that currently hold them. Called once on join, after the HTTP
// server is up and the ring is populated from the gossip exchange with seeds.
func (h *Handler) Bootstrap() {
	ranges := h.ring.GetPrimaryVnodeRanges(h.selfID)
	log.Printf("migration: bootstrapping %d primary vnode ranges", len(ranges))
	for _, vr := range ranges {
		sources := h.ring.GetReplicationNodesForHash(vr.End, h.replicationFactor)
		for _, src := range sources {
			if src.ID == h.selfID {
				continue
			}
			if err := h.pullVnodeRange(src.Address, vr.End); err != nil {
				log.Printf("migration: pull vnode %d from %s: %v", vr.End, src.ID, err)
			}
		}
	}
	log.Printf("migration: bootstrap complete")
}

// applyEntries merges a map of key→siblings into the local store using the
// standard vector clock path. Used during bootstrap bucket pulls.
func (h *Handler) applyEntries(entries map[string][]SyncSibling) {
	for key, sibs := range entries {
		for _, sib := range sibs {
			v := store.VectorClockVersion{Clocks: sib.Clocks}
			if sib.Deleted {
				h.store.Delete(key, v)
			} else {
				h.store.Put(key, sib.Value, v)
			}
		}
	}
}

// pullVnodeRange fetches all keys in the vnode range ending at vnodeHash from
// srcAddr by iterating all 16 Merkle buckets and merging each entry into the
// local store via the standard vector clock path.
func (h *Handler) pullVnodeRange(srcAddr string, vnodeHash uint32) error {
	for b := 0; b < merkle.BucketCount; b++ {
		keys, err := h.migFetchBucketKeys(srcAddr, vnodeHash, b)
		if err != nil {
			return fmt.Errorf("bucket %d keys: %w", b, err)
		}
		if len(keys) == 0 {
			continue
		}
		entries, err := h.migFetchSyncKeys(srcAddr, keys)
		if err != nil {
			return fmt.Errorf("bucket %d entries: %w", b, err)
		}
		h.applyEntries(entries)
	}
	return nil
}

// collectKeyForPush adds the given key's siblings to toPush, keyed by the
// address of each alive non-self replica that owns the key.
func (h *Handler) collectKeyForPush(key string, toPush map[string]map[string][]SyncSibling) {
	entry, ok := h.store.Get(key)
	if !ok {
		return
	}
	sibs := make([]SyncSibling, len(entry.Siblings))
	for i, sib := range entry.Siblings {
		sibs[i] = SyncSibling{Value: sib.Value, Deleted: sib.Deleted, Clocks: sib.Version.Clocks}
	}
	nodes := h.ring.GetReplicationNodes(key, h.replicationFactor)
	for _, n := range nodes {
		if n.ID == h.selfID {
			continue
		}
		m, ok := h.memberList.Get(n.ID)
		if !ok || m.Status != gossip.MemberAlive {
			continue
		}
		if toPush[m.Address] == nil {
			toPush[m.Address] = make(map[string][]SyncSibling)
		}
		toPush[m.Address][key] = sibs
	}
}

// PushKeysToSuccessors sends all locally-held keys to the alive replica nodes
// in a single batched push per target. Called during graceful shutdown before
// HTTP drain so data survives a planned node departure.
func (h *Handler) PushKeysToSuccessors() {
	toPush := make(map[string]map[string][]SyncSibling) // addr → key → siblings
	for key := range h.store.KeyHashes() {
		h.collectKeyForPush(key, toPush)
	}

	log.Printf("migration: pushing keys to %d successor nodes", len(toPush))
	for addr, entries := range toPush {
		if err := h.migPushSyncEntries(addr, entries); err != nil {
			log.Printf("migration: push to %s failed: %v", addr, err)
		}
	}
}

// CleanupStaleKeys evicts any key from the local store that this node is no
// longer responsible for according to the current ring state. Called after
// Bootstrap completes (self cleanup) and when a peer exits bootstrapping
// (ring has changed, some keys may have moved to the new node).
func (h *Handler) CleanupStaleKeys() {
	evicted := 0
	for key := range h.store.KeyHashes() {
		nodes := h.ring.GetReplicationNodes(key, h.replicationFactor)
		owned := false
		for _, n := range nodes {
			if n.ID == h.selfID {
				owned = true
				break
			}
		}
		if !owned {
			h.store.Evict(key)
			evicted++
		}
	}
	if evicted > 0 {
		log.Printf("migration: evicted %d stale keys after ring change", evicted)
	}
}

func (h *Handler) migFetchBucketKeys(addr string, vnodeHash uint32, bucket int) ([]string, error) {
	url := fmt.Sprintf("http://%s/sync/bucket-keys?vnode=%d&bucket=%d", addr, vnodeHash, bucket)
	resp, err := h.replicaClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var result SyncBucketKeysResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Keys, nil
}

func (h *Handler) migFetchSyncKeys(addr string, keys []string) (map[string][]SyncSibling, error) {
	body, err := json.Marshal(SyncKeysRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	resp, err := h.replicaClient.Post("http://"+addr+"/sync/keys", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var result SyncKeysResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Entries, nil
}

func (h *Handler) migPushSyncEntries(addr string, entries map[string][]SyncSibling) error {
	body, err := json.Marshal(SyncKeysResponse{Entries: entries})
	if err != nil {
		return err
	}
	resp, err := h.replicaClient.Post("http://"+addr+"/sync/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("sync push returned %d", resp.StatusCode)
	}
	return nil
}
