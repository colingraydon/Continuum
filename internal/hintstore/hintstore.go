package hintstore

import (
	"sync"
	"time"
)

// Hint is a buffered write for a temporarily unreachable replica. It carries
// everything needed to replay the write as a replica sub-write once the target
// node recovers.
type Hint struct {
	Key     string
	Value   string
	Clocks  map[string]uint64
	Deleted bool
	At      time.Time
}

// HintStore buffers writes and deletes for unreachable replica nodes. When a
// node recovers it drains its hints and replays them. Anti-entropy is the
// safety net for hints that expire or are lost on coordinator restart.
type HintStore struct {
	mu         sync.Mutex
	hints      map[string][]Hint
	maxPerNode int
	ttl        time.Duration
}

func New(maxPerNode int, ttl time.Duration) *HintStore {
	return &HintStore{
		hints:      make(map[string][]Hint),
		maxPerNode: maxPerNode,
		ttl:        ttl,
	}
}

// Store buffers a hint for nodeID. If the per-node cap is reached the oldest
// hint is evicted; anti-entropy will repair any keys whose hints were dropped.
func (hs *HintStore) Store(nodeID string, h Hint) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hints := hs.hints[nodeID]
	if len(hints) >= hs.maxPerNode {
		hints = hints[1:] // drop oldest
	}
	hs.hints[nodeID] = append(hints, h)
}

// Drain removes and returns all buffered hints for nodeID. Returns nil if
// there are no buffered hints.
func (hs *HintStore) Drain(nodeID string) []Hint {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hints := hs.hints[nodeID]
	if len(hints) == 0 {
		return nil
	}
	delete(hs.hints, nodeID)
	return hints
}

// PendingNodes returns the node IDs that have buffered hints.
func (hs *HintStore) PendingNodes() []string {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	nodes := make([]string, 0, len(hs.hints))
	for id, hints := range hs.hints {
		if len(hints) > 0 {
			nodes = append(nodes, id)
		}
	}
	return nodes
}

// ExpireOld removes hints older than the store's TTL. Call periodically to
// bound memory; anti-entropy handles any keys whose hints were evicted.
func (hs *HintStore) ExpireOld() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	cutoff := time.Now().Add(-hs.ttl)
	for nodeID, hints := range hs.hints {
		var fresh []Hint
		for _, h := range hints {
			if h.At.After(cutoff) {
				fresh = append(fresh, h)
			}
		}
		if len(fresh) == 0 {
			delete(hs.hints, nodeID)
		} else {
			hs.hints[nodeID] = fresh
		}
	}
}
