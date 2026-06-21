package hintstore

import (
	"log"
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

// storedHint is a Hint plus the monotonic id the store assigns it. The id lets
// the persistent log express removals (drain, eviction, expiry) precisely as
// append-only records instead of rewriting the whole log every time.
type storedHint struct {
	Hint
	seq uint64
}

// HintStore buffers writes and deletes for unreachable replica nodes. When a
// node recovers it drains its hints and replays them. Anti-entropy is the
// safety net for hints that expire or are lost on coordinator restart.
//
// When opened with NewPersistent, every mutation is also written to an
// append-only log (see persist.go) so buffered hints survive a coordinator
// crash; New returns a memory-only store with no log.
type HintStore struct {
	mu         sync.Mutex
	hints      map[string][]storedHint
	maxPerNode int
	ttl        time.Duration
	nextSeq    uint64
	log        *hintLog // nil = memory-only
}

func New(maxPerNode int, ttl time.Duration) *HintStore {
	return &HintStore{
		hints:      make(map[string][]storedHint),
		maxPerNode: maxPerNode,
		ttl:        ttl,
	}
}

// Store buffers a hint for nodeID. If the per-node cap is reached the oldest
// hint is evicted; anti-entropy will repair any keys whose hints were dropped.
func (hs *HintStore) Store(nodeID string, h Hint) {
	hs.mu.Lock()
	hints := hs.hints[nodeID]
	var (
		evictedSeq uint64
		evicted    bool
	)
	if len(hints) >= hs.maxPerNode {
		evictedSeq = hints[0].seq
		evicted = true
		hints = hints[1:] // drop oldest
	}
	hs.nextSeq++
	sh := storedHint{Hint: h, seq: hs.nextSeq}
	hs.hints[nodeID] = append(hints, sh)

	logRef := hs.log
	var walSeq uint64
	if logRef != nil {
		if evicted {
			// The store record below gets a higher wal seq, so syncing up to it
			// covers this remove too; no need to capture its seq.
			logRef.appendRemove(nodeID, []uint64{evictedSeq})
		}
		walSeq = logRef.appendStore(nodeID, sh)
	}
	hs.mu.Unlock()

	// Group commit: batch this fsync with other concurrent writers. Hint
	// durability is best-effort (anti-entropy backstops loss), so a failure is
	// logged rather than surfaced.
	if logRef != nil {
		if err := logRef.syncUpTo(walSeq); err != nil {
			log.Printf("hintstore: sync failed for %s: %v", nodeID, err)
		}
	}
}

// Drain removes and returns all buffered hints for nodeID. Returns nil if
// there are no buffered hints.
func (hs *HintStore) Drain(nodeID string) []Hint {
	hs.mu.Lock()
	stored := hs.hints[nodeID]
	if len(stored) == 0 {
		hs.mu.Unlock()
		return nil
	}
	delete(hs.hints, nodeID)

	out := make([]Hint, len(stored))
	seqs := make([]uint64, len(stored))
	for i, sh := range stored {
		out[i] = sh.Hint
		seqs[i] = sh.seq
	}

	logRef := hs.log
	var walSeq uint64
	if logRef != nil {
		walSeq = logRef.appendRemove(nodeID, seqs)
	}
	hs.mu.Unlock()

	// The removal must be durable before the hints are delivered, otherwise a
	// crash mid-delivery could resurrect already-delivered hints on restart.
	if logRef != nil {
		if err := logRef.syncUpTo(walSeq); err != nil {
			log.Printf("hintstore: sync failed draining %s: %v", nodeID, err)
		}
	}
	return out
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
	removed := hs.expireLocked(time.Now().Add(-hs.ttl))
	logRef := hs.log
	var walSeq uint64
	if logRef != nil {
		walSeq = logRef.appendRemovals(removed)
	}
	hs.mu.Unlock()

	if logRef != nil && len(removed) > 0 {
		if err := logRef.syncUpTo(walSeq); err != nil {
			log.Printf("hintstore: sync failed expiring hints: %v", err)
		}
	}
	hs.maybeCompact()
}

// expireLocked drops hints older than cutoff from the in-memory map and returns
// the removed sequence numbers per node. Must be called with hs.mu held.
func (hs *HintStore) expireLocked(cutoff time.Time) map[string][]uint64 {
	removed := make(map[string][]uint64)
	for nodeID, hints := range hs.hints {
		fresh := hints[:0]
		for _, h := range hints {
			if h.At.After(cutoff) {
				fresh = append(fresh, h)
			} else {
				removed[nodeID] = append(removed[nodeID], h.seq)
			}
		}
		if len(fresh) == 0 {
			delete(hs.hints, nodeID)
		} else {
			hs.hints[nodeID] = fresh
		}
	}
	return removed
}
