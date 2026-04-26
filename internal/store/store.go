package store

import (
	"sync"

	"github.com/spaolacci/murmur3"
)

// VectorClockVersion tracks per-node logical counters. A clock A happens-before
// B iff every counter in A is ≤ the corresponding counter in B and at least one
// is strictly less (standard Lamport partial order).
type VectorClockVersion struct {
	Clocks map[string]uint64 `json:"clocks"`
}

// NewClock returns an empty vector clock.
func NewClock() VectorClockVersion {
	return VectorClockVersion{Clocks: make(map[string]uint64)}
}

// Increment returns a new clock with nodeID's counter incremented by one.
// The receiver is not modified.
func (v VectorClockVersion) Increment(nodeID string) VectorClockVersion {
	clocks := make(map[string]uint64, len(v.Clocks)+1)
	for k, val := range v.Clocks {
		clocks[k] = val
	}
	clocks[nodeID]++
	return VectorClockVersion{Clocks: clocks}
}

func (v VectorClockVersion) HappensBefore(other VectorClockVersion) bool {
	atLeastOneLess := false
	for nodeID, vClock := range v.Clocks {
		oClock := other.Clocks[nodeID]
		if vClock > oClock {
			return false
		}
		if vClock < oClock {
			atLeastOneLess = true
		}
	}
	for nodeID, oClock := range other.Clocks {
		if _, exists := v.Clocks[nodeID]; !exists && oClock > 0 {
			atLeastOneLess = true
		}
	}
	return atLeastOneLess
}

// Equal reports whether v and other have identical clock counters.
func (v VectorClockVersion) Equal(other VectorClockVersion) bool {
	if len(v.Clocks) != len(other.Clocks) {
		return false
	}
	for k, vc := range v.Clocks {
		if oc, ok := other.Clocks[k]; !ok || vc != oc {
			return false
		}
	}
	return true
}

// Sibling is a single causally-distinct value for a key. Deleted=true marks a
// tombstone: the key was deleted at this vector clock position.
type Sibling struct {
	Value   string
	Deleted bool
	Version VectorClockVersion
	Hash    uint32 // murmur3(value), reserved for Merkle anti-entropy
}

// Entry holds all active siblings for a key. Len(Siblings)==1 means no
// conflict; Len(Siblings)>1 means concurrent writes exist and should be
// surfaced to the client for resolution.
type Entry struct {
	Siblings []Sibling
}

type Store struct {
	mu       sync.RWMutex
	data     map[string]Entry
	onUpdate func(key string, hash uint32)
}

func New() *Store {
	return &Store{data: make(map[string]Entry)}
}

// SetOnUpdate registers a callback invoked after every write that changes the
// store. hash is the canonical hash of the key's new state, suitable for
// updating a Merkle tree. Safe to call before any writes.
func (s *Store) SetOnUpdate(fn func(key string, hash uint32)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onUpdate = fn
}

// tombstoneSentinel is XOR'd into entryHash for deleted siblings so that a
// tombstone and a zero-hash value produce different hashes.
const tombstoneSentinel uint32 = 0x544f4d42 // "TOMB"

// entryHash returns a canonical hash for an entry by XOR-ing all sibling
// hashes. Commutative across siblings, so sibling order doesn't matter.
func entryHash(e Entry) uint32 {
	var h uint32
	for _, sib := range e.Siblings {
		if sib.Deleted {
			h ^= tombstoneSentinel
		} else {
			h ^= sib.Hash
		}
	}
	return h
}

// applySibling applies conflict-resolution logic for incoming against the
// existing entry. Returns true if the store was modified. Must be called with
// s.mu held for writing.
func (s *Store) applySibling(key string, incoming Sibling) bool {
	existing, ok := s.data[key]
	if !ok {
		s.data[key] = Entry{Siblings: []Sibling{incoming}}
		return true
	}

	var survivors []Sibling
	for _, sib := range existing.Siblings {
		if incoming.Version.HappensBefore(sib.Version) {
			return false
		}
		if sib.Version.Equal(incoming.Version) {
			return false
		}
		if !sib.Version.HappensBefore(incoming.Version) {
			survivors = append(survivors, sib)
		}
	}

	s.data[key] = Entry{Siblings: append(survivors, incoming)}
	return true
}

// Put stores key=value at version v. If v is dominated by any existing sibling
// the write is dropped. If v dominates existing siblings they are replaced. If v
// is concurrent with existing siblings it is appended, producing a conflict.
// Equal clocks are treated as an idempotent write and ignored.
func (s *Store) Put(key, value string, v VectorClockVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applySibling(key, Sibling{
		Value:   value,
		Version: v,
		Hash:    murmur3.Sum32([]byte(value)),
	}) && s.onUpdate != nil {
		s.onUpdate(key, entryHash(s.data[key]))
	}
}

// Delete writes a tombstone for key at version v. The tombstone participates in
// conflict resolution identically to a value write: it wins if v dominates
// existing siblings, loses if dominated, and becomes a sibling on concurrent
// writes. Tombstones are never garbage-collected until anti-entropy is in place.
func (s *Store) Delete(key string, v VectorClockVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applySibling(key, Sibling{Deleted: true, Version: v}) && s.onUpdate != nil {
		s.onUpdate(key, entryHash(s.data[key]))
	}
}

func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

// KeyHashes returns a snapshot of every key and its current entry hash.
// Used by the anti-entropy manager to populate Merkle trees on startup and by
// the sync endpoint to compute bucket hashes on-the-fly.
func (s *Store) KeyHashes() map[string]uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]uint32, len(s.data))
	for key, entry := range s.data {
		out[key] = entryHash(entry)
	}
	return out
}
