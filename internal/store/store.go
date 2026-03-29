package store

import (
	"sync"

	"github.com/spaolacci/murmur3"
)

// Version represents a causality token for conflict resolution.
// HappensBefore reports whether v is strictly causally earlier than other.
// A new write replaces an existing entry only when the existing version
// HappensBefore the incoming one — concurrent versions keep the existing value.
type Version interface {
	HappensBefore(other Version) bool
}

// VectorClockVersion tracks per-node logical counters. A clock A happens-before
// B iff every counter in A is <= the corresponding counter in B and at least one
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

func (v VectorClockVersion) HappensBefore(other Version) bool {
	o, ok := other.(VectorClockVersion)
	if !ok {
		return false
	}
	atLeastOneLess := false
	for nodeID, vClock := range v.Clocks {
		oClock := o.Clocks[nodeID]
		if vClock > oClock {
			return false
		}
		if vClock < oClock {
			atLeastOneLess = true
		}
	}
	// keys present only in o have an implicit v[i]=0, so they contribute to
	// atLeastOneLess if o[i] > 0
	for nodeID, oClock := range o.Clocks {
		if _, exists := v.Clocks[nodeID]; !exists && oClock > 0 {
			atLeastOneLess = true
		}
	}
	return atLeastOneLess
}

// Entry holds a stored value with its version and a precomputed hash for
// future Merkle tree anti-entropy.
type Entry struct {
	Value   string
	Version Version
	Hash    uint32 // murmur3(value)
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func New() *Store {
	return &Store{data: make(map[string]Entry)}
}

// Put stores key=value at version v. Drops the write if the existing entry's
// version does not happen-before v (i.e. existing is newer or concurrent).
func (s *Store) Put(key, value string, v Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.data[key]; ok && !existing.Version.HappensBefore(v) {
		return
	}
	s.data[key] = Entry{
		Value:   value,
		Version: v,
		Hash:    murmur3.Sum32([]byte(value)),
	}
}

func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}
