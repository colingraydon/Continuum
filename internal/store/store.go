package store

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
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
	mu            sync.RWMutex
	data          map[string]Entry
	onUpdate      func(key string, hash uint32)
	onEvict       func(key string)
	tombstoneAges map[string]time.Time // key → when tombstone was first accepted on this node
	wal           *wal.Writer          // nil = memory-only mode
}

func New() *Store {
	return &Store{
		data:          make(map[string]Entry),
		tombstoneAges: make(map[string]time.Time),
	}
}

// SetWAL installs a write-ahead log writer. After SetWAL, every mutating
// operation appends to the log and fsyncs before applying to memory. If
// Append or Sync returns an error the in-memory state is not modified and
// the error is returned to the caller. Safe to call before any writes.
// Passing nil disables WAL durability.
func (s *Store) SetWAL(w *wal.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wal = w
}

// SetOnUpdate registers a callback invoked after every write that changes the
// store. hash is the canonical hash of the key's new state, suitable for
// updating a Merkle tree. Safe to call before any writes.
func (s *Store) SetOnUpdate(fn func(key string, hash uint32)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onUpdate = fn
}

// SetOnEvict registers a callback invoked when a key is evicted via Evict.
// The anti-entropy manager uses this to remove the key from its Merkle trees.
func (s *Store) SetOnEvict(fn func(key string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onEvict = fn
}

// Evict removes key from the store without creating a tombstone. This is a
// local bookkeeping operation for keys that have migrated to another node; it
// must not be used for logical deletes (use Delete for that). The onEvict
// callback fires so the anti-entropy manager can drop the key from its trees.
// When a WAL is installed, the eviction is logged and fsynced before applying.
func (s *Store) Evict(key string) error {
	s.mu.Lock()
	if s.wal != nil {
		payload, err := encodeEvict(key)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if _, err := s.wal.Append(payload); err != nil {
			s.mu.Unlock()
			return err
		}
		if err := s.wal.Sync(); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	onEvict := s.onEvict
	delete(s.data, key)
	delete(s.tombstoneAges, key)
	s.mu.Unlock()
	if onEvict != nil {
		onEvict(key)
	}
	return nil
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
// Equal clocks are treated as an idempotent write and ignored. When a WAL is
// installed, the write is logged and fsynced before being applied to memory;
// any WAL error returns without modifying state.
func (s *Store) Put(key, value string, v VectorClockVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal != nil {
		payload, err := encodePut(key, value, v)
		if err != nil {
			return err
		}
		if _, err := s.wal.Append(payload); err != nil {
			return err
		}
		if err := s.wal.Sync(); err != nil {
			return err
		}
	}
	if s.applySibling(key, Sibling{
		Value:   value,
		Version: v,
		Hash:    murmur3.Sum32([]byte(value)),
	}) {
		// A live write supersedes any prior tombstone age for this key. If the
		// key is deleted again later, the new tombstone gets a fresh timestamp.
		delete(s.tombstoneAges, key)
		if s.onUpdate != nil {
			s.onUpdate(key, entryHash(s.data[key]))
		}
	}
	return nil
}

// Delete writes a tombstone for key at version v. The tombstone participates in
// conflict resolution identically to a value write: it wins if v dominates
// existing siblings, loses if dominated, and becomes a sibling on concurrent
// writes. When a WAL is installed, the tombstone (with its original wall time)
// is logged and fsynced before being applied to memory.
func (s *Store) Delete(key string, v VectorClockVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tombAt := time.Now()
	if s.wal != nil {
		payload, err := encodeDelete(key, tombAt, v)
		if err != nil {
			return err
		}
		if _, err := s.wal.Append(payload); err != nil {
			return err
		}
		if err := s.wal.Sync(); err != nil {
			return err
		}
	}
	if s.applySibling(key, Sibling{Deleted: true, Version: v}) {
		// Always record the current time so that a new deletion event (different
		// clock) resets the TTL window. Equal-clock re-applications never reach
		// this branch because applySibling returns false for idempotent writes.
		s.tombstoneAges[key] = tombAt
		if s.onUpdate != nil {
			s.onUpdate(key, entryHash(s.data[key]))
		}
	}
	return nil
}

// GCTombstones removes uncontested tombstones — entries with exactly one
// sibling that is deleted — older than maxAge. It returns the purged keys so
// callers can remove them from auxiliary structures such as Merkle trees.
// When a WAL is installed, a single GC record listing every purged key is
// appended and fsynced before the in-memory deletes happen. This guarantees
// that WAL replay does not resurrect already-GC'd tombstones.
//
// Safety: only call after bidirectional anti-entropy has had time to propagate
// tombstones to all replicas. maxAge must be longer than the maximum expected
// propagation window (see gcTTL in the anti-entropy manager).
func (s *Store) GCTombstones(maxAge time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	var purged []string
	for key, entry := range s.data {
		if len(entry.Siblings) != 1 || !entry.Siblings[0].Deleted {
			continue
		}
		age, ok := s.tombstoneAges[key]
		if !ok || age.After(cutoff) {
			continue
		}
		purged = append(purged, key)
	}
	if len(purged) == 0 {
		return nil, nil
	}
	if s.wal != nil {
		payload, err := encodeGC(purged)
		if err != nil {
			return nil, err
		}
		if _, err := s.wal.Append(payload); err != nil {
			return nil, err
		}
		if err := s.wal.Sync(); err != nil {
			return nil, err
		}
	}
	for _, key := range purged {
		delete(s.data, key)
		delete(s.tombstoneAges, key)
	}
	return purged, nil
}

func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

// WriteCheckpoint appends a CHECKPOINT record naming snapshotSeq and fsyncs.
// Used after a snapshot is durable to mark which WAL prefix is covered.
// No-op if no WAL is installed.
func (s *Store) WriteCheckpoint(snapshotSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	payload, err := encodeCheckpoint(snapshotSeq)
	if err != nil {
		return err
	}
	if _, err := s.wal.Append(payload); err != nil {
		return err
	}
	return s.wal.Sync()
}

// Replay applies every record from r whose sequence is greater than
// skipBelow. Suppresses the onUpdate callback; callers are expected to
// rebuild any derived state (e.g. Merkle trees) after Replay returns.
// Returns the highest sequence number observed (whether skipped or applied).
func (s *Store) Replay(r *wal.Reader, skipBelow uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var lastSeq uint64
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return lastSeq, nil
		}
		if err != nil {
			return 0, err
		}
		lastSeq = rec.Seq
		if rec.Seq <= skipBelow {
			continue
		}
		if err := s.applyWALRecord(rec.Payload); err != nil {
			return 0, fmt.Errorf("apply seq %d: %w", rec.Seq, err)
		}
	}
}

// applyWALRecord decodes and applies a single WAL payload. Caller must hold
// s.mu. Suppresses onUpdate.
func (s *Store) applyWALRecord(payload []byte) error {
	if len(payload) < 1 {
		return errors.New("store: empty wal record")
	}
	body := payload[1:]
	switch payload[0] {
	case recPut:
		key, value, v, err := decodePut(body)
		if err != nil {
			return err
		}
		sib := Sibling{Value: value, Version: v, Hash: murmur3.Sum32([]byte(value))}
		if s.applySibling(key, sib) {
			delete(s.tombstoneAges, key)
		}
	case recDelete:
		key, tombAt, v, err := decodeDelete(body)
		if err != nil {
			return err
		}
		if s.applySibling(key, Sibling{Deleted: true, Version: v}) {
			s.tombstoneAges[key] = tombAt
		}
	case recEvict:
		key, err := decodeEvict(body)
		if err != nil {
			return err
		}
		delete(s.data, key)
		delete(s.tombstoneAges, key)
	case recGC:
		keys, err := decodeGC(body)
		if err != nil {
			return err
		}
		for _, key := range keys {
			delete(s.data, key)
			delete(s.tombstoneAges, key)
		}
	case recCheckpoint:
		// No-op for state; the snapshot's sequence_at drives skip logic.
		if _, err := decodeCheckpoint(body); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: 0x%02x", errUnknownRecordType, payload[0])
	}
	return nil
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
