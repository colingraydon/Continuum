package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/colingraydon/continuum/internal/sstable"
	"github.com/colingraydon/continuum/internal/wal"
	"github.com/twmb/murmur3"
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

// WAL is the contract the store needs from a write-ahead log. *wal.Writer
// satisfies it; tests inject fakes to exercise error paths. SyncUpTo batches
// concurrent hot-path fsyncs (group commit); Sync is the synchronous flush
// used by the strict durable-before-visible mutations.
type WAL interface {
	Append(payload []byte) (uint64, error)
	Sync() error
	SyncUpTo(seq uint64) error
	TruncateThrough(seq uint64) error
}

// memtable is an immutable snapshot of the active write set, frozen while it
// is flushed to an SSTable. Reads consult it between the active memtable and
// the tables; nothing mutates it after the freeze.
type memtable struct {
	data    map[string]Entry
	evicted map[string]struct{}
	ages    map[string]time.Time
	seq     uint64 // highest WAL sequence covered by this memtable
}

// memEntryOverhead is the per-entry constant added to the memtable size
// estimate on top of key and value bytes.
const memEntryOverhead = 64

// Store is an LSM-shaped versioned KV store. Writes land in an in-memory
// memtable (WAL-durable when one is installed); when the memtable exceeds the
// flush threshold it is frozen and written out as an immutable SSTable, and
// the WAL segments it covers are deleted.
//
// Generations are searched newest-first: memtable, frozen memtable, tables.
// Every write merges any older-generation state for its key into the
// memtable first (one bloom-guarded table probe), preserving the invariant
// that a generation holding a key holds its complete merged sibling set —
// so a lookup stops at the first generation that contains the key.
type Store struct {
	mu            sync.RWMutex
	data          map[string]Entry
	evicted       map[string]struct{} // keys locally evicted but still present in tables
	tombstoneAges map[string]time.Time
	frozen        *memtable   // non-nil while a flush is in progress or pending retry
	tables        []liveTable // immutable readers, newest first
	onUpdate      func(key string, hash uint32)
	onEvict       func(key string)
	wal           WAL // nil = memory-only mode
	lastSeq       uint64
	memBytes      int64
	flushDir      string // "" = flushing disabled
	flushBytes    int64
	flushing      bool
	maxTableSeq   uint64 // highest WAL sequence any attached table covers
	nextFileNum   uint64 // next table file number to allocate
	compacting    bool
	compaction    compactionPolicy
	blockCache    *sstable.Cache // shared across all table readers; nil = uncached
	// tablesRW guards table-reader lifetime. Lock-free reads (Get, KeyHashes)
	// hold it shared while reading from table files outside s.mu; compaction
	// takes it exclusively before closing retired readers, so a closed reader
	// can never be read. It is distinct from s.mu so table IO does not block
	// writers.
	tablesRW sync.RWMutex
}

func New() *Store {
	return &Store{
		data:          make(map[string]Entry),
		evicted:       make(map[string]struct{}),
		tombstoneAges: make(map[string]time.Time),
		nextFileNum:   1,
		compaction:    defaultCompactionPolicy(),
	}
}

// SetWAL installs a write-ahead log. After SetWAL, every mutating operation
// appends to the log and fsyncs before applying to memory. If Append or
// Sync returns an error the in-memory state is not modified and the error
// is returned to the caller. Safe to call before any writes. Passing nil
// disables WAL durability.
func (s *Store) SetWAL(w WAL) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w == nil {
		s.wal = nil
		return
	}
	s.wal = w
}

// SetBlockCache installs a shared SSTable block cache attached to every
// table reader the store opens. Call once at startup, before OpenTables and
// before any flush or compaction can run; a nil cache disables caching.
func (s *Store) SetBlockCache(c *sstable.Cache) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockCache = c
}

// BlockCacheStats reports block cache effectiveness; all zeros when no cache
// is installed.
func (s *Store) BlockCacheStats() sstable.CacheStats {
	s.mu.RLock()
	c := s.blockCache
	s.mu.RUnlock()
	return c.Stats()
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

// LastSeq returns the highest WAL sequence applied to the store.
func (s *Store) LastSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastSeq
}

// tombstoneSentinel is XOR'd into entryHash for deleted siblings so that a
// tombstone and a zero-hash value produce different hashes.
const tombstoneSentinel uint32 = 0x544f4d42 // "TOMB"

// entryHash returns a canonical hash for an entry by XOR-ing all sibling
// contributions. Commutative across siblings, so sibling order doesn't
// matter. Each contribution folds in the sibling's vector clock: replicas
// holding the same value at different clocks must hash differently, or
// anti-entropy can never see (and repair) the clock divergence.
func entryHash(e Entry) uint32 {
	var h uint32
	for _, sib := range e.Siblings {
		sh := sib.Hash
		if sib.Deleted {
			sh = tombstoneSentinel
		}
		h ^= sh ^ clockHash(sib.Version)
	}
	return h
}

// clockHash returns a canonical hash of a vector clock: node IDs are sorted
// so the result is independent of map iteration order.
func clockHash(v VectorClockVersion) uint32 {
	if len(v.Clocks) == 0 {
		return 0
	}
	ids := make([]string, 0, len(v.Clocks))
	for id := range v.Clocks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var buf bytes.Buffer
	for _, id := range ids {
		buf.WriteString(id)
		buf.WriteByte(0)
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], v.Clocks[id])
		buf.Write(n[:])
	}
	return murmur3.Sum32(buf.Bytes())
}

// lookupLocked returns the complete visible entry for key, searching
// generations newest-first. Must be called with s.mu held (read or write).
func (s *Store) lookupLocked(key string) (Entry, bool, error) {
	if e, ok := s.data[key]; ok {
		return e, true, nil
	}
	if _, ok := s.evicted[key]; ok {
		return Entry{}, false, nil
	}
	if f := s.frozen; f != nil {
		if e, ok := f.data[key]; ok {
			return e, true, nil
		}
		if _, ok := f.evicted[key]; ok {
			return Entry{}, false, nil
		}
	}
	return tableGet(s.tables, key)
}

// tableGet probes tables newest-first and returns the first hit. An evict
// marker means the key was migrated away: it shadows older tables.
func tableGet(tables []liveTable, key string) (Entry, bool, error) {
	kb := []byte(key)
	for _, t := range tables {
		val, ok, err := t.r.Get(kb)
		if err != nil {
			return Entry{}, false, fmt.Errorf("store: table lookup %q: %w", key, err)
		}
		if !ok {
			continue
		}
		e, evicted, err := decodeTableEntry(val)
		if err != nil {
			return Entry{}, false, fmt.Errorf("store: decode table entry %q: %w", key, err)
		}
		if evicted {
			return Entry{}, false, nil
		}
		return e, true, nil
	}
	return Entry{}, false, nil
}

// shadowedBelowLocked reports whether key is visible in a generation below
// the active memtable (frozen memtable or tables). Used to decide whether an
// eviction or tombstone purge needs to leave shadowing state behind. Must be
// called with s.mu held.
func (s *Store) shadowedBelowLocked(key string) (bool, error) {
	if f := s.frozen; f != nil {
		if _, ok := f.evicted[key]; ok {
			return false, nil
		}
		if _, ok := f.data[key]; ok {
			return true, nil
		}
	}
	_, found, err := tableGet(s.tables, key)
	return found, err
}

// mergeSibling computes the post-merge entry for incoming against the
// complete visible state of key, without mutating anything. Returns false if
// incoming is dominated by (or equal to) an existing sibling and should be
// dropped. Must be called with s.mu held.
func (s *Store) mergeSibling(key string, incoming Sibling) (Entry, bool, error) {
	existing, found, err := s.lookupLocked(key)
	if err != nil {
		return Entry{}, false, err
	}
	if !found {
		return Entry{Siblings: []Sibling{incoming}}, true, nil
	}
	var survivors []Sibling
	for _, sib := range existing.Siblings {
		if incoming.Version.HappensBefore(sib.Version) {
			return Entry{}, false, nil
		}
		if sib.Version.Equal(incoming.Version) {
			return Entry{}, false, nil
		}
		if !sib.Version.HappensBefore(incoming.Version) {
			survivors = append(survivors, sib)
		}
	}
	return Entry{Siblings: append(survivors, incoming)}, true, nil
}

// commitEntry installs a merged entry into the memtable. Because mergeSibling
// folded in any older-generation state, the memtable now subsumes everything
// below it for this key. Must be called with s.mu held for writing.
func (s *Store) commitEntry(key string, e Entry, incoming Sibling) {
	s.data[key] = e
	delete(s.evicted, key)
	s.memBytes += int64(len(key)+len(incoming.Value)) + memEntryOverhead
}

// Put stores key=value at version v. If v is dominated by any existing sibling
// the write is dropped (and not logged to the WAL). If v dominates existing
// siblings they are replaced. If v is concurrent with existing siblings it is
// appended, producing a conflict. Equal clocks are treated as an idempotent
// write and ignored. When a WAL is installed, the write is logged and fsynced
// before being applied to memory; any WAL error returns without modifying
// state. May trigger a memtable flush after the write is applied.
func (s *Store) Put(key, value string, v VectorClockVersion) error {
	s.mu.Lock()
	incoming := Sibling{Value: value, Version: v, Hash: murmur3.Sum32([]byte(value))}
	merged, applied, err := s.mergeSibling(key, incoming)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if !applied {
		s.mu.Unlock()
		return nil
	}
	var (
		w   WAL
		seq uint64
	)
	if s.wal != nil {
		payload, err := encodePut(key, value, v)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		seq, err = s.walAppendLocked(payload)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		w = s.wal
	}
	s.commitEntry(key, merged, incoming)
	// A live write supersedes any prior tombstone age for this key. If the
	// key is deleted again later, the new tombstone gets a fresh timestamp.
	delete(s.tombstoneAges, key)
	if s.onUpdate != nil {
		s.onUpdate(key, entryHash(merged))
	}
	s.mu.Unlock()
	// Group commit: the record is buffered and applied; SyncUpTo batches this
	// fsync with other concurrent writers before the write is acknowledged.
	if w != nil {
		if err := w.SyncUpTo(seq); err != nil {
			return err
		}
	}
	s.flushIfNeeded()
	return nil
}

// Delete writes a tombstone for key at version v. The tombstone participates in
// conflict resolution identically to a value write: it wins if v dominates
// existing siblings, loses if dominated, and becomes a sibling on concurrent
// writes. When a WAL is installed, the tombstone (with its original wall time)
// is logged and fsynced before being applied to memory.
func (s *Store) Delete(key string, v VectorClockVersion) error {
	s.mu.Lock()
	incoming := Sibling{Deleted: true, Version: v}
	merged, applied, err := s.mergeSibling(key, incoming)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if !applied {
		s.mu.Unlock()
		return nil
	}
	tombAt := time.Now()
	var (
		w   WAL
		seq uint64
	)
	if s.wal != nil {
		payload, err := encodeDelete(key, tombAt, v)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		seq, err = s.walAppendLocked(payload)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		w = s.wal
	}
	s.commitEntry(key, merged, incoming)
	// Always record the current time so that a new deletion event (different
	// clock) resets the TTL window. Equal-clock re-applications never reach
	// this branch because mergeSibling rejects idempotent writes.
	s.tombstoneAges[key] = tombAt
	if s.onUpdate != nil {
		s.onUpdate(key, entryHash(merged))
	}
	s.mu.Unlock()
	// Group commit: see Put.
	if w != nil {
		if err := w.SyncUpTo(seq); err != nil {
			return err
		}
	}
	s.flushIfNeeded()
	return nil
}

// Evict removes key from the store without creating a tombstone. This is a
// local bookkeeping operation for keys that have migrated to another node; it
// must not be used for logical deletes (use Delete for that). If the key is
// still visible in a frozen memtable or table, an evict marker is left in the
// memtable so the older copy stays shadowed; compaction removes both later.
// The onEvict callback fires so the anti-entropy manager can drop the key
// from its trees. When a WAL is installed, the eviction is logged and fsynced
// before applying.
func (s *Store) Evict(key string) error {
	s.mu.Lock()
	if s.wal != nil {
		payload, err := encodeEvict(key)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if err := s.walAppendSyncLocked(payload); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	if err := s.evictLocked(key); err != nil {
		s.mu.Unlock()
		return err
	}
	onEvict := s.onEvict
	s.mu.Unlock()
	if onEvict != nil {
		onEvict(key)
	}
	s.flushIfNeeded()
	return nil
}

// evictLocked applies eviction to the memtable: shared by Evict and WAL
// replay. Must be called with s.mu held for writing.
func (s *Store) evictLocked(key string) error {
	below, err := s.shadowedBelowLocked(key)
	if err != nil {
		return err
	}
	delete(s.data, key)
	delete(s.tombstoneAges, key)
	if below {
		s.evicted[key] = struct{}{}
		s.memBytes += int64(len(key)) + memEntryOverhead
	}
	return nil
}

// walAppendLocked appends payload and records the assigned sequence WITHOUT
// fsyncing, returning the sequence. The hot path (Put, Delete) batches the
// fsync via SyncUpTo after releasing s.mu; the strict mutations follow with a
// synchronous Sync. Must be called with s.mu held for writing and s.wal
// non-nil.
func (s *Store) walAppendLocked(payload []byte) (uint64, error) {
	seq, err := s.wal.Append(payload)
	if err != nil {
		return 0, err
	}
	s.lastSeq = seq
	return seq, nil
}

// walAppendSyncLocked appends payload and fsyncs before returning, preserving
// the durable-before-visible contract. Used by the non-hot-path mutations
// (evict, GC, checkpoint). Must be called with s.mu held for writing and
// s.wal non-nil.
func (s *Store) walAppendSyncLocked(payload []byte) error {
	if _, err := s.walAppendLocked(payload); err != nil {
		return err
	}
	return s.wal.Sync()
}

// GCTombstones removes uncontested tombstones — entries with exactly one
// sibling that is deleted — older than maxAge. It returns the purged keys so
// callers can remove them from auxiliary structures such as Merkle trees.
// Tombstones whose key is still visible in a frozen memtable or table are
// skipped: purging them would resurrect the older value on read. Those are
// reclaimed at compaction instead. When a WAL is installed, a single GC
// record listing every purged key is appended and fsynced before the
// in-memory deletes happen, so WAL replay does not resurrect them.
//
// Safety: only call after bidirectional anti-entropy has had time to propagate
// tombstones to all replicas. maxAge must be longer than the maximum expected
// propagation window (see gcTTL in the anti-entropy manager).
func (s *Store) GCTombstones(maxAge time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidates := collectExpiredTombstones(s.data, s.tombstoneAges, time.Now().Add(-maxAge))
	var purged []string
	for _, key := range candidates {
		below, err := s.shadowedBelowLocked(key)
		if err != nil {
			return nil, err
		}
		if !below {
			purged = append(purged, key)
		}
	}
	if len(purged) == 0 {
		return nil, nil
	}
	if s.wal != nil {
		payload, err := encodeGC(purged)
		if err != nil {
			return nil, err
		}
		if err := s.walAppendSyncLocked(payload); err != nil {
			return nil, err
		}
	}
	for _, key := range purged {
		delete(s.data, key)
		delete(s.tombstoneAges, key)
	}
	return purged, nil
}

func collectExpiredTombstones(data map[string]Entry, ages map[string]time.Time, cutoff time.Time) []string {
	var purged []string
	for key, entry := range data {
		if len(entry.Siblings) != 1 || !entry.Siblings[0].Deleted {
			continue
		}
		age, ok := ages[key]
		if !ok || age.After(cutoff) {
			continue
		}
		purged = append(purged, key)
	}
	return purged
}

// Get returns the complete visible entry for key. It may read from disk when
// the key lives only in an SSTable; a table read or decode failure is
// surfaced as an error.
func (s *Store) Get(key string) (Entry, bool, error) {
	s.mu.RLock()
	if e, ok := s.data[key]; ok {
		s.mu.RUnlock()
		return e, true, nil
	}
	if _, ok := s.evicted[key]; ok {
		s.mu.RUnlock()
		return Entry{}, false, nil
	}
	if f := s.frozen; f != nil {
		if e, ok := f.data[key]; ok {
			s.mu.RUnlock()
			return e, true, nil
		}
		if _, ok := f.evicted[key]; ok {
			s.mu.RUnlock()
			return Entry{}, false, nil
		}
	}
	tables := s.tables
	// Acquire the table-reader guard before releasing s.mu so a compaction
	// cannot retire and close a reader in this captured slice between here and
	// the read below.
	s.tablesRW.RLock()
	s.mu.RUnlock()
	defer s.tablesRW.RUnlock()
	// Table probing happens outside s.mu: the slice is immutable and any key
	// concurrently moving generations was already covered by the checks above.
	return tableGet(tables, key)
}

// WriteCheckpoint appends a CHECKPOINT record naming snapshotSeq and fsyncs.
// Used after a snapshot is durable to mark which WAL prefix is covered.
// No-op if no WAL is installed. Legacy: superseded by seq-named SSTables;
// kept so old WALs containing CHECKPOINT records stay replayable.
func (s *Store) WriteCheckpoint(snapshotSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	return s.walAppendSyncLocked(encodeCheckpoint(snapshotSeq))
}

// Replay applies every record from r whose sequence is greater than
// skipBelow. Suppresses the onUpdate callback; callers are expected to
// rebuild any derived state (e.g. Merkle trees) after Replay returns.
// Attach tables (OpenTables) before replaying so records merge against
// table-resident state. Returns the highest sequence number observed
// (whether skipped or applied).
func (s *Store) Replay(r *wal.Reader, skipBelow uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var lastSeq uint64
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			if lastSeq > s.lastSeq {
				s.lastSeq = lastSeq
			}
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
		return s.applyPutRecord(body)
	case recDelete:
		return s.applyDeleteRecord(body)
	case recEvict:
		return s.applyEvictRecord(body)
	case recGC:
		return s.applyGCRecord(body)
	case recCheckpoint:
		return s.applyCheckpointRecord(body)
	default:
		return fmt.Errorf("%w: 0x%02x", errUnknownRecordType, payload[0])
	}
}

func (s *Store) applyPutRecord(body []byte) error {
	key, value, v, err := decodePut(body)
	if err != nil {
		return err
	}
	incoming := Sibling{Value: value, Version: v, Hash: murmur3.Sum32([]byte(value))}
	merged, applied, err := s.mergeSibling(key, incoming)
	if err != nil {
		return err
	}
	if applied {
		s.commitEntry(key, merged, incoming)
		delete(s.tombstoneAges, key)
	}
	return nil
}

func (s *Store) applyDeleteRecord(body []byte) error {
	key, tombAt, v, err := decodeDelete(body)
	if err != nil {
		return err
	}
	incoming := Sibling{Deleted: true, Version: v}
	merged, applied, err := s.mergeSibling(key, incoming)
	if err != nil {
		return err
	}
	if applied {
		s.commitEntry(key, merged, incoming)
		s.tombstoneAges[key] = tombAt
	}
	return nil
}

func (s *Store) applyEvictRecord(body []byte) error {
	key, err := decodeEvict(body)
	if err != nil {
		return err
	}
	return s.evictLocked(key)
}

func (s *Store) applyGCRecord(body []byte) error {
	keys, err := decodeGC(body)
	if err != nil {
		return err
	}
	for _, key := range keys {
		delete(s.data, key)
		delete(s.tombstoneAges, key)
	}
	return nil
}

func (s *Store) applyCheckpointRecord(body []byte) error {
	_, err := decodeCheckpoint(body)
	return err
}

// SetTombstoneAge overrides the recorded age for a tombstone. Intended for
// tests that need to simulate aged-out tombstones without sleeping.
func (s *Store) SetTombstoneAge(key string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstoneAges[key] = t
}

// KeyHashes returns every visible key and its current entry hash, merged
// across all generations. Used by the anti-entropy manager to populate Merkle
// trees on startup and by the sync endpoints to compute bucket hashes
// on-the-fly. Scans every table, so cost is proportional to total data.
func (s *Store) KeyHashes() (map[string]uint32, error) {
	s.mu.RLock()
	tables := s.tables
	frozen := s.frozen
	// Hold the table-reader guard across the scan (acquired before releasing
	// s.mu) so compaction cannot close a reader mid-iteration. It is released
	// before the memtable overlay re-acquires s.mu below.
	s.tablesRW.RLock()
	s.mu.RUnlock()

	out, err := scanTableHashes(tables)
	s.tablesRW.RUnlock()
	if err != nil {
		return nil, err
	}
	if frozen != nil {
		overlayHashes(out, frozen.data, frozen.evicted)
	}
	s.mu.RLock()
	overlayHashes(out, s.data, s.evicted)
	s.mu.RUnlock()
	return out, nil
}

// scanTableHashes folds every table's entries into a key→hash map, oldest to
// newest so later generations overwrite earlier ones and evict markers remove
// the key entirely. The caller holds s.tablesRW for the duration.
func scanTableHashes(tables []liveTable) (map[string]uint32, error) {
	out := make(map[string]uint32)
	for i := len(tables) - 1; i >= 0; i-- {
		it := tables[i].r.Iter()
		for it.Next() {
			key := string(it.Key())
			e, evicted, err := decodeTableEntry(it.Value())
			if err != nil {
				return nil, fmt.Errorf("store: decode table entry %q: %w", key, err)
			}
			if evicted {
				delete(out, key)
				continue
			}
			out[key] = entryHash(e)
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("store: table scan: %w", err)
		}
	}
	return out, nil
}

func overlayHashes(out map[string]uint32, data map[string]Entry, evicted map[string]struct{}) {
	for key, e := range data {
		out[key] = entryHash(e)
	}
	for key := range evicted {
		delete(out, key)
	}
}
