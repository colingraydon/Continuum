package hintstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
)

// Hint log record types (first byte of each WAL payload).
//
//	STORE:  node(len16) | seq(8) | key(len16) | value(len32) | deleted(1) |
//	        at_ns(8) | clock_count(2) | (id(len16) | counter(8))×n
//	REMOVE: node(len16) | seq_count(4) | seq(8)×n
//
// All integers are big-endian; clock entries are sorted by id for
// deterministic encoding. STORE buffers a hint; REMOVE drops the listed hint
// seqs for a node (drain, cap eviction, or TTL expiry).
const (
	opStore  byte = 0x01
	opRemove byte = 0x02
)

// compactThreshold is the number of appended records after which the log is
// rewritten to drop superseded entries, bounding on-disk growth as hints are
// stored and removed.
const compactThreshold = 1024

// compactSuffix names the scratch directory used while rewriting the log.
const compactSuffix = ".compact"

// logWriter is the subset of *wal.Writer the hint log appends through. It is an
// interface so tests can inject append/sync/close failures (mirroring the
// store.WAL seam); production always uses *wal.Writer.
type logWriter interface {
	Append(payload []byte) (uint64, error)
	SyncUpTo(seq uint64) error
	Close() error
}

// walOpen and removeAll are indirections over the WAL and filesystem so tests
// can exercise the I/O-error branches deterministically. They default to the
// real implementations.
var (
	walOpen = func(dir string) (logWriter, error) {
		w, err := wal.Open(dir)
		if err != nil {
			return nil, err
		}
		return w, nil
	}
	removeAll = os.RemoveAll
)

// hintLog is the append-only persistence backend for a HintStore, layered on
// the shared segmented WAL (CRC framing, torn-tail recovery, group commit).
// Its methods are called with the owning HintStore's mutex held except for
// syncUpTo, which the caller invokes after releasing it.
type hintLog struct {
	dir              string
	w                logWriter
	appendsSinceComp int
}

// NewPersistent opens a hint store backed by an append-only log under dir,
// replaying it to rebuild buffered hints. Hints already older than ttl are
// dropped during replay, so a long coordinator downtime self-prunes stale
// hints without coupling to the storage downtime gate.
func NewPersistent(dir string, maxPerNode int, ttl time.Duration) (*HintStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("hintstore: mkdir %s: %w", dir, err)
	}
	// Drop any scratch dir left by a compaction that crashed mid-swap.
	if err := removeAll(dir + compactSuffix); err != nil {
		return nil, fmt.Errorf("hintstore: clean compact scratch: %w", err)
	}

	hints, nextSeq, err := replayHintLog(dir, ttl)
	if err != nil {
		return nil, err
	}
	w, err := walOpen(dir)
	if err != nil {
		return nil, err
	}
	hs := New(maxPerNode, ttl)
	hs.hints = hints
	hs.nextSeq = nextSeq
	hs.log = &hintLog{dir: dir, w: w}
	return hs, nil
}

// Close compacts the log to a minimal form and closes it. Safe on a
// memory-only store (no log) and on a nil receiver.
func (hs *HintStore) Close() error {
	if hs == nil {
		return nil
	}
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.log == nil {
		return nil
	}
	if err := hs.log.rewrite(hs.hints); err != nil {
		log.Printf("hintstore: compact on close failed: %v", err)
	}
	return hs.log.w.Close()
}

// maybeCompact rewrites the log if it has accumulated enough records since the
// last compaction. Called from the periodic ExpireOld tick.
func (hs *HintStore) maybeCompact() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.log == nil || hs.log.appendsSinceComp < compactThreshold {
		return
	}
	if err := hs.log.rewrite(hs.hints); err != nil {
		log.Printf("hintstore: compaction failed: %v", err)
	}
}

func (l *hintLog) appendStore(nodeID string, sh storedHint) uint64 {
	l.appendsSinceComp++
	seq, err := l.w.Append(encodeStore(nodeID, sh))
	if err != nil {
		log.Printf("hintstore: append store record failed: %v", err)
		return 0
	}
	return seq
}

func (l *hintLog) appendRemove(nodeID string, seqs []uint64) uint64 {
	l.appendsSinceComp++
	seq, err := l.w.Append(encodeRemove(nodeID, seqs))
	if err != nil {
		log.Printf("hintstore: append remove record failed: %v", err)
		return 0
	}
	return seq
}

// appendRemovals appends one REMOVE record per node and returns the highest
// wal sequence written, or 0 if nothing was appended (so syncUpTo is a no-op).
func (l *hintLog) appendRemovals(removed map[string][]uint64) uint64 {
	var walSeq uint64
	for nodeID, seqs := range removed {
		walSeq = l.appendRemove(nodeID, seqs)
	}
	return walSeq
}

func (l *hintLog) syncUpTo(seq uint64) error {
	if seq == 0 { // append failed or nothing to sync
		return nil
	}
	return l.w.SyncUpTo(seq)
}

// rewrite replaces the log with one STORE record per live hint, dropping the
// accumulated REMOVE records. The current writer is closed, a fresh log is
// written to a scratch dir, and the scratch dir is swapped in. On a crash
// during the swap the next NewPersistent removes the orphaned scratch dir; the
// lost hints are repaired by anti-entropy.
func (l *hintLog) rewrite(hints map[string][]storedHint) error {
	if err := l.w.Close(); err != nil {
		return err
	}
	tmp := l.dir + compactSuffix
	if err := removeAll(tmp); err != nil {
		return err
	}
	nw, err := walOpen(tmp)
	if err != nil {
		return err
	}
	for nodeID, shs := range hints {
		for _, sh := range shs {
			if _, err := nw.Append(encodeStore(nodeID, sh)); err != nil {
				_ = nw.Close()
				return err
			}
		}
	}
	if err := nw.Close(); err != nil { // fsyncs the rewritten log
		return err
	}
	if err := removeAll(l.dir); err != nil {
		return err
	}
	if err := os.Rename(tmp, l.dir); err != nil {
		return err
	}
	w, err := walOpen(l.dir)
	if err != nil {
		return err
	}
	l.w = w
	l.appendsSinceComp = 0
	return nil
}

func replayHintLog(dir string, ttl time.Duration) (map[string][]storedHint, uint64, error) {
	r, err := wal.NewReader(dir)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = r.Close() }()

	hints := make(map[string][]storedHint)
	var maxSeq uint64
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("hintstore: replay: %w", err)
		}
		maxSeq, err = applyReplayRecord(hints, rec.Payload, maxSeq)
		if err != nil {
			return nil, 0, err
		}
	}

	pruneExpired(hints, time.Now().Add(-ttl))
	return hints, maxSeq, nil
}

// applyReplayRecord folds one log record into hints and returns the updated
// high-water sequence number.
func applyReplayRecord(hints map[string][]storedHint, payload []byte, maxSeq uint64) (uint64, error) {
	if len(payload) < 1 {
		return 0, fmt.Errorf("hintstore: empty record")
	}
	switch payload[0] {
	case opStore:
		nodeID, sh, err := decodeStore(payload[1:])
		if err != nil {
			return 0, err
		}
		hints[nodeID] = append(hints[nodeID], sh)
		if sh.seq > maxSeq {
			maxSeq = sh.seq
		}
	case opRemove:
		nodeID, seqs, err := decodeRemove(payload[1:])
		if err != nil {
			return 0, err
		}
		hints[nodeID] = removeSeqs(hints[nodeID], seqs)
		if len(hints[nodeID]) == 0 {
			delete(hints, nodeID)
		}
	default:
		return 0, fmt.Errorf("hintstore: unknown record type 0x%02x", payload[0])
	}
	return maxSeq, nil
}

// removeSeqs returns hints with every entry whose seq is in drop filtered out.
func removeSeqs(hints []storedHint, drop []uint64) []storedHint {
	if len(hints) == 0 || len(drop) == 0 {
		return hints
	}
	dropSet := make(map[uint64]struct{}, len(drop))
	for _, s := range drop {
		dropSet[s] = struct{}{}
	}
	out := hints[:0]
	for _, h := range hints {
		if _, gone := dropSet[h.seq]; !gone {
			out = append(out, h)
		}
	}
	return out
}

func pruneExpired(hints map[string][]storedHint, cutoff time.Time) {
	for nodeID, shs := range hints {
		fresh := shs[:0]
		for _, h := range shs {
			if h.At.After(cutoff) {
				fresh = append(fresh, h)
			}
		}
		if len(fresh) == 0 {
			delete(hints, nodeID)
		} else {
			hints[nodeID] = fresh
		}
	}
}

func encodeStore(nodeID string, sh storedHint) []byte {
	var b bytes.Buffer
	b.WriteByte(opStore)
	putString16(&b, nodeID)
	putUint64(&b, sh.seq)
	putString16(&b, sh.Key)
	putString32(&b, sh.Value)
	if sh.Deleted {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	putUint64(&b, uint64(sh.At.UnixNano()))
	putClocks(&b, sh.Clocks)
	return b.Bytes()
}

func decodeStore(body []byte) (string, storedHint, error) {
	r := bytes.NewReader(body)
	nodeID, err := readString16(r)
	if err != nil {
		return "", storedHint{}, fmt.Errorf("store node: %w", err)
	}
	seq, err := readUint64(r)
	if err != nil {
		return "", storedHint{}, fmt.Errorf("store seq: %w", err)
	}
	key, err := readString16(r)
	if err != nil {
		return "", storedHint{}, fmt.Errorf("store key: %w", err)
	}
	value, err := readString32(r)
	if err != nil {
		return "", storedHint{}, fmt.Errorf("store value: %w", err)
	}
	delByte, err := r.ReadByte()
	if err != nil {
		return "", storedHint{}, fmt.Errorf("store deleted: %w", err)
	}
	atNs, err := readUint64(r)
	if err != nil {
		return "", storedHint{}, fmt.Errorf("store at: %w", err)
	}
	clocks, err := readClocks(r)
	if err != nil {
		return "", storedHint{}, fmt.Errorf("store clocks: %w", err)
	}
	return nodeID, storedHint{
		Hint: Hint{
			Key:     key,
			Value:   value,
			Clocks:  clocks,
			Deleted: delByte == 1,
			At:      time.Unix(0, int64(atNs)),
		},
		seq: seq,
	}, nil
}

func encodeRemove(nodeID string, seqs []uint64) []byte {
	var b bytes.Buffer
	b.WriteByte(opRemove)
	putString16(&b, nodeID)
	putUint32(&b, uint32(len(seqs)))
	for _, s := range seqs {
		putUint64(&b, s)
	}
	return b.Bytes()
}

func decodeRemove(body []byte) (string, []uint64, error) {
	r := bytes.NewReader(body)
	nodeID, err := readString16(r)
	if err != nil {
		return "", nil, fmt.Errorf("remove node: %w", err)
	}
	n, err := readUint32(r)
	if err != nil {
		return "", nil, fmt.Errorf("remove count: %w", err)
	}
	seqs := make([]uint64, n)
	for i := range seqs {
		s, err := readUint64(r)
		if err != nil {
			return "", nil, fmt.Errorf("remove seq %d: %w", i, err)
		}
		seqs[i] = s
	}
	return nodeID, seqs, nil
}

func putClocks(b *bytes.Buffer, clocks map[string]uint64) {
	ids := make([]string, 0, len(clocks))
	for id := range clocks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	putUint16(b, uint16(len(ids)))
	for _, id := range ids {
		putString16(b, id)
		putUint64(b, clocks[id])
	}
}

func readClocks(r io.Reader) (map[string]uint64, error) {
	n, err := readUint16(r)
	if err != nil {
		return nil, fmt.Errorf("clock count: %w", err)
	}
	if n == 0 {
		return nil, nil
	}
	clocks := make(map[string]uint64, n)
	for i := uint16(0); i < n; i++ {
		id, err := readString16(r)
		if err != nil {
			return nil, fmt.Errorf("clock %d id: %w", i, err)
		}
		ctr, err := readUint64(r)
		if err != nil {
			return nil, fmt.Errorf("clock %d counter: %w", i, err)
		}
		clocks[id] = ctr
	}
	return clocks, nil
}

// Fixed-width big-endian primitives. bytes.Buffer.Write never fails, so the
// writers omit error returns. String lengths are bounded by the field width;
// nodeIDs, keys, and clock ids fit in 16 bits, values in 32.

func putUint16(b *bytes.Buffer, v uint16) {
	var x [2]byte
	binary.BigEndian.PutUint16(x[:], v)
	b.Write(x[:])
}

func putUint32(b *bytes.Buffer, v uint32) {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], v)
	b.Write(x[:])
}

func putUint64(b *bytes.Buffer, v uint64) {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], v)
	b.Write(x[:])
}

func putString16(b *bytes.Buffer, s string) {
	putUint16(b, uint16(len(s)))
	b.WriteString(s)
}

func putString32(b *bytes.Buffer, s string) {
	putUint32(b, uint32(len(s)))
	b.WriteString(s)
}

func readUint16(r io.Reader) (uint16, error) {
	var x [2]byte
	if _, err := io.ReadFull(r, x[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(x[:]), nil
}

func readUint32(r io.Reader) (uint32, error) {
	var x [4]byte
	if _, err := io.ReadFull(r, x[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(x[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var x [8]byte
	if _, err := io.ReadFull(r, x[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(x[:]), nil
}

func readString16(r io.Reader) (string, error) {
	n, err := readUint16(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readString32(r io.Reader) (string, error) {
	n, err := readUint32(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
