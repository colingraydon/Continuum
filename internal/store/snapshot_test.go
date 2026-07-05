package store

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func newTestVC(clocks map[string]uint64) VectorClockVersion {
	c := make(map[string]uint64, len(clocks))
	for k, v := range clocks {
		c[k] = v
	}
	return VectorClockVersion{Clocks: c}
}

func populate(t *testing.T, s *Store) {
	t.Helper()
	// Plain live key.
	s.Put("alpha", "one", newTestVC(map[string]uint64{"a": 1}))
	// Two concurrent siblings on the same key.
	s.Put("beta", "left", newTestVC(map[string]uint64{"a": 1}))
	s.Put("beta", "right", newTestVC(map[string]uint64{"b": 1}))
	// Tombstone — should produce an entry plus tombstoneAges record.
	s.Delete("gamma", newTestVC(map[string]uint64{"a": 1}))
	// Live key with multiple clock entries.
	s.Put("delta", "hello world", newTestVC(map[string]uint64{"a": 5, "b": 3, "c": 9}))
}

func TestSnapshot_RoundTrip(t *testing.T) {
	src := New()
	populate(t, src)

	var buf bytes.Buffer
	hdr := SnapHeader{NodeID: "node-A", Epoch: 7, SequenceAt: 42}
	if err := src.Snapshot(&buf, hdr); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	dst := New()
	got, err := dst.LoadSnapshot(&buf, "node-A")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got.NodeID != "node-A" || got.Epoch != 7 || got.SequenceAt != 42 {
		t.Fatalf("header round-trip mismatch: %+v", got)
	}
	if got.EntryCount != uint64(len(memData(src))) {
		t.Fatalf("EntryCount = %d, want %d", got.EntryCount, len(memData(src)))
	}
	assertStoresEqual(t, src, dst)
}

func TestSnapshot_EmptyStore(t *testing.T) {
	src := New()
	var buf bytes.Buffer
	if err := src.Snapshot(&buf, SnapHeader{NodeID: "n"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := New()
	hdr, err := dst.LoadSnapshot(&buf, "n")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if hdr.EntryCount != 0 {
		t.Fatalf("EntryCount = %d, want 0", hdr.EntryCount)
	}
	if len(memData(dst)) != 0 || len(memAges(dst)) != 0 {
		t.Fatalf("expected empty dst, got %d data / %d ages", len(memData(dst)), len(memAges(dst)))
	}
}

func TestSnapshot_IdentityMismatch(t *testing.T) {
	src := New()
	populate(t, src)
	var buf bytes.Buffer
	if err := src.Snapshot(&buf, SnapHeader{NodeID: "node-A"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := New()
	_, err := dst.LoadSnapshot(&buf, "node-B")
	if err == nil || !strings.Contains(err.Error(), "node id mismatch") {
		t.Fatalf("expected node id mismatch, got %v", err)
	}
}

func TestSnapshot_BadMagic(t *testing.T) {
	src := New()
	populate(t, src)
	var buf bytes.Buffer
	src.Snapshot(&buf, SnapHeader{NodeID: "n"})
	data := buf.Bytes()
	data[0] = 'X' // corrupt magic
	dst := New()
	_, err := dst.LoadSnapshot(bytes.NewReader(data), "n")
	if err == nil || !strings.Contains(err.Error(), "bad magic") {
		t.Fatalf("expected bad magic, got %v", err)
	}
}

func TestSnapshot_HeaderCRCFailure(t *testing.T) {
	src := New()
	populate(t, src)
	var buf bytes.Buffer
	src.Snapshot(&buf, SnapHeader{NodeID: "n"})
	data := buf.Bytes()
	// Flip a byte inside the header's variable region (node_id).
	// The fixed prefix is 32 bytes; node id starts at offset 32.
	data[32] = data[32] ^ 0xFF
	dst := New()
	_, err := dst.LoadSnapshot(bytes.NewReader(data), "")
	if err == nil || !strings.Contains(err.Error(), "crc") {
		t.Fatalf("expected header crc failure, got %v", err)
	}
}

func TestSnapshot_BodyCRCFailure(t *testing.T) {
	src := New()
	populate(t, src)
	var buf bytes.Buffer
	src.Snapshot(&buf, SnapHeader{NodeID: "n"})
	data := buf.Bytes()
	// Flip a byte near the end of the buffer but before the trailing crc.
	// The body crc is the last 4 bytes; corrupt one byte just before it.
	if len(data) < 10 {
		t.Fatalf("snapshot too small: %d", len(data))
	}
	target := len(data) - 8
	data[target] = data[target] ^ 0xFF
	dst := New()
	_, err := dst.LoadSnapshot(bytes.NewReader(data), "")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestSnapshot_TruncatedBody(t *testing.T) {
	src := New()
	populate(t, src)
	var buf bytes.Buffer
	src.Snapshot(&buf, SnapHeader{NodeID: "n"})
	data := buf.Bytes()
	truncated := data[:len(data)-20] // cut into the body
	dst := New()
	_, err := dst.LoadSnapshot(bytes.NewReader(truncated), "")
	if err == nil {
		t.Fatalf("expected error on truncated snapshot, got nil")
	}
}

func TestSnapshot_TombstoneAgePreserved(t *testing.T) {
	src := New()
	src.Delete("k", newTestVC(map[string]uint64{"a": 1}))

	var buf bytes.Buffer
	if err := src.Snapshot(&buf, SnapHeader{NodeID: "n"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	srcAge := memAges(src)["k"]
	dst := New()
	if _, err := dst.LoadSnapshot(&buf, "n"); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	dstAge, ok := memAges(dst)["k"]
	if !ok {
		t.Fatalf("tombstone age missing after load")
	}
	if !dstAge.Equal(srcAge) {
		t.Fatalf("tombstone age changed: src=%v dst=%v", srcAge, dstAge)
	}
}

func TestSnapshot_GCTombstonesUsesLoadedAges(t *testing.T) {
	// A tombstone whose age survives a round-trip should still be GC-eligible
	// when its age exceeds the TTL.
	src := New()
	src.Delete("k", newTestVC(map[string]uint64{"a": 1}))
	// Force the age into the past.
	src.SetTombstoneAge("k", time.Now().Add(-2*time.Hour))

	var buf bytes.Buffer
	if err := src.Snapshot(&buf, SnapHeader{NodeID: "n"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := New()
	if _, err := dst.LoadSnapshot(&buf, "n"); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	purged, _ := dst.GCTombstones(time.Hour)
	if len(purged) != 1 || purged[0] != "k" {
		t.Fatalf("expected k to be GC'd, got %v", purged)
	}
}

func TestSnapshot_HeaderTooShort(t *testing.T) {
	dst := New()
	if _, err := dst.LoadSnapshot(bytes.NewReader([]byte("CON")), ""); err == nil {
		t.Fatalf("expected short-header error")
	}
}

func TestSnapshot_NodeIDTruncated(t *testing.T) {
	src := New()
	var buf bytes.Buffer
	if err := src.Snapshot(&buf, SnapHeader{NodeID: "node-A"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Truncate after the 32-byte fixed prefix — the node_id read should fail.
	data := buf.Bytes()
	dst := New()
	if _, err := dst.LoadSnapshot(bytes.NewReader(data[:32]), ""); err == nil {
		t.Fatalf("expected truncated node-id error")
	}
}

func TestSnapshot_HeaderCRCTruncated(t *testing.T) {
	src := New()
	var buf bytes.Buffer
	src.Snapshot(&buf, SnapHeader{NodeID: "n"})
	data := buf.Bytes()
	// 32 prefix + 1 node-id byte = 33; CRC starts at 33. Truncate inside the CRC.
	dst := New()
	if _, err := dst.LoadSnapshot(bytes.NewReader(data[:35]), ""); err == nil {
		t.Fatalf("expected truncated crc error")
	}
}

func TestSnapshot_UnsupportedVersion(t *testing.T) {
	src := New()
	var buf bytes.Buffer
	src.Snapshot(&buf, SnapHeader{NodeID: "n"})
	data := buf.Bytes()
	// version is uint16 at offset 4-5; bump to 99.
	data[4] = 0
	data[5] = 99
	dst := New()
	if _, err := dst.LoadSnapshot(bytes.NewReader(data), ""); err == nil ||
		!strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("expected unsupported version, got %v", err)
	}
}

func TestSnapshot_BodyCRCTruncated(t *testing.T) {
	src := New()
	populate(t, src)
	var buf bytes.Buffer
	src.Snapshot(&buf, SnapHeader{NodeID: "n"})
	data := buf.Bytes()
	dst := New()
	if _, err := dst.LoadSnapshot(bytes.NewReader(data[:len(data)-2]), ""); err == nil {
		t.Fatalf("expected truncated body-crc error")
	}
}

func TestWriteSnapHeader_NodeIDTooLong(t *testing.T) {
	err := writeSnapHeader(&bytes.Buffer{}, SnapHeader{NodeID: strings.Repeat("x", 1<<16)})
	if err == nil || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("expected too-long error, got %v", err)
	}
}

func TestWriteLenString16_TooLong(t *testing.T) {
	err := writeLenString16(&bytes.Buffer{}, strings.Repeat("a", 1<<16))
	if err == nil || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("expected too-long error, got %v", err)
	}
}

// failingWriter returns an error after budget bytes have been accepted.
type failingWriter struct {
	written int
	budget  int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	remaining := f.budget - f.written
	if remaining <= 0 {
		return 0, errors.New("write failed")
	}
	if len(p) <= remaining {
		f.written += len(p)
		return len(p), nil
	}
	f.written += remaining
	return remaining, errors.New("write failed")
}

func TestSnapshot_WriteErrorAtHeader(t *testing.T) {
	src := New()
	if err := src.Snapshot(&failingWriter{budget: 0}, SnapHeader{NodeID: "n"}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSnapshot_WriteErrorMidBody(t *testing.T) {
	src := New()
	populate(t, src)
	// Header + a few body bytes then fail.
	if err := src.Snapshot(&failingWriter{budget: 50}, SnapHeader{NodeID: "n"}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSnapshot_WriteErrorAtBodyCRC(t *testing.T) {
	src := New()
	populate(t, src)
	// Compute the full snapshot size so we can fail exactly at the trailing crc.
	var sizing bytes.Buffer
	if err := src.Snapshot(&sizing, SnapHeader{NodeID: "n"}); err != nil {
		t.Fatalf("sizing snapshot: %v", err)
	}
	if err := src.Snapshot(&failingWriter{budget: sizing.Len() - 1}, SnapHeader{NodeID: "n"}); err == nil {
		t.Fatalf("expected error at body crc write")
	}
}

func TestSnapshot_LoadHandlesMixedSiblings(t *testing.T) {
	// Build an entry with one live and one deleted sibling (concurrent).
	src := New()
	src.Put("k", "v", newTestVC(map[string]uint64{"a": 1}))
	src.Delete("k", newTestVC(map[string]uint64{"b": 1}))
	if got := len(memData(src)["k"].Siblings); got != 2 {
		t.Fatalf("setup: want 2 siblings, got %d", got)
	}

	var buf bytes.Buffer
	if err := src.Snapshot(&buf, SnapHeader{NodeID: "n"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := New()
	if _, err := dst.LoadSnapshot(&buf, "n"); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	assertStoresEqual(t, src, dst)
}

func TestWriteHelpers_PropagateWriteErrors(t *testing.T) {
	cases := map[string]func(io.Writer) error{
		"writeUint16":      func(w io.Writer) error { return writeUint16(w, 1) },
		"writeUint64":      func(w io.Writer) error { return writeUint64(w, 1) },
		"writeLenString16": func(w io.Writer) error { return writeLenString16(w, "abc") },
		"writeLenString32": func(w io.Writer) error { return writeLenString32(w, "abc") },
	}
	for name, fn := range cases {
		if err := fn(&failingWriter{}); err == nil {
			t.Fatalf("%s: expected error on failingWriter", name)
		}
	}
}

func TestReadHelpers_HandleShortReads(t *testing.T) {
	cases := map[string]struct {
		input []byte
		read  func(io.Reader) error
	}{
		"readUint16 short": {
			[]byte{1},
			func(r io.Reader) error { _, err := readUint16(r); return err },
		},
		"readUint64 short": {
			[]byte{1, 2},
			func(r io.Reader) error { _, err := readUint64(r); return err },
		},
		"readLenString16 short length": {
			[]byte{0},
			func(r io.Reader) error { _, err := readLenString16(r); return err },
		},
		"readLenString16 short body": {
			[]byte{0, 5, 'a'},
			func(r io.Reader) error { _, err := readLenString16(r); return err },
		},
		"readLenString32 short length": {
			[]byte{0, 0, 0},
			func(r io.Reader) error { _, err := readLenString32(r); return err },
		},
		"readLenString32 short body": {
			[]byte{0, 0, 0, 5, 'a'},
			func(r io.Reader) error { _, err := readLenString32(r); return err },
		},
	}
	for name, c := range cases {
		if err := c.read(bytes.NewReader(c.input)); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestWriteEntry_BruteForceBudgets(t *testing.T) {
	// Vary the failingWriter budget so every write-error branch in writeEntry
	// fires at least once.
	e := Entry{Siblings: []Sibling{
		{Value: "v", Version: VectorClockVersion{Clocks: map[string]uint64{"a": 1}}},
		{Value: "w", Deleted: true, Version: VectorClockVersion{Clocks: map[string]uint64{"b": 2}}},
	}}
	ages := map[string]time.Time{"k": time.Now()}
	var sizing bytes.Buffer
	if err := writeEntry(&sizing, "k", e, ages); err != nil {
		t.Fatalf("sizing writeEntry: %v", err)
	}
	for budget := 0; budget < sizing.Len(); budget++ {
		err := writeEntry(&failingWriter{budget: budget}, "k", e, ages)
		if err == nil {
			t.Fatalf("budget %d (total %d): expected error", budget, sizing.Len())
		}
	}
}

func TestReadEntry_BruteForceTruncations(t *testing.T) {
	// Encode a representative entry, then feed every truncated prefix to
	// readEntry. Each cut should produce some error.
	e := Entry{Siblings: []Sibling{
		{Value: "v", Version: VectorClockVersion{Clocks: map[string]uint64{"a": 1}}},
		{Value: "w", Deleted: true, Version: VectorClockVersion{Clocks: map[string]uint64{"b": 2}}},
	}}
	ages := map[string]time.Time{"k": time.Now()}
	var buf bytes.Buffer
	if err := writeEntry(&buf, "k", e, ages); err != nil {
		t.Fatalf("writeEntry: %v", err)
	}
	full := buf.Bytes()
	for cut := 0; cut < len(full); cut++ {
		if _, _, _, _, err := readEntry(bytes.NewReader(full[:cut])); err == nil {
			t.Fatalf("cut %d: expected error", cut)
		}
	}
}

// memDataLocked materializes the active memtable's live (non-evicted) entries.
// Caller holds s.mu.
func memDataLocked(s *Store) map[string]Entry {
	out := make(map[string]Entry)
	for it := s.mem.iter(); it.next(); {
		if v := it.value(); !v.evicted {
			out[it.key()] = v.entry
		}
	}
	return out
}

// memAgesLocked materializes the active memtable's tombstone ages. Caller holds
// s.mu.
func memAgesLocked(s *Store) map[string]time.Time {
	out := make(map[string]time.Time)
	for it := s.mem.iter(); it.next(); {
		if v := it.value(); !v.evicted && !v.age.IsZero() {
			out[it.key()] = v.age
		}
	}
	return out
}

func memData(s *Store) map[string]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return memDataLocked(s)
}

func memAges(s *Store) map[string]time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return memAgesLocked(s)
}

// assertStoresEqual checks both stores contain the same logical entries.
// Sibling order within an Entry is not guaranteed, so we match by content.
func assertStoresEqual(t *testing.T, a, b *Store) {
	t.Helper()
	a.mu.RLock()
	defer a.mu.RUnlock()
	b.mu.RLock()
	defer b.mu.RUnlock()
	aData, bData := memDataLocked(a), memDataLocked(b)
	if len(aData) != len(bData) {
		t.Fatalf("data len mismatch: %d vs %d", len(aData), len(bData))
	}
	for k, ae := range aData {
		be, ok := bData[k]
		if !ok {
			t.Fatalf("key %q missing from dst", k)
		}
		if !entryEqual(ae, be) {
			t.Fatalf("entry %q differs:\nA: %+v\nB: %+v", k, ae, be)
		}
	}
	aAges, bAges := memAgesLocked(a), memAgesLocked(b)
	if len(aAges) != len(bAges) {
		t.Fatalf("ages len mismatch: %d vs %d", len(aAges), len(bAges))
	}
	for k, at := range aAges {
		bt, ok := bAges[k]
		if !ok {
			t.Fatalf("tombstone age for %q missing from dst", k)
		}
		if !at.Equal(bt) {
			t.Fatalf("tombstone age for %q differs: %v vs %v", k, at, bt)
		}
	}
}

func entryEqual(a, b Entry) bool {
	if len(a.Siblings) != len(b.Siblings) {
		return false
	}
	for _, sa := range a.Siblings {
		found := false
		for _, sb := range b.Siblings {
			if sa.Value == sb.Value && sa.Deleted == sb.Deleted && sa.Version.Equal(sb.Version) && sa.Hash == sb.Hash {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
