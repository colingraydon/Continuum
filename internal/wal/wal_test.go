package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestOpen_EmptyDir(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if got := w.NextSeq(); got != 1 {
		t.Fatalf("NextSeq = %d, want 1", got)
	}
}

func TestAppend_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	seq, err := w.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 1 {
		t.Fatalf("Append seq = %d, want 1", seq)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	rec, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.Seq != 1 {
		t.Fatalf("rec.Seq = %d, want 1", rec.Seq)
	}
	if string(rec.Payload) != "hello" {
		t.Fatalf("rec.Payload = %q, want %q", rec.Payload, "hello")
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestAppend_MultipleRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 100 {
		if _, err := w.Append([]byte{byte(i)}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	for i := range 100 {
		rec, err := r.Next()
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if rec.Seq != uint64(i+1) {
			t.Fatalf("rec %d Seq = %d, want %d", i, rec.Seq, i+1)
		}
		if len(rec.Payload) != 1 || rec.Payload[0] != byte(i) {
			t.Fatalf("rec %d payload = %v", i, rec.Payload)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestAppend_EmptyAndLargePayloads(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := w.Append(nil); err != nil {
		t.Fatalf("Append empty: %v", err)
	}
	big := make([]byte, 100_000)
	for i := range big {
		big[i] = byte(i)
	}
	if _, err := w.Append(big); err != nil {
		t.Fatalf("Append big: %v", err)
	}
	w.Close()

	r, _ := NewReader(dir)
	defer r.Close()
	rec1, err := r.Next()
	if err != nil {
		t.Fatalf("Next empty: %v", err)
	}
	if len(rec1.Payload) != 0 {
		t.Fatalf("empty rec payload len = %d", len(rec1.Payload))
	}
	rec2, err := r.Next()
	if err != nil {
		t.Fatalf("Next big: %v", err)
	}
	if !bytes.Equal(rec2.Payload, big) {
		t.Fatalf("big payload mismatch (len got %d, want %d)", len(rec2.Payload), len(big))
	}
}

func TestReopen_ContinuesSequence(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	if _, err := w.Append([]byte("a")); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := w.Append([]byte("b")); err != nil {
		t.Fatalf("Append b: %v", err)
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := w2.NextSeq(); got != 3 {
		t.Fatalf("NextSeq after reopen = %d, want 3", got)
	}
	if _, err := w2.Append([]byte("c")); err != nil {
		t.Fatalf("Append c: %v", err)
	}
	w2.Close()

	r, _ := NewReader(dir)
	defer r.Close()
	var got []string
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, string(rec.Payload))
	}
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.SetMaxSegmentBytes(100) // small enough to force rotation
	const n = 20
	for range n {
		if _, err := w.Append([]byte("aaaaaaaa")); err != nil { // 24-byte frames
			t.Fatalf("Append: %v", err)
		}
	}
	w.Close()

	segs, _ := listSegments(dir)
	if len(segs) < 2 {
		t.Fatalf("expected multiple segments, got %d: %v", len(segs), segs)
	}

	r, _ := NewReader(dir)
	defer r.Close()
	count := 0
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		count++
		if rec.Seq != uint64(count) {
			t.Fatalf("seq gap at record %d: got %d", count, rec.Seq)
		}
	}
	if count != n {
		t.Fatalf("expected %d records, got %d", n, count)
	}
}

func TestTornTail_TruncatedByWriterOpen(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Append([]byte("first"))
	w.Append([]byte("second"))
	w.Close()

	// Simulate a torn write by appending a partial frame to the segment.
	segs, _ := listSegments(dir)
	f, err := os.OpenFile(segs[0], os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 99}); err != nil { // partial header
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := w2.NextSeq(); got != 3 {
		t.Fatalf("NextSeq after torn tail = %d, want 3", got)
	}
	w2.Append([]byte("third"))
	w2.Close()

	r, _ := NewReader(dir)
	defer r.Close()
	var got []string
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, string(rec.Payload))
	}
	want := []string{"first", "second", "third"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if r.TornTail() {
		t.Fatalf("TornTail should be false after writer truncation")
	}
}

func TestTornTail_ReaderReports(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Append([]byte("first"))
	w.Close()

	segs, _ := listSegments(dir)
	f, err := os.OpenFile(segs[0], os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	f.Write([]byte{0, 0, 0, 99}) // partial header
	f.Close()

	r, _ := NewReader(dir)
	defer r.Close()
	if _, err := r.Next(); err != nil {
		t.Fatalf("Next first: %v", err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if !r.TornTail() {
		t.Fatalf("expected TornTail")
	}
}

func TestMidStreamCRCFailure_NonNewestSegment(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.SetMaxSegmentBytes(50) // force frequent rotation
	for range 6 {
		w.Append([]byte("aaaaaaaa"))
	}
	w.Close()

	segs, _ := listSegments(dir)
	if len(segs) < 2 {
		t.Fatalf("need at least 2 segments, got %d", len(segs))
	}
	// Corrupt a byte in the first segment's first record's payload.
	f, err := os.OpenFile(segs[0], os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Seek(headerSize+1, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()

	r, _ := NewReader(dir)
	defer r.Close()
	if _, err := r.Next(); !errors.Is(err, errCRC) {
		t.Fatalf("expected errCRC, got %v", err)
	}
}

func TestTruncateThrough(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.SetMaxSegmentBytes(50)
	for range 6 {
		w.Append([]byte("aaaaaaaa"))
	}
	before, _ := listSegments(dir)
	if len(before) < 2 {
		t.Fatalf("need at least 2 segments, got %d", len(before))
	}

	lastStart, err := segmentStartSeq(before[len(before)-1])
	if err != nil {
		t.Fatalf("segmentStartSeq: %v", err)
	}
	if err := w.TruncateThrough(lastStart - 1); err != nil {
		t.Fatalf("TruncateThrough: %v", err)
	}

	after, _ := listSegments(dir)
	if len(after) != 1 {
		t.Fatalf("expected 1 segment after truncate, got %d: %v", len(after), after)
	}
	w.Close()
}

func TestTruncateThrough_PreservesCurrentSegment(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	for range 3 {
		w.Append([]byte("x"))
	}
	// Even with snapshotSeq past everything, the open segment must remain.
	if err := w.TruncateThrough(1_000_000); err != nil {
		t.Fatalf("TruncateThrough: %v", err)
	}
	after, _ := listSegments(dir)
	if len(after) != 1 {
		t.Fatalf("expected 1 segment to remain, got %d", len(after))
	}
	w.Close()
}

func TestClose_Idempotent(t *testing.T) {
	w, _ := Open(t.TempDir())
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSync_AfterClose(t *testing.T) {
	w, _ := Open(t.TempDir())
	w.Close()
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync after Close: %v", err)
	}
}

func TestOpen_RecoversFromEmptySegment(t *testing.T) {
	// Simulate a Writer that created a fresh segment file but crashed before
	// the first Append. The segment name carries the expected start seq.
	dir := t.TempDir()
	name := filepath.Join(dir, segmentName(42))
	if err := os.WriteFile(name, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := w.NextSeq(); got != 42 {
		t.Fatalf("NextSeq = %d, want 42", got)
	}
	if _, err := w.Append([]byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()
}

func TestOpen_MissingDirCreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "subdir", "wal")
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.Close()
}

func TestNewReader_MissingDir(t *testing.T) {
	_, err := NewReader(filepath.Join(t.TempDir(), "nope"))
	if err == nil {
		t.Fatalf("expected error for missing dir")
	}
}

func TestNewReader_EmptyDir(t *testing.T) {
	r, err := NewReader(t.TempDir())
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReader_CloseIdempotent(t *testing.T) {
	r, _ := NewReader(t.TempDir())
	if err := r.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestTruncateThrough_StopsAtSnapshotBoundary(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.SetMaxSegmentBytes(50)
	for range 6 {
		w.Append([]byte("aaaaaaaa"))
	}
	before, _ := listSegments(dir)
	if len(before) < 3 {
		t.Fatalf("need at least 3 segments, got %d", len(before))
	}
	// snapshotSeq before the start of segs[1] → only segs[0] eligible.
	firstStart, _ := segmentStartSeq(before[1])
	if err := w.TruncateThrough(firstStart - 1); err != nil {
		t.Fatalf("TruncateThrough: %v", err)
	}
	after, _ := listSegments(dir)
	if len(after) != len(before)-1 {
		t.Fatalf("expected %d segments after truncate, got %d", len(before)-1, len(after))
	}
	w.Close()
}

func TestTruncateThrough_SingleSegmentNoOp(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Append([]byte("x"))
	if err := w.TruncateThrough(999); err != nil {
		t.Fatalf("TruncateThrough: %v", err)
	}
	segs, _ := listSegments(dir)
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	w.Close()
}

func TestSegmentStartSeq_BadName(t *testing.T) {
	if _, err := segmentStartSeq("/tmp/garbage.txt"); err == nil {
		t.Fatalf("expected error for bad suffix")
	}
	if _, err := segmentStartSeq("/tmp/notanumber.wal"); err == nil {
		t.Fatalf("expected error for non-numeric name")
	}
}

func TestListSegments_FiltersNonWALAndDirs(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, segmentName(1)), nil, 0o644)
	os.WriteFile(filepath.Join(dir, "notes.txt"), nil, 0o644)
	os.Mkdir(filepath.Join(dir, "subdir"), 0o755)
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(segs) != 1 || !strings.HasSuffix(segs[0], segmentName(1)) {
		t.Fatalf("expected just the .wal file, got %v", segs)
	}
}

func TestReadFrame_BadLength(t *testing.T) {
	// Construct a frame with length < 8 (invalid: must cover at least seq).
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[4:8], 4) // length < 8
	binary.BigEndian.PutUint64(hdr[8:16], 1)
	crc := crc32.ChecksumIEEE(hdr[4:])
	binary.BigEndian.PutUint32(hdr[0:4], crc)
	_, err := readFrame(bytes.NewReader(hdr[:]))
	if err == nil || !strings.Contains(err.Error(), "invalid length") {
		t.Fatalf("expected invalid length, got %v", err)
	}
}

func TestReadFrame_TruncatedPayload(t *testing.T) {
	// Header claims a 16-byte payload but only 5 bytes follow.
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[4:8], 8+16)
	binary.BigEndian.PutUint64(hdr[8:16], 1)
	crc := crc32.ChecksumIEEE(hdr[4:])
	binary.BigEndian.PutUint32(hdr[0:4], crc)
	r := bytes.NewReader(append(hdr[:], []byte("12345")...))
	_, err := readFrame(r)
	if !errors.Is(err, errTornTail) {
		t.Fatalf("expected errTornTail, got %v", err)
	}
}

func TestOpen_BadSegmentName(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "xyz.wal"), nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatalf("expected Open to fail on unparseable segment name")
	}
}

func TestTruncateThrough_BadSegmentNameInDir(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := w.Append([]byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Drop a malformed .wal file in the dir (sorts after the good segment).
	if err := os.WriteFile(filepath.Join(dir, "zzz_bad.wal"), nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := w.TruncateThrough(1); err == nil {
		t.Fatalf("expected TruncateThrough to fail on bad segment name")
	}
	w.Close()
}

type failingReader struct{ err error }

func (f *failingReader) Read(p []byte) (int, error) { return 0, f.err }

func TestReadFrame_HeaderReadError(t *testing.T) {
	if _, err := readFrame(&failingReader{err: errors.New("boom")}); err == nil ||
		!strings.Contains(err.Error(), "read header") {
		t.Fatalf("expected read header error, got %v", err)
	}
}

func TestReadFrame_PayloadReadError(t *testing.T) {
	// Header reads cleanly, then payload Read returns a non-EOF error.
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[4:8], 8+16)
	binary.BigEndian.PutUint64(hdr[8:16], 1)
	crc := crc32.ChecksumIEEE(hdr[4:])
	binary.BigEndian.PutUint32(hdr[0:4], crc)
	r := io.MultiReader(bytes.NewReader(hdr[:]), &failingReader{err: errors.New("disk error")})
	if _, err := readFrame(r); err == nil || !strings.Contains(err.Error(), "read payload") {
		t.Fatalf("expected read payload error, got %v", err)
	}
}

func TestOpen_MkdirFails(t *testing.T) {
	// mkdir fails when a regular file blocks the path.
	parent := t.TempDir()
	blocker := filepath.Join(parent, "blocker")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := Open(filepath.Join(blocker, "sub")); err == nil {
		t.Fatalf("expected mkdir error")
	}
}

func TestReader_NextFailsWhenSegmentDisappears(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Append([]byte("x"))
	w.Close()

	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	segs, _ := listSegments(dir)
	os.Remove(segs[0])
	if _, err := r.Next(); err == nil {
		t.Fatalf("expected open error after segment removed")
	}
}

func TestScanSegment_OpenFails(t *testing.T) {
	if _, _, err := scanSegment("/nonexistent/path/file.wal"); err == nil {
		t.Fatalf("expected open error")
	}
}

func TestOpen_EmptyLastSegment(t *testing.T) {
	dir := t.TempDir()
	// Seed one valid record so the first segment has a known lastSeq.
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := w.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	nextSeq := w.NextSeq() // 2 after one append
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Add a second, empty segment — simulates a crash after rotation but before any write.
	emptyPath := filepath.Join(dir, segmentName(nextSeq))
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	// Open must succeed and derive nextSeq from the empty segment's filename.
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open with empty last segment: %v", err)
	}
	defer w2.Close()
	if got := w2.NextSeq(); got != nextSeq {
		t.Fatalf("NextSeq: got %d, want %d", got, nextSeq)
	}
}

func TestRotation_StartSegmentFails(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	if _, err := w.Append([]byte("x")); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	// Pre-place a directory where the next rotated segment file would land,
	// so the EXCL create fails.
	if err := os.Mkdir(filepath.Join(dir, segmentName(2)), 0o755); err != nil {
		t.Fatalf("Mkdir blocker: %v", err)
	}
	w.SetMaxSegmentBytes(1) // any further Append triggers rotate
	if _, err := w.Append([]byte("y")); err == nil {
		t.Fatalf("expected rotation error")
	}
}

func TestConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	const n = 100
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := w.Append([]byte{byte(i)}); err != nil {
				t.Errorf("Append: %v", err)
			}
		}(i)
	}
	wg.Wait()
	w.Close()

	r, _ := NewReader(dir)
	defer r.Close()
	seqs := map[uint64]bool{}
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		seqs[rec.Seq] = true
	}
	if len(seqs) != n {
		t.Fatalf("expected %d unique seqs, got %d", n, len(seqs))
	}
	for i := 1; i <= n; i++ {
		if !seqs[uint64(i)] {
			t.Fatalf("missing seq %d", i)
		}
	}
}

func TestSyncUpTo_MakesRecordsDurable(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	defer w.Close()
	var lastSeq uint64
	for i := range 5 {
		seq, err := w.Append([]byte{byte(i)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lastSeq = seq
	}
	if err := w.SyncUpTo(lastSeq); err != nil {
		t.Fatalf("SyncUpTo: %v", err)
	}
	if got := w.FsyncCount(); got != 1 {
		t.Fatalf("FsyncCount = %d, want 1", got)
	}
	// A second call covering an already-durable seq must not fsync again.
	if err := w.SyncUpTo(lastSeq); err != nil {
		t.Fatalf("SyncUpTo (redundant): %v", err)
	}
	if got := w.FsyncCount(); got != 1 {
		t.Fatalf("redundant SyncUpTo fsynced: count = %d, want 1", got)
	}
}

func TestSyncUpTo_ZeroTargetIsNoOp(t *testing.T) {
	w, _ := Open(t.TempDir())
	defer w.Close()
	if err := w.SyncUpTo(0); err != nil {
		t.Fatalf("SyncUpTo(0): %v", err)
	}
	if got := w.FsyncCount(); got != 0 {
		t.Fatalf("FsyncCount = %d, want 0", got)
	}
}

// TestSyncUpTo_BatchesConcurrentCallers asserts the core group-commit property:
// many writers that append then SyncUpTo collapse into far fewer fsyncs than
// the number of records.
func TestSyncUpTo_BatchesConcurrentCallers(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	defer w.Close()

	const n = 200
	// Pre-append everything so every SyncUpTo target is already buffered; the
	// first leader's single fsync then covers all concurrent followers.
	seqs := make([]uint64, n)
	for i := range n {
		seq, err := w.Append([]byte{byte(i)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		seqs[i] = seq
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(target uint64) {
			defer wg.Done()
			if err := w.SyncUpTo(target); err != nil {
				t.Errorf("SyncUpTo: %v", err)
			}
		}(seqs[i])
	}
	wg.Wait()

	// Worst case every goroutine could lead its own fsync if perfectly
	// serialized, but in practice a leader's flush covers the rest. Require at
	// least real batching: far fewer fsyncs than records.
	if got := w.FsyncCount(); got == 0 || got >= n {
		t.Fatalf("FsyncCount = %d, want batched (0 < count < %d)", got, n)
	}
}

func TestSyncUpTo_DurableAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	defer w.Close()
	w.SetMaxSegmentBytes(50) // force frequent rotation

	var seqs []uint64
	for range 20 {
		seq, err := w.Append([]byte("aaaaaaaa"))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		seqs = append(seqs, seq)
	}
	// Syncing an early seq whose segment has since rotated away must still
	// report success (rotateLocked fsynced it before closing).
	if err := w.SyncUpTo(seqs[0]); err != nil {
		t.Fatalf("SyncUpTo across rotation: %v", err)
	}
	if err := w.SyncUpTo(seqs[len(seqs)-1]); err != nil {
		t.Fatalf("SyncUpTo latest: %v", err)
	}
}

func TestSync_RoutesThroughGroupCommit(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	defer w.Close()
	if _, err := w.Append([]byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := w.FsyncCount(); got != 1 {
		t.Fatalf("FsyncCount = %d, want 1", got)
	}
}

// TestSyncUpTo_FsyncErrorSurfaces closes the underlying file out from under the
// writer (no rotation) so the next fsync fails, exercising the error path that
// caches and returns the failure to the caller.
func TestSyncUpTo_FsyncErrorSurfaces(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	if _, err := w.Append([]byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.f.Close(); err != nil {
		t.Fatalf("close underlying file: %v", err)
	}
	if err := w.SyncUpTo(1); err == nil {
		t.Fatalf("expected fsync error to surface")
	}
	if got := w.FsyncCount(); got != 1 {
		t.Fatalf("FsyncCount = %d, want 1 (the failed attempt is still counted)", got)
	}
}
