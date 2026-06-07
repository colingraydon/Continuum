// Package wal implements an append-only segmented write-ahead log with
// CRC-checked records and sequence numbers. Records are opaque byte
// payloads; the encoding of payload contents (record type, fields) is the
// caller's responsibility.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DefaultMaxSegmentBytes is the rotation threshold for new segments.
const DefaultMaxSegmentBytes int64 = 64 * 1024 * 1024

const (
	headerSize    = 16 // crc:4 | len:4 | seq:8
	segmentSuffix = ".wal"
)

// On-disk frame layout:
//
//   [crc32:4][len:4][seq:8][payload:len-8]
//
// CRC is computed over len | seq | payload. len covers seq + payload, so
// total record size on disk is 8 + len bytes.

// Record is a single sequenced log entry.
type Record struct {
	Seq     uint64
	Payload []byte
}

// Writer is an append-only segmented WAL. Append buffers a record and Sync
// fsyncs the current segment. Append and Sync are safe to call from
// multiple goroutines but are serialized internally.
type Writer struct {
	mu              sync.Mutex
	dir             string
	maxSegmentBytes int64
	f               *os.File
	segmentSize     int64
	nextSeq         uint64
}

// Open opens (or creates) a WAL in dir. If the newest segment ends with a
// torn record it is truncated to the last valid record before the writer
// is opened for append. The next sequence number is one past the last
// durable record (or the newest segment's starting seq if that segment is
// empty).
func Open(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	w := &Writer{dir: dir, maxSegmentBytes: DefaultMaxSegmentBytes, nextSeq: 1}
	if len(segs) == 0 {
		if err := w.startSegment(1); err != nil {
			return nil, err
		}
		return w, nil
	}
	newest := segs[len(segs)-1]
	lastSeq, validEnd, err := scanSegment(newest)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(newest, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open segment: %w", err)
	}
	if err := f.Truncate(validEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: truncate torn tail: %w", err)
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: seek: %w", err)
	}
	w.f = f
	w.segmentSize = validEnd
	if lastSeq > 0 {
		w.nextSeq = lastSeq + 1
	} else {
		startSeq, err := segmentStartSeq(newest)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		w.nextSeq = startSeq
	}
	return w, nil
}

// SetMaxSegmentBytes overrides the rotation threshold. Intended for tests.
func (w *Writer) SetMaxSegmentBytes(n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.maxSegmentBytes = n
}

// Append writes payload with the next sequence number. The record is in
// the OS write buffer after this returns; call Sync to make it durable.
func (w *Writer) Append(payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	seq := w.nextSeq
	frame := encodeFrame(seq, payload)
	if w.segmentSize > 0 && w.segmentSize+int64(len(frame)) > w.maxSegmentBytes {
		if err := w.rotateLocked(seq); err != nil {
			return 0, err
		}
	}
	if _, err := w.f.Write(frame); err != nil {
		return 0, fmt.Errorf("wal: write: %w", err)
	}
	w.segmentSize += int64(len(frame))
	w.nextSeq++
	return seq, nil
}

// Sync fsyncs the current segment.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	return w.f.Sync()
}

// Close fsyncs and closes the current segment.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	syncErr := w.f.Sync()
	closeErr := w.f.Close()
	w.f = nil
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// NextSeq returns the sequence number the next Append will assign.
func (w *Writer) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

// TruncateThrough removes segments whose records are entirely covered by a
// snapshot at snapshotSeq. The currently-open segment is never deleted.
func (w *Writer) TruncateThrough(snapshotSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	if len(segs) <= 1 {
		return nil
	}
	openPath := ""
	if w.f != nil {
		openPath = w.f.Name()
	}
	// Sequence numbers are contiguous across segments by construction, so
	// segment i ends at seq(segment i+1) - 1.
	for i := 0; i < len(segs)-1; i++ {
		nextStart, err := segmentStartSeq(segs[i+1])
		if err != nil {
			return err
		}
		if nextStart-1 > snapshotSeq {
			break
		}
		if segs[i] == openPath {
			continue
		}
		if err := os.Remove(segs[i]); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: remove %s: %w", segs[i], err)
		}
	}
	return nil
}

func (w *Writer) rotateLocked(startSeq uint64) error {
	if w.f != nil {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("wal: sync before rotate: %w", err)
		}
		if err := w.f.Close(); err != nil {
			return fmt.Errorf("wal: close before rotate: %w", err)
		}
	}
	return w.startSegment(startSeq)
}

func (w *Writer) startSegment(startSeq uint64) error {
	path := filepath.Join(w.dir, segmentName(startSeq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	w.f = f
	w.segmentSize = 0
	return nil
}

// Reader iterates records across all segments in a directory. Not safe for
// concurrent use.
type Reader struct {
	segments []string
	idx      int
	f        *os.File
	tornTail bool
}

// NewReader opens a Reader over dir. If dir contains no segments the first
// Next returns io.EOF.
func NewReader(dir string) (*Reader, error) {
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	return &Reader{segments: segs, idx: -1}, nil
}

// Next returns the next record or io.EOF at the end. If a torn record is
// detected at the tail of the newest segment, iteration ends cleanly with
// io.EOF and TornTail returns true. Reopening a Writer on the same
// directory truncates the torn tail. A CRC failure mid-stream (i.e. in any
// segment other than the newest, or before its end) is reported as an
// error.
func (r *Reader) Next() (Record, error) {
	for {
		if r.f == nil {
			r.idx++
			if r.idx >= len(r.segments) {
				return Record{}, io.EOF
			}
			f, err := os.Open(r.segments[r.idx])
			if err != nil {
				return Record{}, fmt.Errorf("wal: open %s: %w", r.segments[r.idx], err)
			}
			r.f = f
		}
		rec, err := readFrame(r.f)
		if err == nil {
			return rec, nil
		}
		isLast := r.idx == len(r.segments)-1
		if (errors.Is(err, errTornTail) || errors.Is(err, errCRC)) && isLast {
			r.tornTail = true
			_ = r.f.Close()
			r.f = nil
			return Record{}, io.EOF
		}
		if err == io.EOF {
			_ = r.f.Close()
			r.f = nil
			continue
		}
		return Record{}, err
	}
}

// TornTail reports whether iteration ended because the last segment had a
// torn or CRC-failed record at its tail.
func (r *Reader) TornTail() bool { return r.tornTail }

// Close releases the current file handle if any.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

var (
	errTornTail = errors.New("wal: torn record")
	errCRC      = errors.New("wal: crc mismatch")
)

func encodeFrame(seq uint64, payload []byte) []byte {
	frame := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(frame[4:8], uint32(8+len(payload)))
	binary.BigEndian.PutUint64(frame[8:16], seq)
	copy(frame[16:], payload)
	crc := crc32.ChecksumIEEE(frame[4:])
	binary.BigEndian.PutUint32(frame[0:4], crc)
	return frame
}

// readFrame reads a single record from r. Returns io.EOF at clean end,
// errTornTail if the frame is incomplete at end-of-file, errCRC on CRC
// failure, or another error on IO failure.
func readFrame(r io.Reader) (Record, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, errTornTail
		}
		return Record{}, fmt.Errorf("wal: read header: %w", err)
	}
	crc := binary.BigEndian.Uint32(hdr[0:4])
	length := binary.BigEndian.Uint32(hdr[4:8])
	seq := binary.BigEndian.Uint64(hdr[8:16])
	if length < 8 {
		return Record{}, fmt.Errorf("wal: invalid length %d", length)
	}
	payload := make([]byte, length-8)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return Record{}, errTornTail
		}
		return Record{}, fmt.Errorf("wal: read payload: %w", err)
	}
	h := crc32.NewIEEE()
	_, _ = h.Write(hdr[4:])
	_, _ = h.Write(payload)
	if h.Sum32() != crc {
		return Record{}, errCRC
	}
	return Record{Seq: seq, Payload: payload}, nil
}

// scanSegment returns the highest sequence number in the segment at path
// and the offset immediately after the last valid record. A torn tail or
// CRC failure at the end is silently absorbed; the caller truncates to
// validEnd.
func scanSegment(path string) (lastSeq uint64, validEnd int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("wal: scan open: %w", err)
	}
	defer func() { _ = f.Close() }()
	for {
		rec, rerr := readFrame(f)
		if rerr == nil {
			lastSeq = rec.Seq
			validEnd += int64(headerSize + len(rec.Payload))
			continue
		}
		if errors.Is(rerr, io.EOF) || errors.Is(rerr, errTornTail) || errors.Is(rerr, errCRC) {
			return lastSeq, validEnd, nil
		}
		return 0, 0, fmt.Errorf("wal: scan %s at %d: %w", path, validEnd, rerr)
	}
}

func segmentName(seq uint64) string {
	return fmt.Sprintf("%020d%s", seq, segmentSuffix)
}

func segmentStartSeq(path string) (uint64, error) {
	base := filepath.Base(path)
	if !strings.HasSuffix(base, segmentSuffix) {
		return 0, fmt.Errorf("wal: bad segment name %q", base)
	}
	seq, err := strconv.ParseUint(strings.TrimSuffix(base, segmentSuffix), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wal: parse %q: %w", base, err)
	}
	return seq, nil
}

func listSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: readdir %s: %w", dir, err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	return out, nil
}
