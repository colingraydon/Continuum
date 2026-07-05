package sstable

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

func TestWriterSurfacesWriteError(t *testing.T) {
	// BlockSize 1 forces a block flush on the first Add, where the write fails.
	w := NewWriter(errWriter{}, Options{BlockSize: 1})
	if err := w.Add([]byte("k"), []byte("v")); err == nil {
		t.Fatal("Add should surface a write error when flushing a block")
	}
}

func TestFinishSurfacesWriteError(t *testing.T) {
	// No data blocks flushed; Finish writes the index block first, which fails.
	w := NewWriter(errWriter{}, Options{})
	if err := w.Finish(); err == nil {
		t.Fatal("Finish should surface a write error")
	}
}

func TestOpenMissingFile(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing.sst")); err == nil {
		t.Fatal("Open should error on a missing file")
	}
}

func TestNewReaderTooSmall(t *testing.T) {
	if _, err := NewReader(bytes.NewReader([]byte("tiny")), 4); err == nil {
		t.Fatal("NewReader should reject a file smaller than the footer")
	}
}

func TestBloomCorruption(t *testing.T) {
	data := buildTable(t, Options{BlockSize: 256}, sortedEntries(100))
	f, err := decodeFooter(data[len(data)-footerSize:])
	if err != nil {
		t.Fatalf("decodeFooter: %v", err)
	}
	data[f.bloomOff] ^= 0xFF
	if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("NewReader with a corrupted bloom block should error")
	}
}

func TestShortBlockLengthRejected(t *testing.T) {
	// An index entry claiming a block shorter than its own CRC trailer.
	r := openTable(t, assembleTable([]byte{1, 2, 3}))
	if _, _, err := r.Get([]byte("a")); err == nil {
		t.Fatal("Get on a 3-byte block: want error")
	}
}

// failingBlockReaderAt serves reads at or above failBelow (footer, index,
// bloom) and fails everything below it (the data block region).
type failingBlockReaderAt struct {
	data      []byte
	failBelow int64
}

func (f failingBlockReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < f.failBelow {
		return 0, errors.New("boom")
	}
	return bytes.NewReader(f.data).ReadAt(p, off)
}

func TestBlockReadErrorSurfaced(t *testing.T) {
	entries := sortedEntries(100)
	data := buildTable(t, Options{BlockSize: 256}, entries)
	f, err := decodeFooter(data[len(data)-footerSize:])
	if err != nil {
		t.Fatalf("decodeFooter: %v", err)
	}
	// Metadata reads succeed, so the reader opens; the data block read fails.
	r, err := NewReader(failingBlockReaderAt{data: data, failBelow: int64(f.indexOff)}, int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, _, err := r.Get(entries[0].key); err == nil {
		t.Fatal("Get with failing block read: want error")
	}
}

func TestCloseNewReaderIsNoop(t *testing.T) {
	// A reader built via NewReader owns no file handle; Close is a no-op.
	r := openTable(t, buildTable(t, Options{}, sortedEntries(5)))
	if err := r.Close(); err != nil {
		t.Fatalf("Close on a NewReader-built reader = %v, want nil", err)
	}
}
