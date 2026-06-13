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

func TestCloseNewReaderIsNoop(t *testing.T) {
	// A reader built via NewReader owns no file handle; Close is a no-op.
	r := openTable(t, buildTable(t, Options{}, sortedEntries(5)))
	if err := r.Close(); err != nil {
		t.Fatalf("Close on a NewReader-built reader = %v, want nil", err)
	}
}
