package hintstore

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
)

// fakeLogWriter is an injectable logWriter whose operations fail on demand.
type fakeLogWriter struct {
	appendErr error
	syncErr   error
	closeErr  error
	seq       uint64
}

func (f *fakeLogWriter) Append([]byte) (uint64, error) {
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	f.seq++
	return f.seq, nil
}
func (f *fakeLogWriter) SyncUpTo(uint64) error { return f.syncErr }
func (f *fakeLogWriter) Close() error          { return f.closeErr }

func swapWalOpen(t *testing.T, fn func(string) (logWriter, error)) {
	t.Helper()
	prev := walOpen
	walOpen = fn
	t.Cleanup(func() { walOpen = prev })
}

func swapRemoveAll(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := removeAll
	removeAll = fn
	t.Cleanup(func() { removeAll = prev })
}

// --- write-path best-effort error logging ----------------------------------

func TestPersistent_AppendErrorsAreBestEffort(t *testing.T) {
	hs := New(100, time.Hour)
	hs.log = &hintLog{dir: t.TempDir(), w: &fakeLogWriter{appendErr: errors.New("disk full")}}

	// Store appends a STORE record (fails -> logged, seq 0 -> syncUpTo(0) no-op),
	// but the hint still lands in memory.
	hs.Store("nodeA", makeHint("k"))
	if got := hs.PendingNodes(); len(got) != 1 {
		t.Fatalf("hint should be buffered in memory despite append failure: %v", got)
	}
	// Drain appends a REMOVE record (also fails -> logged).
	if got := hs.Drain("nodeA"); len(got) != 1 {
		t.Fatalf("drain: want 1, got %d", len(got))
	}
}

func TestPersistent_SyncUpToZeroIsNoop(t *testing.T) {
	l := &hintLog{w: &fakeLogWriter{syncErr: errors.New("should not be called")}}
	if err := l.syncUpTo(0); err != nil {
		t.Fatalf("syncUpTo(0) must be a no-op, got %v", err)
	}
}

// --- NewPersistent error branches ------------------------------------------

func TestPersistent_MkdirError(t *testing.T) {
	// A regular file where a parent directory is expected makes MkdirAll fail.
	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPersistent(filepath.Join(file, "hints"), 100, time.Hour); err == nil {
		t.Fatal("expected mkdir error")
	}
}

func TestPersistent_CleanScratchError(t *testing.T) {
	swapRemoveAll(t, func(string) error { return errors.New("rm fail") })
	if _, err := NewPersistent(filepath.Join(t.TempDir(), "hints"), 100, time.Hour); err == nil {
		t.Fatal("expected clean-scratch error")
	}
}

func TestPersistent_OpenWriterError(t *testing.T) {
	swapWalOpen(t, func(string) (logWriter, error) { return nil, errors.New("open fail") })
	if _, err := NewPersistent(filepath.Join(t.TempDir(), "hints"), 100, time.Hour); err == nil {
		t.Fatal("expected open-writer error")
	}
}

// --- Close / maybeCompact rewrite-error logging ----------------------------

func TestPersistent_CloseRewriteError(t *testing.T) {
	hs := New(100, time.Hour)
	hs.log = &hintLog{dir: t.TempDir(), w: &fakeLogWriter{closeErr: errors.New("close fail")}}
	if err := hs.Close(); err == nil {
		t.Fatal("expected close error to surface after compaction failure")
	}
}

func TestPersistent_MaybeCompactRewriteError(t *testing.T) {
	hs := New(100, time.Hour)
	hs.log = &hintLog{
		dir:              t.TempDir(),
		w:                &fakeLogWriter{closeErr: errors.New("boom")},
		appendsSinceComp: compactThreshold,
	}
	hs.maybeCompact() // rewrite fails at l.w.Close -> logged, no panic
}

// --- rewrite internal error branches ---------------------------------------

func TestRewrite_OpenTmpError(t *testing.T) {
	l := &hintLog{dir: t.TempDir(), w: &fakeLogWriter{}}
	swapWalOpen(t, func(string) (logWriter, error) { return nil, errors.New("open tmp fail") })
	if err := l.rewrite(map[string][]storedHint{}); err == nil {
		t.Fatal("expected open-tmp error")
	}
}

func TestRewrite_AppendError(t *testing.T) {
	l := &hintLog{dir: t.TempDir(), w: &fakeLogWriter{}}
	swapWalOpen(t, func(string) (logWriter, error) {
		return &fakeLogWriter{appendErr: errors.New("append fail")}, nil
	})
	hints := map[string][]storedHint{"n": {{Hint: Hint{Key: "k"}, seq: 1}}}
	if err := l.rewrite(hints); err == nil {
		t.Fatal("expected rewrite append error")
	}
}

func TestRewrite_NewWriterCloseError(t *testing.T) {
	l := &hintLog{dir: t.TempDir(), w: &fakeLogWriter{}}
	swapWalOpen(t, func(string) (logWriter, error) {
		return &fakeLogWriter{closeErr: errors.New("nw close fail")}, nil
	})
	if err := l.rewrite(map[string][]storedHint{}); err == nil {
		t.Fatal("expected new-writer close error")
	}
}

func TestRewrite_RemoveTmpError(t *testing.T) {
	l := &hintLog{dir: t.TempDir(), w: &fakeLogWriter{}}
	swapRemoveAll(t, func(string) error { return errors.New("rm tmp fail") })
	if err := l.rewrite(map[string][]storedHint{}); err == nil {
		t.Fatal("expected remove-tmp error")
	}
}

func TestRewrite_RemoveDirError(t *testing.T) {
	dir := t.TempDir()
	l := &hintLog{dir: dir, w: &fakeLogWriter{}}
	swapWalOpen(t, func(string) (logWriter, error) { return &fakeLogWriter{}, nil })
	// Succeed for the tmp removal, fail for the live-dir removal.
	swapRemoveAll(t, func(p string) error {
		if p == dir {
			return errors.New("rm dir fail")
		}
		return os.RemoveAll(p)
	})
	if err := l.rewrite(map[string][]storedHint{}); err == nil {
		t.Fatal("expected remove-dir error")
	}
}

func TestRewrite_RenameError(t *testing.T) {
	dir := t.TempDir()
	l := &hintLog{dir: dir, w: &fakeLogWriter{}}
	// All-fake opens never create a real tmp dir, so the rename has no source.
	swapWalOpen(t, func(string) (logWriter, error) { return &fakeLogWriter{}, nil })
	if err := l.rewrite(map[string][]storedHint{}); err == nil {
		t.Fatal("expected rename error (missing scratch dir)")
	}
}

func TestRewrite_ReopenError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	l := &hintLog{dir: dir, w: &fakeLogWriter{}}
	// Real open for the scratch dir so the swap succeeds; fail only the reopen.
	swapWalOpen(t, func(p string) (logWriter, error) {
		if p == dir {
			return nil, errors.New("reopen fail")
		}
		w, err := wal.Open(p)
		if err != nil {
			return nil, err
		}
		return w, nil
	})
	if err := l.rewrite(map[string][]storedHint{}); err == nil {
		t.Fatal("expected reopen error")
	}
}

// --- replay / decode error branches ----------------------------------------

func TestReplayHintLog_ReaderError(t *testing.T) {
	// A file (not a directory) makes the reader's directory scan fail.
	file := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := replayHintLog(file, time.Hour); err == nil {
		t.Fatal("expected reader error")
	}
}

func TestReplayHintLog_InvalidFrameLength(t *testing.T) {
	dir := t.TempDir()
	// Hand-write a 16-byte WAL frame header whose length field is < 8, which the
	// reader rejects as a hard (non-torn) error mid-stream.
	hdr := make([]byte, 16)
	binary.BigEndian.PutUint32(hdr[4:8], 3)
	name := filepath.Join(dir, "00000000000000000001.wal")
	if err := os.WriteFile(name, hdr, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := replayHintLog(dir, time.Hour); err == nil {
		t.Fatal("expected invalid-frame error")
	}
}

func TestDecodeRemove_Errors(t *testing.T) {
	if _, _, err := decodeRemove([]byte{0x00}); err == nil {
		t.Fatal("expected node-length error")
	}
	if _, _, err := decodeRemove([]byte{0x00, 0x01, 'n'}); err == nil {
		t.Fatal("expected count error")
	}
	if _, _, err := decodeRemove([]byte{0x00, 0x01, 'n', 0, 0, 0, 1}); err == nil {
		t.Fatal("expected seq error")
	}
	node, seqs, err := decodeRemove(encodeRemove("n", []uint64{5, 6})[1:])
	if err != nil || node != "n" || len(seqs) != 2 {
		t.Fatalf("round-trip: %q %v %v", node, seqs, err)
	}
}

func TestRemoveSeqs_EmptyInputs(t *testing.T) {
	if got := removeSeqs(nil, []uint64{1}); got != nil {
		t.Fatalf("nil hints should pass through: %v", got)
	}
	in := []storedHint{{seq: 1}}
	if got := removeSeqs(in, nil); len(got) != 1 {
		t.Fatalf("nil drop should pass through: %v", got)
	}
}

func TestPruneExpired_DeleteAndKeep(t *testing.T) {
	now := time.Now()
	hints := map[string][]storedHint{
		"all":  {{Hint: Hint{At: now.Add(-2 * time.Hour)}}},
		"some": {{Hint: Hint{At: now}}, {Hint: Hint{At: now.Add(-2 * time.Hour)}}},
	}
	pruneExpired(hints, now.Add(-time.Hour))
	if _, ok := hints["all"]; ok {
		t.Fatal("fully-expired node should be deleted")
	}
	if len(hints["some"]) != 1 {
		t.Fatalf("partially-expired node should keep 1, got %d", len(hints["some"]))
	}
}

// TestPersistent_SyncErrorsAreLogged covers the best-effort sync-failure logging
// in Store, Drain, and ExpireOld (append succeeds, the batched fsync fails).
func TestPersistent_SyncErrorsAreLogged(t *testing.T) {
	hs := New(100, time.Hour)
	hs.log = &hintLog{dir: t.TempDir(), w: &fakeLogWriter{syncErr: errors.New("fsync fail")}}

	hs.Store("nodeA", makeHint("k")) // Store sync-error branch
	hs.Drain("nodeA")                // Drain sync-error branch

	hs.Store("nodeB", Hint{Key: "old", At: time.Now().Add(-2 * time.Hour)})
	hs.ExpireOld() // ExpireOld sync-error branch (the aged hint is removed)
}
