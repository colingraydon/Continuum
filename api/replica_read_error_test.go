package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// TestReplicaRead_StoreError covers the local-read error path of
// handleReplicaRead: a key whose only copy is in an SSTable whose data block is
// corrupt makes store.Get fail, which must surface as HTTP 500.
func TestReplicaRead_StoreError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tables")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s := store.New()
	s.SetFlushPolicy(dir, 0)
	if err := s.Put("k", "v", store.NewClock().Increment("a")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	t.Cleanup(func() { _ = s.CloseTables() })

	corruptTableDataBlock(t, dir)

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	h := NewHandler(r, ml, s, HandlerConfig{
		SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/keys/k", nil)
	req.Header.Set(headerXProxiedFrom, "coord") // route to handleReplicaRead
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("replica read of corrupted table: status = %d, want 500", w.Code)
	}
}

// corruptTableDataBlock flips a byte inside the first data block of the single
// .sst in dir. The footer/index/bloom (read into memory at open) stay intact,
// so the table opens cleanly but an on-demand block read trips the CRC.
func corruptTableDataBlock(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		f, err := os.OpenFile(filepath.Join(dir, e.Name()), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		b := make([]byte, 1)
		if _, err := f.ReadAt(b, 5); err != nil {
			t.Fatal(err)
		}
		b[0] ^= 0xFF
		if _, err := f.WriteAt(b, 5); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("no .sst file found to corrupt")
}
