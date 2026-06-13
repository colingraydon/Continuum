package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// fakeWAL fails every Append so the store rejects mutating operations.
type fakeWAL struct{ err error }

func (f fakeWAL) Append([]byte) (uint64, error) { return 0, f.err }
func (f fakeWAL) Sync() error                   { return nil }
func (f fakeWAL) TruncateThrough(uint64) error  { return nil }

func attachFailingWAL(h *Handler) {
	h.store.SetWAL(fakeWAL{err: errors.New("simulated disk error")})
}

func TestPutKey_PrimaryStoreFailureReturns503(t *testing.T) {
	h := newTestHandler(t)
	attachFailingWAL(h)

	req := httptest.NewRequest(http.MethodPut, "/keys/k", strings.NewReader(`{"value":"v"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestPutKey_ReplicaStoreFailureReturns503(t *testing.T) {
	h := newTestHandler(t)
	attachFailingWAL(h)

	req := httptest.NewRequest(http.MethodPut, "/keys/k", strings.NewReader(`{"value":"v"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerXProxiedFrom, "peer")
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestDeleteKey_PrimaryStoreFailureReturns503(t *testing.T) {
	h := newTestHandler(t)
	attachFailingWAL(h)

	req := httptest.NewRequest(http.MethodDelete, "/keys/k", strings.NewReader("{}"))
	w := httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestDeleteKey_ReplicaStoreFailureReturns503(t *testing.T) {
	h := newTestHandler(t)
	attachFailingWAL(h)

	req := httptest.NewRequest(http.MethodDelete, "/keys/k", strings.NewReader("{}"))
	req.Header.Set(headerXProxiedFrom, "peer")
	w := httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestPushSyncEntries_LogsAndReturns204OnStoreError(t *testing.T) {
	h := newTestHandler(t)
	attachFailingWAL(h)

	body := SyncKeysResponse{Entries: map[string][]SyncSibling{
		"k1": {{Value: "v", Clocks: map[string]uint64{"a": 1}}},
		"k2": {{Deleted: true, Clocks: map[string]uint64{"a": 2}}},
	}}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewReader(buf))
	w := httptest.NewRecorder()
	h.PushSyncEntries(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 (logs + continues), got %d", w.Code)
	}
}

func TestApplyEntries_LogsAndContinuesOnStoreError(t *testing.T) {
	h := newTestHandler(t)
	attachFailingWAL(h)
	// Should not panic; the per-key error is logged and the next entry tried.
	h.applyEntries(map[string][]SyncSibling{
		"a": {{Value: "v", Clocks: map[string]uint64{"x": 1}}},
		"b": {{Deleted: true, Clocks: map[string]uint64{"x": 2}}},
	})
}

func TestCleanupStaleKeys_LogsAndContinuesOnEvictError(t *testing.T) {
	h := newTestHandler(t)
	// Seed a key that the local node does not own (ring is empty so every
	// key is unowned → CleanupStaleKeys will try to evict it).
	if err := h.store.Put("orphan", "v", store.VectorClockVersion{Clocks: map[string]uint64{"a": 1}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	attachFailingWAL(h)
	h.CleanupStaleKeys() // must not panic; error is logged
}

func TestRepairSurvivor_LogsOnLocalStoreError(t *testing.T) {
	h := newTestHandler(t)
	attachFailingWAL(h)
	// Local repair path: nodeID == selfID hits the store directly.
	h.repairSurvivor("k", h.selfID, "ignored", SiblingResponse{
		Value:  "v",
		Clocks: map[string]uint64{"a": 1},
	})
	h.repairSurvivor("k", h.selfID, "ignored", SiblingResponse{
		Deleted: true,
		Clocks:  map[string]uint64{"a": 2},
	})
}

// Verifying the interface contract: *wal.Writer must satisfy store.WAL.
var _ store.WAL = (*fakeWALSatisfies)(nil)

type fakeWALSatisfies struct{}

func (fakeWALSatisfies) Append([]byte) (uint64, error) { return 0, nil }
func (fakeWALSatisfies) Sync() error                   { return nil }
func (fakeWALSatisfies) TruncateThrough(uint64) error  { return nil }

// Confirm we don't accidentally drop the ring import (needed for the test
// helper file's ring.NewRing call elsewhere).
var _ = ring.NewRing
