package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// newSelfOnlyHandler returns a handler whose ring contains only the
// coordinator itself, so writes and reads resolve entirely locally.
func newSelfOnlyHandler(t *testing.T) *Handler {
	t.Helper()
	h := newTestHandler(t)
	h.ring.AddNode("self", "localhost:8080")
	return h
}

func sessionClockFromHeader(t *testing.T, w *httptest.ResponseRecorder) map[string]uint64 {
	t.Helper()
	raw := w.Header().Get(headerXSessionClock)
	if raw == "" {
		t.Fatal("expected X-Session-Clock response header")
	}
	var clocks map[string]uint64
	if err := json.Unmarshal([]byte(raw), &clocks); err != nil {
		t.Fatalf("decode session clock header %q: %v", raw, err)
	}
	return clocks
}

func doPut(h *Handler, url, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, url, bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	return w
}

// --- CAS tests ---

func TestPutKeyCASInsertThenConflict(t *testing.T) {
	h := newSelfOnlyHandler(t)

	// Empty precondition clock on a missing key: insert-if-absent succeeds.
	w := doPut(h, "/keys/cas-key?cas=true", `{"value":"v1"}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("CAS insert: got %d, want 204", w.Code)
	}
	clocks := sessionClockFromHeader(t, w)
	if clocks["self"] != 1 {
		t.Errorf("expected clock {self:1}, got %v", clocks)
	}

	// Same empty precondition now that a value exists: 412, value unchanged.
	w = doPut(h, "/keys/cas-key?cas=true", `{"value":"v2"}`)
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("CAS conflict: got %d, want 412", w.Code)
	}
	e, _, _ := h.store.Get("cas-key")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "v1" {
		t.Errorf("conflicting CAS must not modify the store, got %+v", e)
	}

	// Precondition matching the current clock: accepted.
	body := fmt.Sprintf(`{"value":"v2","clocks":{"self":%d}}`, clocks["self"])
	w = doPut(h, "/keys/cas-key?cas=true", body)
	if w.Code != http.StatusNoContent {
		t.Fatalf("CAS with current clock: got %d, want 204", w.Code)
	}
	e, _, _ = h.store.Get("cas-key")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "v2" {
		t.Errorf("expected single sibling 'v2', got %+v", e)
	}
}

func TestPutKeyCASRejectsConcurrentSibling(t *testing.T) {
	h := newSelfOnlyHandler(t)
	// Simulate a replicated write from another coordinator.
	if err := h.store.Put("cas-key", "other", store.VectorClockVersion{Clocks: map[string]uint64{"peer": 3}}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	// A CAS whose clock is concurrent with the peer write must not create a
	// sibling (a plain PUT would).
	w := doPut(h, "/keys/cas-key?cas=true", `{"value":"mine","clocks":{"self":1}}`)
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("got %d, want 412", w.Code)
	}
	e, _, _ := h.store.Get("cas-key")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "other" {
		t.Errorf("expected store unchanged, got %+v", e)
	}
}

func TestPutKeyInvalidCASParamRejectedBeforeWrite(t *testing.T) {
	h := newSelfOnlyHandler(t)
	w := doPut(h, "/keys/cas-bogus?cas=yes", `{"value":"v"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", w.Code)
	}
	if _, ok, err := h.store.Get("cas-bogus"); err != nil || ok {
		t.Errorf("invalid cas param must not write locally (ok=%v err=%v)", ok, err)
	}
}

func TestDeleteKeyCAS(t *testing.T) {
	h := newSelfOnlyHandler(t)
	w := doPut(h, "/keys/cas-del", `{"value":"v"}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("seed put: got %d, want 204", w.Code)
	}
	clocks := sessionClockFromHeader(t, w)

	// Empty precondition clock on an existing key: rejected, value survives.
	req := httptest.NewRequest(http.MethodDelete, "/keys/cas-del?cas=true", bytes.NewBufferString(`{}`))
	w = httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale CAS delete: got %d, want 412", w.Code)
	}
	e, ok, _ := h.store.Get("cas-del")
	if !ok || len(e.Siblings) != 1 || e.Siblings[0].Deleted {
		t.Fatalf("value must survive a failed CAS delete, got ok=%v %+v", ok, e)
	}

	// Matching precondition clock: tombstone written.
	body, _ := json.Marshal(DeleteKeyRequest{Clocks: clocks})
	req = httptest.NewRequest(http.MethodDelete, "/keys/cas-del?cas=true", bytes.NewReader(body))
	w = httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("CAS delete with current clock: got %d, want 204", w.Code)
	}
	e, _, _ = h.store.Get("cas-del")
	if len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Errorf("expected tombstone, got %+v", e)
	}
}

// --- Session guarantee tests ---

func TestPutThenSessionReadIsSatisfied(t *testing.T) {
	h := newSelfOnlyHandler(t)
	w := doPut(h, "/keys/sess-key", `{"value":"v"}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("put: got %d, want 204", w.Code)
	}
	raw := w.Header().Get(headerXSessionClock)

	req := httptest.NewRequest(http.MethodGet, "/keys/sess-key", nil)
	req.Header.Set(headerXSessionClock, raw)
	w = httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("session read: got %d, want 200", w.Code)
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Value != "v" {
		t.Errorf("expected value 'v', got %+v", resp)
	}
	// The read returns the observed clock for monotonic follow-up reads.
	got := sessionClockFromHeader(t, w)
	if got["self"] != 1 {
		t.Errorf("expected observed clock {self:1}, got %v", got)
	}
}

func TestSessionReadUnsatisfiableFailsClosed(t *testing.T) {
	h := newSelfOnlyHandler(t)
	if err := h.store.Put("sess-key", "v", store.VectorClockVersion{Clocks: map[string]uint64{"self": 1}}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	// The session has seen a write no replica can produce.
	req := httptest.NewRequest(http.MethodGet, "/keys/sess-key", nil)
	req.Header.Set(headerXSessionClock, `{"elsewhere":5}`)
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestSessionReadInvalidHeaderRejected(t *testing.T) {
	h := newSelfOnlyHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/keys/sess-key", nil)
	req.Header.Set(headerXSessionClock, "not json")
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", w.Code)
	}
}

// TestSessionReadEscalatesPastQuorum proves the visibility-window fix: with
// R=1 the quorum read may hit a replica that has not seen the session's
// write, and the coordinator must escalate to the full replica set to
// satisfy the session clock rather than returning the stale value.
func TestSessionReadEscalatesPastQuorum(t *testing.T) {
	// Remote replica that holds the newer write.
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.WriteHeader(http.StatusNoContent) // absorb read-repair writes
			return
		}
		resp := NodeResponse{ID: "peer", Status: "alive", Value: "new", Clocks: map[string]uint64{"peer": 2}}
		w.Header().Set(contentTypeHeader, contentTypeJSON)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer peerSrv.Close()

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)
	r.AddNode("self", "localhost:8080")
	r.AddNode("peer", peerSrv.Listener.Addr().String())

	// The coordinator holds a stale version the session has already moved past.
	if err := s.Put("sess-key", "stale", store.VectorClockVersion{Clocks: map[string]uint64{"peer": 1}}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/keys/sess-key", nil)
	req.Header.Set(headerXSessionClock, `{"peer":2}`)
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d (%s), want 200", w.Code, w.Body.String())
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Value != "new" {
		t.Errorf("expected escalated read to return 'new', got %+v", resp)
	}
	got := sessionClockFromHeader(t, w)
	if got["peer"] < 2 {
		t.Errorf("observed clock %v must cover the session clock", got)
	}
}

func TestDeletedKeySessionReadReturnsClockOn404(t *testing.T) {
	h := newSelfOnlyHandler(t)
	if w := doPut(h, "/keys/gone", `{"value":"v"}`); w.Code != http.StatusNoContent {
		t.Fatalf("put: got %d, want 204", w.Code)
	}
	req := httptest.NewRequest(http.MethodDelete, "/keys/gone", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete: got %d, want 204", w.Code)
	}
	tombClock := w.Header().Get(headerXSessionClock)

	// A session read of the deleted key sees the tombstone: 404, but the
	// header still carries the tombstone clock so the session can advance.
	req = httptest.NewRequest(http.MethodGet, "/keys/gone", nil)
	req.Header.Set(headerXSessionClock, tombClock)
	w = httptest.NewRecorder()
	h.GetNode(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", w.Code)
	}
	got := sessionClockFromHeader(t, w)
	if got["self"] != 2 {
		t.Errorf("expected tombstone clock {self:2}, got %v", got)
	}
}
