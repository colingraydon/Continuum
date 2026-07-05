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

// keyWithPrimary returns a key whose strict ring primary is nodeID, so tests
// can steer CAS requests at (or away from) a specific coordinator.
func keyWithPrimary(t *testing.T, r *ring.Ring, nodeID string) string {
	t.Helper()
	for i := range 10000 {
		key := fmt.Sprintf("cas-routed-%d", i)
		if nodes := r.GetReplicationNodes(key, 1); len(nodes) == 1 && nodes[0].ID == nodeID {
			return key
		}
	}
	t.Fatalf("no key found with primary %s", nodeID)
	return ""
}

func TestPutKeyCASForwardsToPrimary(t *testing.T) {
	var gotForwardedFrom, gotCASParam string
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotForwardedFrom = req.Header.Get(headerXCASForwarded)
		gotCASParam = req.URL.Query().Get("cas")
		w.Header().Set(headerXSessionClock, `{"peer":1}`)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer peerSrv.Close()

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", peerSrv.Listener.Addr().String())
	key := keyWithPrimary(t, h.ring, "peer")

	w := doPut(h, "/keys/"+key+"?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("forwarded CAS: got %d (%s), want 204", w.Code, w.Body.String())
	}
	if gotForwardedFrom != "self" {
		t.Errorf("primary must see %s: self, got %q", headerXCASForwarded, gotForwardedFrom)
	}
	if gotCASParam != "true" {
		t.Errorf("cas param must survive forwarding, got %q", gotCASParam)
	}
	// The primary's clock is relayed, and nothing was written locally: the
	// primary owns the write and fans it back out itself.
	if got := sessionClockFromHeader(t, w); got["peer"] != 1 {
		t.Errorf("expected relayed clock {peer:1}, got %v", got)
	}
	if _, ok, _ := h.store.Get(key); ok {
		t.Error("forwarding coordinator must not write locally")
	}
}

func TestPutKeyCASForwardRelays412(t *testing.T) {
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Error(w, "cas conflict: clocks do not dominate the current value", http.StatusPreconditionFailed)
	}))
	defer peerSrv.Close()

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", peerSrv.Listener.Addr().String())
	key := keyWithPrimary(t, h.ring, "peer")

	w := doPut(h, "/keys/"+key+"?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("got %d, want relayed 412", w.Code)
	}
}

func TestDeleteKeyCASForwardsToPrimary(t *testing.T) {
	var gotMethod string
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotMethod = req.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	defer peerSrv.Close()

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", peerSrv.Listener.Addr().String())
	key := keyWithPrimary(t, h.ring, "peer")

	req := httptest.NewRequest(http.MethodDelete, "/keys/"+key+"?cas=true", bytes.NewBufferString(`{"clocks":{"peer":1}}`))
	w := httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("forwarded CAS delete: got %d, want 204", w.Code)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("expected DELETE forwarded, got %q", gotMethod)
	}
}

func TestPutKeyCASPrimaryUnavailableFailsClosed(t *testing.T) {
	h := newSelfOnlyHandler(t)
	// In the ring but not alive in the member list: reachable by ring walk,
	// but not a node CAS may trust.
	h.ring.AddNode("ghost", "localhost:1")
	key := keyWithPrimary(t, h.ring, "ghost")

	w := doPut(h, "/keys/"+key+"?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
	if _, ok, _ := h.store.Get(key); ok {
		t.Error("fail-closed CAS must not write locally")
	}
	// A normal write to the same key still succeeds: fail-closed is CAS-only.
	if w := doPut(h, "/keys/"+key, `{"value":"v"}`); w.Code != http.StatusNoContent {
		t.Errorf("plain write: got %d, want 204", w.Code)
	}
}

func TestPutKeyCASEmptyRingRejected(t *testing.T) {
	h := newTestHandler(t) // ring has no nodes at all
	w := doPut(h, "/keys/cas-noring?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
	if _, ok, _ := h.store.Get("cas-noring"); ok {
		t.Error("empty-ring CAS must not write locally")
	}
}

func TestPutKeyCASPrimaryUnreachableFailsClosed(t *testing.T) {
	h := newSelfOnlyHandler(t)
	// Alive in the member list but nothing listens on the address: the
	// forward itself fails and must surface as a retryable 503.
	h.memberList.Add("peer", "127.0.0.1:1")
	key := keyWithPrimary(t, h.ring, "peer")

	w := doPut(h, "/keys/"+key+"?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
	if _, ok, _ := h.store.Get(key); ok {
		t.Error("failed forward must not write locally")
	}
}

func TestPutKeyCASStoreFailureReturns503(t *testing.T) {
	h := newSelfOnlyHandler(t)
	attachFailingWAL(h)

	// A WAL failure during a CAS write is a store error, not a precondition
	// conflict: 503, never 412.
	w := doPut(h, "/keys/cas-walfail?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestPutKeyCASForwardedMismatchNotReforwarded(t *testing.T) {
	var peerHits int
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		peerHits++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer peerSrv.Close()

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", peerSrv.Listener.Addr().String())
	key := keyWithPrimary(t, h.ring, "peer")

	// An already-forwarded request landing on a node that does not consider
	// itself primary means ring views diverge: reject, never forward again.
	req := httptest.NewRequest(http.MethodPut, "/keys/"+key+"?cas=true", bytes.NewBufferString(`{"value":"v"}`))
	req.Header.Set(headerXCASForwarded, "elsewhere")
	w := httptest.NewRecorder()
	h.PutKey(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
	if peerHits != 0 {
		t.Errorf("mismatch must not re-forward, primary was hit %d times", peerHits)
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

// sessionReadWithStalePeer builds a two-node read set (self plus an httptest
// peer serving peerValue at peerClocks), seeds the coordinator's local store
// with a stale "sess-key" at {peer:1}, and performs a session GET carrying
// sessionClock. With R=1 the quorum read may hit either replica, so the
// outcome exercises the escalation path deterministically.
func sessionReadWithStalePeer(t *testing.T, peerValue string, peerClocks map[string]uint64, sessionClock string) *httptest.ResponseRecorder {
	t.Helper()
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.WriteHeader(http.StatusNoContent) // absorb read-repair writes
			return
		}
		resp := NodeResponse{ID: "peer", Status: "alive", Value: peerValue, Clocks: peerClocks}
		w.Header().Set(contentTypeHeader, contentTypeJSON)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(peerSrv.Close)

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)
	r.AddNode("self", "localhost:8080")
	r.AddNode("peer", peerSrv.Listener.Addr().String())
	if err := s.Put("sess-key", "stale", store.VectorClockVersion{Clocks: map[string]uint64{"peer": 1}}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/keys/sess-key", nil)
	req.Header.Set(headerXSessionClock, sessionClock)
	w := httptest.NewRecorder()
	h.GetNode(w, req)
	return w
}

// TestSessionReadEscalatesPastQuorum proves the visibility-window fix: with
// R=1 the quorum read may hit a replica that has not seen the session's
// write, and the coordinator must escalate to the full replica set to
// satisfy the session clock rather than returning the stale value.
func TestSessionReadEscalatesPastQuorum(t *testing.T) {
	// The peer holds the newer write the session has already seen.
	w := sessionReadWithStalePeer(t, "new", map[string]uint64{"peer": 2}, `{"peer":2}`)

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

// TestSessionReadEscalationStillUnsatisfiableFails proves the escalated read
// fails closed too: when even the full replica set cannot cover the session
// clock, the coordinator returns 503 instead of the freshest stale value.
func TestSessionReadEscalationStillUnsatisfiableFails(t *testing.T) {
	// No replica has ever seen {peer:2}: quorum read misses, escalation to
	// both replicas misses too.
	w := sessionReadWithStalePeer(t, "stale", map[string]uint64{"peer": 1}, `{"peer":2}`)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
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
