package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/paxos"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// newSelfOnlyHandler returns a handler whose ring contains only the
// coordinator itself, so paxos rounds resolve with a majority of one and
// reads resolve entirely locally.
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

func doSerialGet(h *Handler, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/keys/"+key+"?consistency=serial", nil)
	w := httptest.NewRecorder()
	h.GetNode(w, req)
	return w
}

// --- CAS tests (single node: a majority of one) ---

func TestPutKeyCASInsertThenConflict(t *testing.T) {
	h := newSelfOnlyHandler(t)

	// Empty precondition clock on a missing key: insert-if-absent succeeds.
	w := doPut(h, "/keys/cas-key?cas=true", `{"value":"v1"}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("CAS insert: got %d (%s), want 204", w.Code, w.Body.String())
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

// --- CAS tests (multi-node: the paxos round over HTTP) ---

// paxosPeer is a fake replica that answers the three phase endpoints and
// records what it saw. entry is the committed state it reports in promises;
// committed is the last-committed ballot it claims.
type paxosPeer struct {
	mu        sync.Mutex
	phases    []string
	proposed  *paxos.Mutation
	entry     NodeResponse
	committed paxos.Ballot
}

func (p *paxosPeer) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		p.mu.Lock()
		p.phases = append(p.phases, req.URL.Path)
		p.mu.Unlock()
		switch req.URL.Path {
		case pathPaxosPrepare:
			w.Header().Set(contentTypeHeader, contentTypeJSON)
			_ = json.NewEncoder(w).Encode(prepareResponse{
				Promise: paxos.Promise{OK: true, Committed: p.committed},
				Entry:   p.entry,
			})
		case pathPaxosPropose:
			var m paxos.Mutation
			_ = json.NewDecoder(req.Body).Decode(&m)
			p.mu.Lock()
			p.proposed = &m
			p.mu.Unlock()
			w.Header().Set(contentTypeHeader, contentTypeJSON)
			_ = json.NewEncoder(w).Encode(paxos.Promise{OK: true})
		case pathPaxosCommit:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNoContent) // absorb hint/replica traffic
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (p *paxosPeer) sawPhases() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.phases))
	copy(out, p.phases)
	return out
}

func TestPutKeyCASRunsPaxosRoundAcrossReplicas(t *testing.T) {
	peer := &paxosPeer{entry: NodeResponse{ID: "peer", Status: "alive"}}
	srv := peer.server(t)

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", srv.Listener.Addr().String())
	// Two nodes, RF=3: the strict set is both nodes, majority is 2, so every
	// phase must reach the peer.
	w := doPut(h, "/keys/cas-round?cas=true", `{"value":"v1"}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("CAS round: got %d (%s), want 204", w.Code, w.Body.String())
	}

	phases := peer.sawPhases()
	want := []string{pathPaxosPrepare, pathPaxosPropose, pathPaxosCommit}
	if len(phases) != 3 || phases[0] != want[0] || phases[1] != want[1] || phases[2] != want[2] {
		t.Fatalf("peer saw phases %v, want %v", phases, want)
	}
	if peer.proposed == nil || peer.proposed.Value != "v1" || peer.proposed.Key != "cas-round" {
		t.Errorf("peer accepted mutation %+v, want value v1", peer.proposed)
	}
	// The coordinator applied its own commit locally.
	e, ok, _ := h.store.Get("cas-round")
	if !ok || len(e.Siblings) != 1 || e.Siblings[0].Value != "v1" {
		t.Errorf("local commit missing, got ok=%v %+v", ok, e)
	}
}

func TestPutKeyCASConflictAgainstQuorumState(t *testing.T) {
	// The peer's promise carries committed state this coordinator has never
	// seen. The precondition must be evaluated against the quorum-merged
	// state, not the local store — this is exactly the failover staleness
	// that forked histories under the primary-serialized design.
	peer := &paxosPeer{entry: NodeResponse{ID: "peer", Status: "alive", Value: "other", Clocks: map[string]uint64{"peer": 5}}}
	srv := peer.server(t)

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", srv.Listener.Addr().String())

	w := doPut(h, "/keys/cas-stale?cas=true", `{"value":"mine"}`)
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("got %d (%s), want 412", w.Code, w.Body.String())
	}
	if _, ok, _ := h.store.Get("cas-stale"); ok {
		t.Error("failed precondition must not write locally")
	}
	for _, phase := range peer.sawPhases() {
		if phase == pathPaxosPropose || phase == pathPaxosCommit {
			t.Errorf("a 412 must stop the round after prepare, peer saw %s", phase)
		}
	}
}

func TestPutKeyCASQuorumUnavailableFailsClosed(t *testing.T) {
	h := newSelfOnlyHandler(t)
	// Two of three replicas unreachable: self's own promise is not a
	// majority, so CAS fails closed without side effects.
	h.memberList.Add("dead1", "127.0.0.1:1")
	h.memberList.Add("dead2", "127.0.0.1:1")

	w := doPut(h, "/keys/cas-noquorum?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
	if _, ok, _ := h.store.Get("cas-noquorum"); ok {
		t.Error("fail-closed CAS must not write locally")
	}
	// A normal write to the same key still succeeds (W clamps to the live
	// set): fail-closed is CAS-only.
	if w := doPut(h, "/keys/cas-noquorum", `{"value":"v"}`); w.Code != http.StatusNoContent {
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

func TestPutKeyCASStoreFailureReturns503(t *testing.T) {
	h := newSelfOnlyHandler(t)
	attachFailingWAL(h)

	// A WAL failure during the commit apply is a store error, not a
	// precondition conflict: 503, never 412.
	w := doPut(h, "/keys/cas-walfail?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestPutKeyCASFinishesInFlightRound(t *testing.T) {
	h := newSelfOnlyHandler(t)

	// Seed an accepted-but-uncommitted mutation, as if another coordinator
	// crashed between accept and commit. It may already be decided, so a new
	// round must finish it before deciding anything else.
	orphanBallot := paxos.Ballot{Counter: 5, Node: "elsewhere"}
	if _, err := h.acceptor.Prepare("cas-orphan", orphanBallot); err != nil {
		t.Fatal(err)
	}
	orphan := paxos.Mutation{Key: "cas-orphan", Value: "orphan", Clocks: map[string]uint64{"elsewhere": 1}, Ballot: orphanBallot}
	if _, err := h.acceptor.Accept(orphan); err != nil {
		t.Fatal(err)
	}

	w := doPut(h, "/keys/cas-orphan?cas=true", `{"value":"mine"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d (%s), want 503 retry after finishing the round", w.Code, w.Body.String())
	}
	e, ok, _ := h.store.Get("cas-orphan")
	if !ok || len(e.Siblings) != 1 || e.Siblings[0].Value != "orphan" {
		t.Fatalf("in-flight round must be committed, got ok=%v %+v", ok, e)
	}

	// The client's retry chains off the settled state and succeeds.
	w = doPut(h, "/keys/cas-orphan?cas=true", `{"value":"mine","clocks":{"elsewhere":1}}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("retry after resurrection: got %d (%s), want 204", w.Code, w.Body.String())
	}
	e, _, _ = h.store.Get("cas-orphan")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "mine" {
		t.Errorf("expected retried CAS to win, got %+v", e)
	}
}

func TestPutKeyCASIgnoresSupersededDebris(t *testing.T) {
	// Self holds accepted debris from a round (ballot 5) that lost: the
	// peer's promise reports a later commit (ballot 9) this node missed
	// entirely. The debris is dead — resurrecting it would overwrite the
	// newer committed value with a superseded write (the forked-generation
	// signature the history checker caught).
	peer := &paxosPeer{
		entry:     NodeResponse{ID: "peer", Status: "alive", Value: "current", Clocks: map[string]uint64{"peer": 9}},
		committed: paxos.Ballot{Counter: 9, Node: "peer"},
	}
	srv := peer.server(t)

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", srv.Listener.Addr().String())

	debrisBallot := paxos.Ballot{Counter: 5, Node: "elsewhere"}
	if _, err := h.acceptor.Prepare("cas-debris", debrisBallot); err != nil {
		t.Fatal(err)
	}
	if _, err := h.acceptor.Accept(paxos.Mutation{Key: "cas-debris", Value: "orphan", Clocks: map[string]uint64{"elsewhere": 1}, Ballot: debrisBallot}); err != nil {
		t.Fatal(err)
	}

	// A CAS chained off the current committed state must win in one round —
	// no 503 for "finishing" the dead round, and no orphan resurrection.
	w := doPut(h, "/keys/cas-debris?cas=true", `{"value":"mine","clocks":{"peer":9}}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("got %d (%s), want 204: superseded debris must be ignored", w.Code, w.Body.String())
	}
	e, ok, _ := h.store.Get("cas-debris")
	if !ok || len(e.Siblings) != 1 || e.Siblings[0].Value != "mine" {
		t.Fatalf("expected the new write committed, got ok=%v %+v", ok, e)
	}
}

// --- serial read tests ---

func TestSerialReadReturnsCommittedValue(t *testing.T) {
	h := newSelfOnlyHandler(t)
	if w := doPut(h, "/keys/serial-key", `{"value":"v"}`); w.Code != http.StatusNoContent {
		t.Fatalf("seed put: got %d", w.Code)
	}

	w := doSerialGet(h, "serial-key")
	if w.Code != http.StatusOK {
		t.Fatalf("serial read: got %d (%s), want 200", w.Code, w.Body.String())
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Value != "v" {
		t.Errorf("expected value v, got %+v", resp)
	}
	if got := sessionClockFromHeader(t, w); got["self"] != 1 {
		t.Errorf("expected observed clock {self:1}, got %v", got)
	}
}

func TestSerialReadFinishesInFlightRound(t *testing.T) {
	h := newSelfOnlyHandler(t)
	orphanBallot := paxos.Ballot{Counter: 5, Node: "elsewhere"}
	if _, err := h.acceptor.Prepare("serial-orphan", orphanBallot); err != nil {
		t.Fatal(err)
	}
	orphan := paxos.Mutation{Key: "serial-orphan", Value: "orphan", Clocks: map[string]uint64{"elsewhere": 1}, Ballot: orphanBallot}
	if _, err := h.acceptor.Accept(orphan); err != nil {
		t.Fatal(err)
	}

	// The serial read must surface the possibly-decided mutation, not the
	// (absent) committed state — otherwise two consecutive serial reads
	// could disagree depending on which replicas they hit.
	w := doSerialGet(h, "serial-orphan")
	if w.Code != http.StatusOK {
		t.Fatalf("serial read: got %d (%s), want 200", w.Code, w.Body.String())
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Value != "orphan" {
		t.Errorf("expected resurrected value, got %+v", resp)
	}
	e, ok, _ := h.store.Get("serial-orphan")
	if !ok || len(e.Siblings) != 1 || e.Siblings[0].Value != "orphan" {
		t.Errorf("serial read must commit the round it finished, got ok=%v %+v", ok, e)
	}
}

func TestSerialReadQuorumUnavailableFailsClosed(t *testing.T) {
	h := newSelfOnlyHandler(t)
	h.memberList.Add("dead1", "127.0.0.1:1")
	h.memberList.Add("dead2", "127.0.0.1:1")

	w := doSerialGet(h, "serial-unreachable")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestWriteWithSerialConsistencyRejected(t *testing.T) {
	h := newSelfOnlyHandler(t)
	w := doPut(h, "/keys/serial-write?consistency=serial", `{"value":"v"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: serial is a read-only level", w.Code)
	}
}

func TestNextBallotMonotonicAndObserved(t *testing.T) {
	h := newTestHandler(t)
	b1 := h.nextBallot()
	b2 := h.nextBallot()
	if !b1.Less(b2) {
		t.Fatalf("ballots must be strictly increasing: %v then %v", b1, b2)
	}
	// Observing a rival's higher ballot makes the next mint supersede it.
	rival := paxos.Ballot{Counter: b2.Counter + 1_000_000, Node: "rival"}
	h.observeBallot(rival)
	if b3 := h.nextBallot(); !rival.Less(b3) {
		t.Fatalf("minted %v after observing %v; must supersede", b3, rival)
	}
}

// TestPutKeyRetryFromStaleBaseMintsFreshVersion pins the version-uniqueness
// rule: a client retrying from a stale clock base through the same
// coordinator must not reissue an earlier write's exact version for a
// different value. Before the coordinator raised its own counter to its
// local high-water mark, the second write either collided (equal clock,
// different value — permanently divergent replicas) or was silently dropped
// as dominated; the simulation harness caught the divergence as a
// convergence failure.
func TestPutKeyRetryFromStaleBaseMintsFreshVersion(t *testing.T) {
	h := newSelfOnlyHandler(t)

	// An earlier write through this coordinator the client never saw.
	if err := h.store.Put("retry-key", "first", store.VectorClockVersion{Clocks: map[string]uint64{"self": 5, "peer": 1}}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	// The client retries from the stale base {peer:1}. Naively incrementing
	// self yields {self:1,peer:1} — dominated by the stored {self:5,peer:1},
	// so the write would be dropped while still being acknowledged.
	w := doPut(h, "/keys/retry-key", `{"value":"second","clocks":{"peer":1}}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("put: got %d, want 204", w.Code)
	}
	clocks := sessionClockFromHeader(t, w)
	if clocks["self"] != 6 {
		t.Errorf("version must rise past the coordinator's high-water mark, got %v", clocks)
	}
	e, _, _ := h.store.Get("retry-key")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "second" {
		t.Errorf("acknowledged write must be stored and dominate, got %+v", e)
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
