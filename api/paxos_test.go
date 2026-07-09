package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colingraydon/continuum/internal/paxos"
	"github.com/colingraydon/continuum/internal/store"
)

func postJSON(h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/paxos", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h(w, req)
	return w
}

// --- replica-side endpoints -------------------------------------------------

func TestPaxosPrepareEndpoint(t *testing.T) {
	h := newSelfOnlyHandler(t)

	// A malformed body (or empty key) is rejected before touching the acceptor.
	if w := postJSON(h.PaxosPrepare, `not json`); w.Code != http.StatusBadRequest {
		t.Fatalf("bad body: got %d, want 400", w.Code)
	}
	if w := postJSON(h.PaxosPrepare, `{"ballot":{"counter":1,"node":"x"}}`); w.Code != http.StatusBadRequest {
		t.Fatalf("empty key: got %d, want 400", w.Code)
	}

	// A well-formed prepare promises the ballot and reports committed state.
	if err := h.store.Put("k", "v", store.VectorClockVersion{Clocks: map[string]uint64{"self": 1}}); err != nil {
		t.Fatal(err)
	}
	w := postJSON(h.PaxosPrepare, `{"key":"k","ballot":{"counter":10,"node":"c"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("prepare: got %d (%s), want 200", w.Code, w.Body.String())
	}
	var resp prepareResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.OK || resp.Entry.Value != "v" {
		t.Fatalf("expected promised prepare with committed value, got %+v", resp)
	}
}

func TestPaxosProposeEndpoint(t *testing.T) {
	h := newSelfOnlyHandler(t)

	if w := postJSON(h.PaxosPropose, `{bad`); w.Code != http.StatusBadRequest {
		t.Fatalf("bad body: got %d, want 400", w.Code)
	}

	m := paxos.Mutation{Key: "k", Value: "v", Clocks: map[string]uint64{"c": 1}, Ballot: paxos.Ballot{Counter: 5, Node: "c"}}
	body, _ := json.Marshal(m)
	w := postJSON(h.PaxosPropose, string(body))
	if w.Code != http.StatusOK {
		t.Fatalf("propose: got %d, want 200", w.Code)
	}
	var p paxos.Promise
	if err := json.NewDecoder(w.Body).Decode(&p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !p.OK {
		t.Fatal("a fresh proposal must be accepted")
	}
}

func TestPaxosCommitEndpoint(t *testing.T) {
	h := newSelfOnlyHandler(t)

	if w := postJSON(h.PaxosCommit, `{`); w.Code != http.StatusBadRequest {
		t.Fatalf("bad body: got %d, want 400", w.Code)
	}

	ballot := paxos.Ballot{Counter: 5, Node: "c"}
	m := paxos.Mutation{Key: "k", Value: "committed", Clocks: map[string]uint64{"c": 1}, Ballot: ballot}
	if _, err := h.acceptor.Accept(m); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(m)
	if w := postJSON(h.PaxosCommit, string(body)); w.Code != http.StatusNoContent {
		t.Fatalf("commit: got %d, want 204", w.Code)
	}
	// Commit applied the mutation to the store...
	e, ok, _ := h.store.Get("k")
	if !ok || e.Siblings[0].Value != "committed" {
		t.Fatalf("commit must apply to the store, got ok=%v %+v", ok, e)
	}
	// ...and cleared the accepted round, so a later prepare sees no in-flight.
	p, err := h.acceptor.Prepare("k", paxos.Ballot{Counter: 9, Node: "c"})
	if err != nil || p.Accepted != nil {
		t.Fatalf("commit must clear the accepted round, got %+v err=%v", p, err)
	}
}

func TestPaxosCommitDeleteEndpoint(t *testing.T) {
	h := newSelfOnlyHandler(t)
	if err := h.store.Put("k", "v", store.VectorClockVersion{Clocks: map[string]uint64{"c": 1}}); err != nil {
		t.Fatal(err)
	}
	ballot := paxos.Ballot{Counter: 5, Node: "c"}
	m := paxos.Mutation{Key: "k", Deleted: true, Clocks: map[string]uint64{"c": 2}, Ballot: ballot}
	body, _ := json.Marshal(m)
	if w := postJSON(h.PaxosCommit, string(body)); w.Code != http.StatusNoContent {
		t.Fatalf("commit delete: got %d, want 204", w.Code)
	}
	if e, _, _ := h.store.Get("k"); !e.Siblings[0].Deleted {
		t.Fatalf("commit of a tombstone must delete, got %+v", e)
	}
}

// --- coordinator edge branches ----------------------------------------------

func TestSetPaxosAcceptorReplacesAcceptor(t *testing.T) {
	h := newTestHandler(t)
	fresh := paxos.NewAcceptor()
	h.SetPaxosAcceptor(fresh)
	if h.acceptor != fresh {
		t.Fatal("SetPaxosAcceptor must install the given acceptor")
	}
}

func TestCASRetriesPastContendedBallot(t *testing.T) {
	// A peer that rejects the first prepare with a higher ballot, then
	// accepts everything, forces the coordinator down its observe-and-retry
	// path (retryOrFail with a non-zero higher ballot).
	var prepares int
	peer := &contendingPeer{higher: paxos.Ballot{Counter: 1 << 62, Node: "z"}, rejectFirst: &prepares}
	srv := peer.server(t)

	h := newSelfOnlyHandler(t)
	h.memberList.Add("peer", srv.Listener.Addr().String())

	w := doPut(h, "/keys/contended?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("contended CAS must succeed after retry: got %d (%s)", w.Code, w.Body.String())
	}
	if prepares < 2 {
		t.Fatalf("expected a retry (>=2 prepares), saw %d", prepares)
	}
}

// contendingPeer rejects the first prepare with a higher ballot, then behaves
// like a normal accepting replica.
type contendingPeer struct {
	higher      paxos.Ballot
	rejectFirst *int
}

func (p *contendingPeer) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set(contentTypeHeader, contentTypeJSON)
		switch req.URL.Path {
		case pathPaxosPrepare:
			*p.rejectFirst++
			ok := *p.rejectFirst > 1
			resp := prepareResponse{Promise: paxos.Promise{OK: ok, Promised: p.higher}}
			_ = json.NewEncoder(w).Encode(resp)
		case pathPaxosPropose:
			_ = json.NewEncoder(w).Encode(paxos.Promise{OK: true})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestCASUnavailableWhenPrepareFailsWithoutHigherBallot(t *testing.T) {
	h := newSelfOnlyHandler(t)
	// Two unreachable peers: self's lone promise is below majority and no
	// higher ballot is reported, so the round fails closed (retryOrFail with
	// a zero ballot).
	h.memberList.Add("dead1", "127.0.0.1:1")
	h.memberList.Add("dead2", "127.0.0.1:1")
	w := doPut(h, "/keys/unavail?cas=true", `{"value":"v"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestSerialReadEmptyRingFailsClosed(t *testing.T) {
	h := newTestHandler(t) // no nodes in the ring
	if w := doSerialGet(h, "k"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestBallotString(t *testing.T) {
	if got := (paxos.Ballot{Counter: 7, Node: "n1"}).String(); got != "7@n1" {
		t.Errorf("Ballot.String() = %q, want 7@n1", got)
	}
}

// --- precondition and matching helpers --------------------------------------

func TestCASPreconditionHolds(t *testing.T) {
	v := store.VectorClockVersion{Clocks: map[string]uint64{"c": 2}}
	// Empty survivor set (absent key) always passes.
	if !casPreconditionHolds(nil, v) {
		t.Error("absent key must pass the precondition")
	}
	// A survivor the version dominates passes.
	dominated := []SiblingResponse{{Clocks: map[string]uint64{"c": 1}}}
	if !casPreconditionHolds(dominated, v) {
		t.Error("dominating version must pass")
	}
	// A concurrent survivor fails.
	concurrent := []SiblingResponse{{Clocks: map[string]uint64{"other": 1}}}
	if casPreconditionHolds(concurrent, v) {
		t.Error("a concurrent survivor must fail the precondition")
	}
}

func TestCASCommittedHereAndMutationMatches(t *testing.T) {
	v := store.VectorClockVersion{Clocks: map[string]uint64{"c": 2}}
	wr := keyWrite{value: "mine"}

	// An exact version+value match is recognized as this request's own write.
	survivors := []SiblingResponse{{Value: "mine", Clocks: map[string]uint64{"c": 2}}}
	if !casCommittedHere(survivors, v, wr) {
		t.Error("exact match must be recognized as committed here")
	}
	// A different value at the same clock is not this write.
	other := []SiblingResponse{{Value: "other", Clocks: map[string]uint64{"c": 2}}}
	if casCommittedHere(other, v, wr) {
		t.Error("a different value must not match")
	}
	// mutationMatches applies the same test to an accepted mutation.
	m := paxos.Mutation{Value: "mine", Clocks: map[string]uint64{"c": 2}}
	if !mutationMatches(m, v, wr) {
		t.Error("mutationMatches must accept the exact mutation")
	}
	if mutationMatches(paxos.Mutation{Value: "other", Clocks: map[string]uint64{"c": 2}}, v, wr) {
		t.Error("mutationMatches must reject a different value")
	}
}

func TestCASPreconditionFailedResponses(t *testing.T) {
	h := newTestHandler(t)

	// Not yet proposed: a plain 412, no side effect possible.
	w := httptest.NewRecorder()
	h.casPreconditionFailed(w, false)
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("unproposed: got %d, want 412", w.Code)
	}
	// Already proposed: degrade to a retryable 503, since the mutation may
	// have committed and been superseded.
	w = httptest.NewRecorder()
	h.casPreconditionFailed(w, true)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("proposed: got %d, want 503", w.Code)
	}
}

// TestBootstrapReplicaRangesPullsFromReplicas covers the finding-#10 rejoin
// path: a wiped node pulls its entire replica set (not just primary ranges)
// back from the peers that hold it. With RF=2 on a two-node ring, each node
// replicates the whole keyspace, so a key seeded only on the peer must land
// locally after the pull.
func TestBootstrapReplicaRangesPullsFromReplicas(t *testing.T) {
	peerSrv, peerHandler := newMigrationTestNode(t, "peer", 2, 1, 1)
	peerAddr := serverAddress(peerSrv)

	_, wipedHandler := newMigrationTestNode(t, "wiped", 2, 1, 1)
	for _, h := range []*Handler{peerHandler, wipedHandler} {
		h.ring.AddNode("peer", peerAddr)
		h.ring.AddNode("wiped", "127.0.0.1:0")
	}

	peerHandler.store.Put("replicated", "peer-value",
		store.VectorClockVersion{Clocks: map[string]uint64{"peer": 1}})

	wipedHandler.BootstrapReplicaRanges()

	entry, ok, _ := wipedHandler.store.Get("replicated")
	if !ok || len(entry.Siblings) != 1 || entry.Siblings[0].Value != "peer-value" {
		t.Fatalf("replica-range bootstrap must pull the key, got ok=%v %+v", ok, entry)
	}
}
