package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// okReplica is a stand-in replica that accepts writes and serves an empty read.
func okReplica(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set(contentTypeHeader, contentTypeJSON)
			_ = json.NewEncoder(w).Encode(NodeResponse{ID: "peer", Status: "alive"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// deadReplica is a replica that fails every request, standing in for a node or
// whole data center that is unreachable.
func deadReplica(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func hostOf(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// newMultiDCHandler builds a coordinator in us-east under a per-DC table of
// {us-east:3, eu-west:3}: self plus two us-east peers and three eu-west peers.
// Each side's peers point at a live or dead server so a test can take down a
// whole data center.
func newMultiDCHandler(t *testing.T, localUp, remoteUp bool) *Handler {
	t.Helper()
	local, remote := deadReplica(t), deadReplica(t)
	if localUp {
		local = okReplica(t)
	}
	if remoteUp {
		remote = okReplica(t)
	}

	r := ring.NewRing(10)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})
	ml := newTestMemberList(r)
	h := NewHandler(r, ml, store.New(), HandlerConfig{
		SelfID: "self", SelfDC: "us-east",
		ReplicationFactor: 6, WriteQuorum: 4, ReadQuorum: 4,
		ReplicaTimeout: time.Second,
	}, nil)

	r.AddZonedNodeDC("self", "localhost:8080", "us-east", "rack1", 1.0)
	r.AddZonedNodeDC("east-2", hostOf(local), "us-east", "rack2", 1.0)
	r.AddZonedNodeDC("east-3", hostOf(local), "us-east", "rack3", 1.0)
	r.AddZonedNodeDC("west-1", hostOf(remote), "eu-west", "rack1", 1.0)
	r.AddZonedNodeDC("west-2", hostOf(remote), "eu-west", "rack2", 1.0)
	r.AddZonedNodeDC("west-3", hostOf(remote), "eu-west", "rack3", 1.0)
	return h
}

func putWithConsistency(t *testing.T, h *Handler, key, level string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/keys/"+key+"?consistency="+level, bytes.NewBufferString(`{"value":"v"}`))
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	return w.Code
}

func TestRequestedQuorum_LocalLevels(t *testing.T) {
	// Arrange: us-east target is 3, so local_quorum needs 2 and local_one 1.
	h := newMultiDCHandler(t, true, true)

	cases := []struct {
		level     string
		wantSize  int
		wantLocal bool
	}{
		{consistencyLocalOne, 1, true},
		{consistencyLocalQuorum, 2, true},
		{consistencyQuorum, 4, false}, // cluster-wide: RF 6 -> 4
		{consistencyAll, 6, false},
	}
	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/keys/k?consistency="+tc.level, nil)

			// Act
			got, err := h.requestedQuorum(req, 4)

			// Assert
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.size != tc.wantSize {
				t.Errorf("size = %d, want %d", got.size, tc.wantSize)
			}
			if got.localOnly != tc.wantLocal {
				t.Errorf("localOnly = %v, want %v", got.localOnly, tc.wantLocal)
			}
		})
	}
}

func TestRequestedQuorum_LocalLevelsRejectedWithoutDCTable(t *testing.T) {
	// A single-DC cluster has no per-DC target to size a local quorum against,
	// and local_quorum there would just be quorum — so asking for it signals a
	// misconfiguration rather than something to silently reinterpret.
	h := newTestHandler(t) // no SelfDC, no per-DC table

	for _, level := range []string{consistencyLocalOne, consistencyLocalQuorum} {
		req := httptest.NewRequest(http.MethodGet, "/keys/k?consistency="+level, nil)

		if _, err := h.requestedQuorum(req, 2); err == nil {
			t.Errorf("consistency=%s: expected an error without a per-DC table", level)
		}
	}
}

func TestRequestedQuorum_LocalLevelsRejectedWhenDCUnlisted(t *testing.T) {
	// The node is labeled, but its DC carries no replicas, so it can never
	// satisfy a local quorum.
	r := ring.NewRing(10)
	r.SetDCReplication(map[string]int{"eu-west": 3})
	h := NewHandler(r, newTestMemberList(r), store.New(), HandlerConfig{
		SelfID: "self", SelfDC: "ap-south", ReplicationFactor: 3,
		WriteQuorum: 2, ReadQuorum: 2, ReplicaTimeout: time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/keys/k?consistency="+consistencyLocalQuorum, nil)

	if _, err := h.requestedQuorum(req, 2); err == nil {
		t.Error("expected an error when SELF_DC is absent from the per-DC table")
	}
}

func TestPutKey_LocalQuorumSurvivesRemoteDCOutage(t *testing.T) {
	// The whole of eu-west is unreachable. local_quorum needs 2 acks from
	// us-east — self plus one live local peer — so the write still succeeds.
	// This is the availability win the feature exists for.
	h := newMultiDCHandler(t, true, false)

	if code := putWithConsistency(t, h, "k", consistencyLocalQuorum); code != http.StatusNoContent {
		t.Errorf("local_quorum write got %d, want 204 with the remote DC down", code)
	}
}

func TestPutKey_ClusterQuorumFailsOnRemoteDCOutage(t *testing.T) {
	// Same outage, cluster-wide quorum: 4 of 6 acks are unreachable when a
	// whole DC is down, so the write fails. The contrast with local_quorum
	// above is the point of the level.
	h := newMultiDCHandler(t, true, false)

	if code := putWithConsistency(t, h, "k", consistencyQuorum); code != http.StatusServiceUnavailable {
		t.Errorf("cluster quorum write got %d, want 503 with the remote DC down", code)
	}
}

func TestPutKey_LocalQuorumFailsWhenLocalDCIsDown(t *testing.T) {
	// Remote DC healthy, local peers dead. A remote ack cannot satisfy a local
	// quorum, so the write fails rather than quietly counting eu-west.
	h := newMultiDCHandler(t, false, true)

	if code := putWithConsistency(t, h, "k", consistencyLocalQuorum); code != http.StatusServiceUnavailable {
		t.Errorf("local_quorum write got %d, want 503 when the local DC is down", code)
	}
}

func TestPutKey_LocalOneSatisfiedByCoordinatorAlone(t *testing.T) {
	// local_one needs a single local ack, and the coordinator's own write is
	// one, so it succeeds even with every peer in both DCs unreachable.
	h := newMultiDCHandler(t, false, false)

	if code := putWithConsistency(t, h, "k", consistencyLocalOne); code != http.StatusNoContent {
		t.Errorf("local_one write got %d, want 204", code)
	}
}

func TestPutKey_LocalQuorumStillReplicatesToRemoteDC(t *testing.T) {
	// local_quorum narrows the counting, not the fan-out: eu-west must still
	// receive the write, or "local" would silently mean "local-only" and the
	// remote DC would depend entirely on anti-entropy.
	received := make(chan string, 8)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			select {
			case received <- r.URL.Path:
			default:
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(remote.Close)
	local := okReplica(t)

	r := ring.NewRing(10)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})
	h := NewHandler(r, newTestMemberList(r), store.New(), HandlerConfig{
		SelfID: "self", SelfDC: "us-east", ReplicationFactor: 6,
		WriteQuorum: 4, ReadQuorum: 4, ReplicaTimeout: time.Second,
	}, nil)
	r.AddZonedNodeDC("self", "localhost:8080", "us-east", "rack1", 1.0)
	r.AddZonedNodeDC("east-2", hostOf(local), "us-east", "rack2", 1.0)
	r.AddZonedNodeDC("east-3", hostOf(local), "us-east", "rack3", 1.0)
	r.AddZonedNodeDC("west-1", hostOf(remote), "eu-west", "rack1", 1.0)
	r.AddZonedNodeDC("west-2", hostOf(remote), "eu-west", "rack2", 1.0)
	r.AddZonedNodeDC("west-3", hostOf(remote), "eu-west", "rack3", 1.0)

	// Act
	if code := putWithConsistency(t, h, "fanout", consistencyLocalQuorum); code != http.StatusNoContent {
		t.Fatalf("local_quorum write got %d, want 204", code)
	}

	// Assert: the remote DC receives the write even though it never counted.
	select {
	case path := <-received:
		if path != "/keys/fanout" {
			t.Errorf("remote replica got path %q, want /keys/fanout", path)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("remote DC never received the local_quorum write")
	}
}

func TestGetKey_LocalQuorumSurvivesRemoteDCOutage(t *testing.T) {
	// Arrange: seed a value locally, then take eu-west down.
	h := newMultiDCHandler(t, true, false)
	if code := putWithConsistency(t, h, "rk", consistencyLocalOne); code != http.StatusNoContent {
		t.Fatalf("seed write got %d, want 204", code)
	}

	// Act
	req := httptest.NewRequest(http.MethodGet, "/keys/rk?consistency="+consistencyLocalQuorum, nil)
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("local_quorum read got %d, want 200 with the remote DC down", w.Code)
	}
}

func TestGetKey_ClusterQuorumFailsOnRemoteDCOutage(t *testing.T) {
	// The same read at cluster-wide quorum cannot reach 4 of 6 replicas.
	h := newMultiDCHandler(t, true, false)
	if code := putWithConsistency(t, h, "rk", consistencyLocalOne); code != http.StatusNoContent {
		t.Fatalf("seed write got %d, want 204", code)
	}

	req := httptest.NewRequest(http.MethodGet, "/keys/rk?consistency="+consistencyQuorum, nil)
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("cluster quorum read got %d, want 503 with the remote DC down", w.Code)
	}
}

func TestAckCounter_ScopesToCoordinatorDC(t *testing.T) {
	// Arrange
	h := newMultiDCHandler(t, true, true)
	nodes := []*ring.Node{
		{ID: "east-2", DC: "us-east"},
		{ID: "west-1", DC: "eu-west"},
	}

	// Act
	clusterWide := h.ackCounter(quorumSpec{size: 2}, nodes)
	localOnly := h.ackCounter(quorumSpec{size: 2, localOnly: true}, nodes)

	// Assert
	if !clusterWide("west-1") || !clusterWide("east-2") {
		t.Error("cluster-wide counter rejected a replica")
	}
	if !localOnly("east-2") {
		t.Error("local counter rejected a same-DC replica")
	}
	if localOnly("west-1") {
		t.Error("local counter accepted a remote-DC replica")
	}
}

func TestCountable_CountsOnlyLocalUnderLocalLevel(t *testing.T) {
	h := newMultiDCHandler(t, true, true)
	nodes := []*ring.Node{
		{ID: "east-2", DC: "us-east"},
		{ID: "east-3", DC: "us-east"},
		{ID: "west-1", DC: "eu-west"},
	}

	if got := h.countable(quorumSpec{}, nodes); got != 3 {
		t.Errorf("cluster-wide countable = %d, want 3", got)
	}
	if got := h.countable(quorumSpec{localOnly: true}, nodes); got != 2 {
		t.Errorf("local countable = %d, want 2", got)
	}
}
