package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/hintstore"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

func newNamedTestServer(t *testing.T, selfID string) *httptest.Server {
	t.Helper()
	return newNamedTestServerQ(t, selfID, 3, 1, 2)
}

func newNamedTestServerQ(t *testing.T, selfID string, rf, wq, rq int) *httptest.Server {
	t.Helper()
	r := ring.NewRing(50)
	ml := gossip.NewMemberList(selfID, "", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
	s := store.New()
	srv := httptest.NewServer(NewServer(r, ml, s, HandlerConfig{SelfID: selfID, ReplicationFactor: rf, WriteQuorum: wq, ReadQuorum: rq, ReplicaTimeout: time.Second}, nil))
	t.Cleanup(func() { srv.Close() })
	return srv
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	r := ring.NewRing(50)
	ml := gossip.NewMemberList("self", "localhost", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
	s := store.New()
	srv := httptest.NewServer(NewServer(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil))
	t.Cleanup(func() { srv.Close() })
	return srv
}

func serverAddress(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

func postNode(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to post node: %v", err)
	}
	return resp
}


func TestE2EAddAndGetNode(t *testing.T) {
	srv := newTestServer(t)
	addr := serverAddress(srv)

	resp := postNode(t,
		fmt.Sprintf("%s/nodes", srv.URL),
		fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr), 
	)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}

	resp2, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp2.StatusCode)
	}

	var node NodeResponse
	if err := json.NewDecoder(resp2.Body).Decode(&node); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if node.ID != "node1" {
		t.Errorf("expected node1, got %s", node.ID)
	}
}

func TestE2EAddAndRemoveNode(t *testing.T) {
	srv := newTestServer(t)
	addr := serverAddress(srv)

	resp0 := postNode(t,
		fmt.Sprintf("%s/nodes", srv.URL),
		fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr), 
	)
	defer resp0.Body.Close()

	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/nodes/node1", srv.URL), nil)
	if err != nil {
		t.Fatalf("failed to create delete request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to remove node: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}

	resp2, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 after removal, got %d", resp2.StatusCode)
	}
}

func TestE2EGetNodes(t *testing.T) {
	srv := newTestServer(t)
	addr := serverAddress(srv) // ✅

	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "%s"}`, i, addr) 
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer r.Body.Close()
	}

	resp, err := http.Get(fmt.Sprintf("%s/nodes", srv.URL))
	if err != nil {
		t.Fatalf("failed to get nodes: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var nodes []NodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}
}

func TestE2EKeyConsistency(t *testing.T) {
	srv := newTestServer(t)
	addr := serverAddress(srv)

	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "%s"}`, i, addr)
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer r.Body.Close()
	}

	getNode := func() string {
		resp, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
		if err != nil {
			t.Fatalf("failed to get node: %v", err)
		}
		defer resp.Body.Close()

		var node NodeResponse
		if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
			t.Errorf("failed to decode response: %v", err)
		}
		return node.ID
	}

	first := getNode()
	second := getNode()

	if first != second {
		t.Errorf("expected consistent lookup, got %s then %s", first, second)
	}
}

func TestE2EGetStats(t *testing.T) {
	srv := newTestServer(t)
	addr := serverAddress(srv)

	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "%s"}`, i, addr)
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer r.Body.Close()
	}

	resp, err := http.Get(fmt.Sprintf("%s/stats", srv.URL))
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var stats ring.RingStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if stats.TotalNodes != 3 {
		t.Errorf("expected 3 nodes, got %d", stats.TotalNodes)
	}
	if stats.MostLoaded == "" {
		t.Error("expected most loaded to be set")
	}
	if stats.LeastLoaded == "" {
		t.Error("expected least loaded to be set")
	}
}

func TestE2EGetReplicationNodes(t *testing.T) {
	srv := newTestServer(t)
	addr := serverAddress(srv)

	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "%s"}`, i, addr) 
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer r.Body.Close()
	}

	body := `{"key": "somekey", "factor": 3}`
	resp, err := http.Post(fmt.Sprintf("%s/replicate", srv.URL), "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to get replication nodes: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result ReplicateResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(result.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(result.Nodes))
	}
	if result.Key != "somekey" {
		t.Errorf("expected somekey, got %s", result.Key)
	}
}

// TestE2ERingConvergence verifies that two servers with the same ring state
// always map any given key to the same primary node, exercising ring determinism.
func TestE2ERingConvergence(t *testing.T) {
	srv1 := newNamedTestServer(t, "node1")
	srv2 := newNamedTestServer(t, "node2")
	addr1 := serverAddress(srv1)
	addr2 := serverAddress(srv2)

	// Register both nodes in both rings.
	for _, srv := range []*httptest.Server{srv1, srv2} {
		r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
		defer r1.Body.Close()
		r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node2", "address": "%s"}`, addr2))
		defer r2.Body.Close()
	}

	keys := []string{"user:123", "order:456", "session:789", "product:abc"}
	for _, key := range keys {
		checkKeyConvergence(t, srv1.URL, srv2.URL, key)
	}
}

func checkKeyConvergence(t *testing.T, srv1URL, srv2URL, key string) {
	t.Helper()
	resp1, err := http.Get(fmt.Sprintf("%s/keys/%s", srv1URL, key))
	if err != nil {
		t.Fatalf("srv1 lookup failed: %v", err)
	}
	defer resp1.Body.Close()

	resp2, err := http.Get(fmt.Sprintf("%s/keys/%s", srv2URL, key))
	if err != nil {
		t.Fatalf("srv2 lookup failed: %v", err)
	}
	defer resp2.Body.Close()

	var n1, n2 NodeResponse
	if err := json.NewDecoder(resp1.Body).Decode(&n1); err != nil {
		t.Fatalf("failed to decode srv1 response for key %q: %v", key, err)
	}
	if err := json.NewDecoder(resp2.Body).Decode(&n2); err != nil {
		t.Fatalf("failed to decode srv2 response for key %q: %v", key, err)
	}

	if n1.ID != n2.ID {
		t.Errorf("key %q: ring divergence - srv1=%s srv2=%s", key, n1.ID, n2.ID)
	}
}

// TestE2EGossipMembershipPropagates verifies that a node added to one server
// appears in another server's ring after a gossip exchange via POST /gossip.
func TestE2EGossipMembershipPropagates(t *testing.T) {
	srv1 := newTestServer(t)
	srv2 := newTestServer(t)

	// Add a node to srv1.
	r := postNode(t, fmt.Sprintf("%s/nodes", srv1.URL), `{"id": "nodeA", "address": "10.0.0.1:8080"}`)
	defer r.Body.Close()

	// Pull srv1's full member list via the gossip endpoint (empty push triggers a pull).
	pullResp, err := http.Post(fmt.Sprintf("%s/gossip", srv1.URL), "application/json",
		bytes.NewBufferString(`{"members": []}`))
	if err != nil {
		t.Fatalf("failed to pull members from srv1: %v", err)
	}
	defer pullResp.Body.Close()

	var members []*gossip.Member
	if err := json.NewDecoder(pullResp.Body).Decode(&members); err != nil {
		t.Fatalf("failed to decode members: %v", err)
	}

	// Push srv1's members into srv2.
	body, err := json.Marshal(map[string]any{"members": members})
	if err != nil {
		t.Fatalf("failed to marshal members: %v", err)
	}
	pushResp, err := http.Post(fmt.Sprintf("%s/gossip", srv2.URL), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to push members to srv2: %v", err)
	}
	defer pushResp.Body.Close()

	// Verify nodeA is now in srv2's ring.
	nodesResp, err := http.Get(fmt.Sprintf("%s/nodes", srv2.URL))
	if err != nil {
		t.Fatalf("failed to get nodes from srv2: %v", err)
	}
	defer nodesResp.Body.Close()

	var nodes []NodeResponse
	if err := json.NewDecoder(nodesResp.Body).Decode(&nodes); err != nil {
		t.Fatalf("failed to decode nodes: %v", err)
	}

	found := false
	for _, n := range nodes {
		if n.ID == "nodeA" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("nodeA not found in srv2's ring after gossip exchange")
	}
}


// TestE2EWriteAndRead verifies that a PUT stores a value that a subsequent GET
// returns on the same node.
func TestE2EWriteAndRead(t *testing.T) {
	srv := newNamedTestServer(t, "node1")
	addr := serverAddress(srv)

	// Register self so GET /keys/:key resolves to this node.
	r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr))
	defer r.Body.Close()

	body := `{"value": "hello"}`
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/keys/mykey", srv.URL), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to create PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", putResp.StatusCode)
	}

	getResp, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}

	var node NodeResponse
	if err := json.NewDecoder(getResp.Body).Decode(&node); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if node.Value != "hello" {
		t.Errorf("expected value 'hello', got %q", node.Value)
	}
}

// TestE2EReplicationFanOut verifies that a write to one node is replicated to
// other nodes in the replica set.
func TestE2EReplicationFanOut(t *testing.T) {
	srv1 := newNamedTestServer(t, "node1")
	srv2 := newNamedTestServer(t, "node2")
	addr1 := serverAddress(srv1)
	addr2 := serverAddress(srv2)

	// Register both nodes in both rings.
	for _, srv := range []*httptest.Server{srv1, srv2} {
		r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
		defer r1.Body.Close()
		r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node2", "address": "%s"}`, addr2))
		defer r2.Body.Close()
	}

	// Write to node1.
	body := `{"value": "replicated"}`
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/keys/rkey", srv1.URL), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to create PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", putResp.StatusCode)
	}

	// Give the async fan-out time to complete.
	time.Sleep(50 * time.Millisecond)

	// Both nodes should return the value for the key regardless of which owns it.
	for _, srv := range []*httptest.Server{srv1, srv2} {
		getResp, err := http.Get(fmt.Sprintf("%s/keys/rkey", srv.URL))
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		defer getResp.Body.Close()
		if getResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", getResp.StatusCode)
		}
		var node NodeResponse
		if err := json.NewDecoder(getResp.Body).Decode(&node); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if node.Value != "replicated" {
			t.Errorf("srv %s: expected value 'replicated', got %q", srv.URL, node.Value)
		}
	}
}

// TestE2EQuorumWriteSuccess verifies that a write with quorum=2 waits for
// acknowledgment from both nodes before returning 204, so both nodes have
// the value without needing a sleep.
func TestE2EQuorumWriteSuccess(t *testing.T) {
	srv1 := newNamedTestServerQ(t, "node1", 2, 2, 1)
	srv2 := newNamedTestServerQ(t, "node2", 2, 2, 1)
	addr1 := serverAddress(srv1)
	addr2 := serverAddress(srv2)

	for _, srv := range []*httptest.Server{srv1, srv2} {
		r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
		defer r1.Body.Close()
		r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node2", "address": "%s"}`, addr2))
		defer r2.Body.Close()
	}

	body := `{"value": "quorum-value"}`
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/keys/qkey", srv1.URL), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to create PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", putResp.StatusCode)
	}

	// No sleep needed: quorum=2 means PUT only returned after srv2 ack'd.
	for _, srv := range []*httptest.Server{srv1, srv2} {
		getResp, err := http.Get(fmt.Sprintf("%s/keys/qkey", srv.URL))
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		defer getResp.Body.Close()
		var node NodeResponse
		if err := json.NewDecoder(getResp.Body).Decode(&node); err != nil {
			t.Fatalf("failed to decode: %v", err)
		}
		if node.Value != "quorum-value" {
			t.Errorf("srv %s: expected 'quorum-value', got %q", srv.URL, node.Value)
		}
	}
}

// TestE2EQuorumWriteFailure verifies that a write returns 503 when a required
// replica is unreachable and quorum cannot be met.
func TestE2EQuorumWriteFailure(t *testing.T) {
	srv := newNamedTestServerQ(t, "node1", 2, 2, 1)
	addr1 := serverAddress(srv)

	r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
	defer r1.Body.Close()
	// Register a second node that is unreachable.
	r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), `{"id": "node2", "address": "localhost:19999"}`)
	defer r2.Body.Close()

	body := `{"value": "should-fail"}`
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/keys/fkey", srv.URL), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to create PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when quorum unreachable, got %d", putResp.StatusCode)
	}
}

// TestE2EConsistentRead verifies that a read with quorum=2 returns the entry
// with the highest vector clock when replicas hold different versions.
func TestE2EConsistentRead(t *testing.T) {
	srv1 := newNamedTestServerQ(t, "node1", 2, 1, 2)
	srv2 := newNamedTestServerQ(t, "node2", 2, 1, 2)
	addr1 := serverAddress(srv1)
	addr2 := serverAddress(srv2)

	for _, srv := range []*httptest.Server{srv1, srv2} {
		r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
		defer r1.Body.Close()
		r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node2", "address": "%s"}`, addr2))
		defer r2.Body.Close()
	}

	// Seed node1 with an older version via a replica write (bypasses fan-out).
	seedWrite := func(t *testing.T, srvURL, value, clocksJSON string) {
		t.Helper()
		body := fmt.Sprintf(`{"value": %q, "clocks": %s}`, value, clocksJSON)
		req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/keys/ck", srvURL), strings.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Proxied-From", "test")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("seed write failed: %v", err)
		}
		defer resp.Body.Close()
	}

	seedWrite(t, srv1.URL, "old", `{"node1": 1}`)
	seedWrite(t, srv2.URL, "new", `{"node1": 1, "node2": 1}`)

	// Consistent read from node1 should return the newer value from node2.
	getResp, err := http.Get(fmt.Sprintf("%s/keys/ck", srv1.URL))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}
	var node NodeResponse
	if err := json.NewDecoder(getResp.Body).Decode(&node); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if node.Value != "new" {
		t.Errorf("expected 'new' (highest clock), got %q", node.Value)
	}
}

// TestE2EReadQuorumFailure verifies that a read returns 503 when a required
// replica is unreachable and read quorum cannot be met.
func TestE2EReadQuorumFailure(t *testing.T) {
	srv := newNamedTestServerQ(t, "node1", 2, 1, 2)
	addr1 := serverAddress(srv)

	r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
	defer r1.Body.Close()
	r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), `{"id": "node2", "address": "localhost:19999"}`)
	defer r2.Body.Close()

	getResp, err := http.Get(fmt.Sprintf("%s/keys/rqkey", srv.URL))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when read quorum unreachable, got %d", getResp.StatusCode)
	}
}

// newHintTestNode creates a test server backed by a real Handler and returns
// both so tests can inspect the handler's ring, store, and hint delivery.
// RF=3, WQ=2, RQ=2 by default (realistic quorum settings for hint tests).
func newHintTestNode(t *testing.T, selfID string, hs *hintstore.HintStore) (*httptest.Server, *Handler) {
	t.Helper()
	r := ring.NewRing(50)
	ml := gossip.NewMemberList(selfID, "", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: selfID, ReplicationFactor: 3, WriteQuorum: 2, ReadQuorum: 2, ReplicaTimeout: time.Second}, hs)
	srv := httptest.NewServer(newMux(h))
	t.Cleanup(func() { srv.Close() })
	return srv, h
}

// awaitPendingHint polls the hint store until nodeID has buffered hints or
// the deadline passes.
func awaitPendingHint(t *testing.T, hs *hintstore.HintStore, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, id := range hs.PendingNodes() {
			if id == nodeID {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for hint for node %q; pending: %v", nodeID, hs.PendingNodes())
}

// TestHintedHandoff_PutHintStoredAndDelivered verifies the full hinted handoff
// flow for a write: when a replica is down during a PUT, a hint is buffered;
// when the node recovers and DeliverHints is called, the write is replayed.
func TestHintedHandoff_PutHintStoredAndDelivered(t *testing.T) {
	hs := hintstore.New(1000, time.Hour)

	node1Srv, node1Handler := newHintTestNode(t, "node1", hs)
	addr1 := serverAddress(node1Srv)

	// node3 is a real alive replica that can accept proxied writes.
	node3Srv, _ := newHintTestNode(t, "node3", nil)
	addr3 := serverAddress(node3Srv)

	// node2 is simulated as down — returns 503 for any request.
	downSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "node2 is down", http.StatusServiceUnavailable)
	}))
	t.Cleanup(downSrv.Close)
	addr2 := serverAddress(downSrv)

	// Add node1 to its own ring (mirrors what main.go does).
	node1Handler.ring.AddNode("node1", addr1)
	// Register node2 and node3 in node1's ring via the HTTP API.
	r2 := postNode(t, node1Srv.URL+"/nodes", fmt.Sprintf(`{"id":"node2","address":%q}`, addr2))
	defer r2.Body.Close()
	r3 := postNode(t, node1Srv.URL+"/nodes", fmt.Sprintf(`{"id":"node3","address":%q}`, addr3))
	defer r3.Body.Close()

	// PUT a key; quorum (node1+node3=2) is met, node2 gets a hint.
	req, err := http.NewRequest(http.MethodPut, node1Srv.URL+"/keys/hint-key",
		strings.NewReader(`{"value":"hinted-value"}`))
	if err != nil {
		t.Fatalf("failed to create PUT: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	// Wait for the hint (may arrive via background goroutine).
	awaitPendingHint(t, hs, "node2")

	// Bring node2 back online with a fresh handler.
	node2Srv, node2Handler := newHintTestNode(t, "node2", nil)
	recoveredAddr2 := serverAddress(node2Srv)

	// Deliver buffered hints from node1 to recovered node2.
	node1Handler.DeliverHints("node2", recoveredAddr2)

	// node2's store should now contain the hinted write.
	entry, ok := node2Handler.store.Get("hint-key")
	if !ok {
		t.Fatal("recovered node2 should have 'hint-key' after hint delivery")
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "hinted-value" {
		t.Errorf("unexpected entry in recovered node2: %+v", entry)
	}

	// Hint store should be drained after successful delivery.
	if nodes := hs.PendingNodes(); len(nodes) != 0 {
		t.Errorf("hint store should be empty after delivery, still has: %v", nodes)
	}
}

// TestHintedHandoff_DeleteHintDelivered verifies that a tombstone (delete) is
// also buffered as a hint and replayed correctly when the node recovers.
func TestHintedHandoff_DeleteHintDelivered(t *testing.T) {
	hs := hintstore.New(1000, time.Hour)

	node1Srv, node1Handler := newHintTestNode(t, "node1", hs)
	addr1 := serverAddress(node1Srv)

	node3Srv, _ := newHintTestNode(t, "node3", nil)
	addr3 := serverAddress(node3Srv)

	downSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "node2 is down", http.StatusServiceUnavailable)
	}))
	t.Cleanup(downSrv.Close)
	addr2 := serverAddress(downSrv)

	node1Handler.ring.AddNode("node1", addr1)
	r2 := postNode(t, node1Srv.URL+"/nodes", fmt.Sprintf(`{"id":"node2","address":%q}`, addr2))
	defer r2.Body.Close()
	r3 := postNode(t, node1Srv.URL+"/nodes", fmt.Sprintf(`{"id":"node3","address":%q}`, addr3))
	defer r3.Body.Close()

	// DELETE a key while node2 is down.
	req, err := http.NewRequest(http.MethodDelete, node1Srv.URL+"/keys/delete-key",
		strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("failed to create DELETE: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	awaitPendingHint(t, hs, "node2")

	// Verify the hint is a tombstone.
	hints := hs.Drain("node2")
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(hints))
	}
	if !hints[0].Deleted {
		t.Errorf("expected Deleted=true in delete hint, got %+v", hints[0])
	}
	// Re-store so DeliverHints can drain it.
	hs.Store("node2", hints[0])

	node2Srv, node2Handler := newHintTestNode(t, "node2", nil)
	recoveredAddr2 := serverAddress(node2Srv)

	node1Handler.DeliverHints("node2", recoveredAddr2)

	// Recovered node2 should have a tombstone for delete-key.
	entry, ok := node2Handler.store.Get("delete-key")
	if !ok {
		t.Fatal("recovered node2 should have tombstone for 'delete-key'")
	}
	if len(entry.Siblings) != 1 || !entry.Siblings[0].Deleted {
		t.Errorf("expected a tombstone, got: %+v", entry)
	}
}

// TestE2EReadRepair verifies that after a quorum read, a stale replica is
// asynchronously repaired with the canonical merged value. node1 holds an
// old version; node2 holds a newer one. The coordinator (node1) must return
// "new" and then repair its own local store without an additional write.
func TestE2EReadRepair(t *testing.T) {
	node1Srv, node1Handler := newHintTestNode(t, "node1", nil)
	node2Srv, _ := newHintTestNode(t, "node2", nil)
	addr1 := serverAddress(node1Srv)
	addr2 := serverAddress(node2Srv)

	// Register both nodes in both rings so quorum reads contact both.
	for _, srv := range []*httptest.Server{node1Srv, node2Srv} {
		r1 := postNode(t, srv.URL+"/nodes", fmt.Sprintf(`{"id":"node1","address":%q}`, addr1))
		defer r1.Body.Close()
		r2 := postNode(t, srv.URL+"/nodes", fmt.Sprintf(`{"id":"node2","address":%q}`, addr2))
		defer r2.Body.Close()
	}

	// node1 is stale (old clock); node2 has the dominant version.
	seedDirectWrite(t, node1Srv.URL, "old", `{"node1":1}`)
	seedDirectWrite(t, node2Srv.URL, "new", `{"node1":1,"node2":1}`)

	// Quorum GET from node1 reads both replicas, returns the newer value, and
	// fires an async goroutine to repair node1's local store.
	getResp, err := http.Get(node1Srv.URL + "/keys/rr-key")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}
	var result NodeResponse
	if err := json.NewDecoder(getResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Value != "new" {
		t.Fatalf("expected 'new' from quorum read, got %q", result.Value)
	}

	// Repair is launched in a goroutine; poll until node1's store holds the
	// new value (direct store write, so very fast in practice).
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if entry, ok := node1Handler.store.Get("rr-key"); ok {
			if len(entry.Siblings) == 1 && entry.Siblings[0].Value == "new" {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	entry, _ := node1Handler.store.Get("rr-key")
	t.Errorf("node1 not repaired within 200ms; store: %+v", entry)
}

// TestE2EConflictSurfacing verifies that concurrent writes (neither clock
// happens-before the other) are returned as siblings rather than silently
// resolved, and that a subsequent dominating write clears the conflict.
func TestE2EConflictSurfacing(t *testing.T) {
	srv1 := newNamedTestServerQ(t, "node1", 2, 1, 2)
	srv2 := newNamedTestServerQ(t, "node2", 2, 1, 2)
	addr1 := serverAddress(srv1)
	addr2 := serverAddress(srv2)

	for _, srv := range []*httptest.Server{srv1, srv2} {
		r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
		defer r1.Body.Close()
		r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node2", "address": "%s"}`, addr2))
		defer r2.Body.Close()
	}

	seedWrite := func(t *testing.T, srvURL, value, clocksJSON string) {
		t.Helper()
		body := fmt.Sprintf(`{"value": %q, "clocks": %s}`, value, clocksJSON)
		req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/keys/conflict-key", srvURL), strings.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Proxied-From", "test")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("seed write failed: %v", err)
		}
		defer resp.Body.Close()
	}

	// Seed concurrent writes: neither clock happens-before the other.
	seedWrite(t, srv1.URL, "alice", `{"node1": 1}`)
	seedWrite(t, srv2.URL, "bob", `{"node2": 1}`)

	// Consistent read should surface both as siblings.
	getResp, err := http.Get(fmt.Sprintf("%s/keys/conflict-key", srv1.URL))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}
	var node NodeResponse
	if err := json.NewDecoder(getResp.Body).Decode(&node); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if len(node.Siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d (value=%q)", len(node.Siblings), node.Value)
	}
	sibValues := map[string]bool{node.Siblings[0].Value: true, node.Siblings[1].Value: true}
	if !sibValues["alice"] || !sibValues["bob"] {
		t.Errorf("expected siblings 'alice' and 'bob', got %v", sibValues)
	}

	// A write whose clock dominates both siblings resolves the conflict.
	seedWrite(t, srv1.URL, "resolved", `{"node1": 1, "node2": 1}`)
	seedWrite(t, srv2.URL, "resolved", `{"node1": 1, "node2": 1}`)

	getResp2, err := http.Get(fmt.Sprintf("%s/keys/conflict-key", srv1.URL))
	if err != nil {
		t.Fatalf("GET after resolution failed: %v", err)
	}
	defer getResp2.Body.Close()
	var resolved NodeResponse
	if err := json.NewDecoder(getResp2.Body).Decode(&resolved); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if resolved.Value != "resolved" {
		t.Errorf("expected 'resolved' after conflict resolution, got %q (siblings=%v)", resolved.Value, resolved.Siblings)
	}
}

func seedDirectWrite(t *testing.T, srvURL, value, clocksJSON string) {
	t.Helper()
	body := fmt.Sprintf(`{"value":%q,"clocks":%s}`, value, clocksJSON)
	req, err := http.NewRequest(http.MethodPut, srvURL+"/keys/rr-key", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Proxied-From", "test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed write: %v", err)
	}
	defer resp.Body.Close()
}