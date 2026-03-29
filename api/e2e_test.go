package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
)

func newNamedTestServer(t *testing.T, selfID string) *httptest.Server {
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
	transport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	g := gossip.NewGossiper(selfID, "0", ml, transport)
	srv := httptest.NewServer(NewServer(r, ml, g, selfID))
	t.Cleanup(func() {
		srv.Close()
		transport.Stop()
	})
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
	transport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	g := gossip.NewGossiper("self", "0", ml, transport)
	srv := httptest.NewServer(NewServer(r, ml, g, "self"))
	t.Cleanup(func() {
		srv.Close()
		transport.Stop()
	})
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

func closeBody(t *testing.T, resp *http.Response) {
	t.Helper()
	if err := resp.Body.Close(); err != nil {
		t.Errorf("failed to close body: %v", err)
	}
}

func TestE2EAddAndGetNode(t *testing.T) {
	srv := newTestServer(t)
	addr := serverAddress(srv)

	resp := postNode(t,
		fmt.Sprintf("%s/nodes", srv.URL),
		fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr), 
	)
	defer closeBody(t, resp)

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}

	resp2, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	defer closeBody(t, resp2)

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
	defer closeBody(t, resp0)

	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/nodes/node1", srv.URL), nil)
	if err != nil {
		t.Fatalf("failed to create delete request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to remove node: %v", err)
	}
	defer closeBody(t, resp)

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}

	resp2, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	defer closeBody(t, resp2)

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
		defer closeBody(t, r)
	}

	resp, err := http.Get(fmt.Sprintf("%s/nodes", srv.URL))
	if err != nil {
		t.Fatalf("failed to get nodes: %v", err)
	}
	defer closeBody(t, resp)

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
		defer closeBody(t, r)
	}

	getNode := func() string {
		resp, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
		if err != nil {
			t.Fatalf("failed to get node: %v", err)
		}
		defer closeBody(t, resp)

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
		defer closeBody(t, r)
	}

	resp, err := http.Get(fmt.Sprintf("%s/stats", srv.URL))
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	defer closeBody(t, resp)

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
		defer closeBody(t, r)
	}

	body := `{"key": "somekey", "factor": 3}`
	resp, err := http.Post(fmt.Sprintf("%s/replicate", srv.URL), "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to get replication nodes: %v", err)
	}
	defer closeBody(t, resp)

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
// always map any given key to the same node. Each server is registered in the
// other's ring so proxying works, exercising both ring determinism and routing.
func TestE2ERingConvergence(t *testing.T) {
	srv1 := newNamedTestServer(t, "node1")
	srv2 := newNamedTestServer(t, "node2")
	addr1 := serverAddress(srv1)
	addr2 := serverAddress(srv2)

	// Register both nodes in both rings.
	for _, srv := range []*httptest.Server{srv1, srv2} {
		r1 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node1", "address": "%s"}`, addr1))
		defer closeBody(t, r1)
		r2 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), fmt.Sprintf(`{"id": "node2", "address": "%s"}`, addr2))
		defer closeBody(t, r2)
	}

	keys := []string{"user:123", "order:456", "session:789", "product:abc"}
	for _, key := range keys {
		resp1, err := http.Get(fmt.Sprintf("%s/keys/%s", srv1.URL, key))
		if err != nil {
			t.Fatalf("srv1 lookup failed: %v", err)
		}
		resp2, err := http.Get(fmt.Sprintf("%s/keys/%s", srv2.URL, key))
		if err != nil {
			t.Fatalf("srv2 lookup failed: %v", err)
		}

		var n1, n2 NodeResponse
		if err := json.NewDecoder(resp1.Body).Decode(&n1); err != nil {
			t.Fatalf("failed to decode srv1 response for key %q: %v", key, err)
		}
		if err := json.NewDecoder(resp2.Body).Decode(&n2); err != nil {
			t.Fatalf("failed to decode srv2 response for key %q: %v", key, err)
		}
		closeBody(t, resp1)
		closeBody(t, resp2)

		if n1.ID != n2.ID {
			t.Errorf("key %q: ring divergence — srv1=%s srv2=%s", key, n1.ID, n2.ID)
		}
	}
}

// TestE2EGossipMembershipPropagates verifies that a node added to one server
// appears in another server's ring after a gossip exchange via POST /gossip.
func TestE2EGossipMembershipPropagates(t *testing.T) {
	srv1 := newTestServer(t)
	srv2 := newTestServer(t)

	// Add a node to srv1.
	r := postNode(t, fmt.Sprintf("%s/nodes", srv1.URL), `{"id": "nodeA", "address": "10.0.0.1:8080"}`)
	defer closeBody(t, r)

	// Pull srv1's full member list via the gossip endpoint (empty push triggers a pull).
	pullResp, err := http.Post(fmt.Sprintf("%s/gossip", srv1.URL), "application/json",
		bytes.NewBufferString(`{"members": []}`))
	if err != nil {
		t.Fatalf("failed to pull members from srv1: %v", err)
	}
	defer closeBody(t, pullResp)

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
	defer closeBody(t, pushResp)

	// Verify nodeA is now in srv2's ring.
	nodesResp, err := http.Get(fmt.Sprintf("%s/nodes", srv2.URL))
	if err != nil {
		t.Fatalf("failed to get nodes from srv2: %v", err)
	}
	defer closeBody(t, nodesResp)

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

// TestE2EPeerRouting verifies that a key lookup on a server that does not own
// the key is transparently proxied to the peer that does.
func TestE2EPeerRouting(t *testing.T) {
	srv1 := newNamedTestServer(t, "srv1")
	srv2 := newNamedTestServer(t, "srv2")
	srv2Addr := serverAddress(srv2)

	// Register srv2 in srv1's ring. srv1 does not register itself, so any key
	// lookup will route to srv2.
	r1 := postNode(t, fmt.Sprintf("%s/nodes", srv1.URL),
		fmt.Sprintf(`{"id": "srv2", "address": "%s"}`, srv2Addr))
	defer closeBody(t, r1)

	// Register srv2 in its own ring so it can serve requests directly.
	r2 := postNode(t, fmt.Sprintf("%s/nodes", srv2.URL),
		fmt.Sprintf(`{"id": "srv2", "address": "%s"}`, srv2Addr))
	defer closeBody(t, r2)

	// Any key lookup on srv1 must proxy to srv2 and return srv2's node.
	resp, err := http.Get(fmt.Sprintf("%s/keys/anykey", srv1.URL))
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	defer closeBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var node NodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if node.ID != "srv2" {
		t.Errorf("expected srv2, got %s", node.ID)
	}
}