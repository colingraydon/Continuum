package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colingraydon/continuum/internal/ring"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(NewServer(ring.NewRing(50)))
	t.Cleanup(func() { srv.Close() })
	return srv
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
	// Arrange
	srv := newTestServer(t)

	// Act
	resp := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), `{"id": "node1", "address": "10.0.0.1"}`)
	defer closeBody(t, resp)

	// Assert
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}

	// Act
	resp2, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	defer closeBody(t, resp2)

	// Assert
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
	// Arrange
	srv := newTestServer(t)
	resp0 := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), `{"id": "node1", "address": "10.0.0.1"}`)
	defer closeBody(t, resp0)

	// Act
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/nodes/node1", srv.URL), nil)
	if err != nil {
		t.Fatalf("failed to create delete request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to remove node: %v", err)
	}
	defer closeBody(t, resp)

	// Assert
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}

	// Assert
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
	// Arrange
	srv := newTestServer(t)
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "10.0.0.%d"}`, i, i)
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer closeBody(t, r)
	}

	// Act
	resp, err := http.Get(fmt.Sprintf("%s/nodes", srv.URL))
	if err != nil {
		t.Fatalf("failed to get nodes: %v", err)
	}
	defer closeBody(t, resp)

	// Assert
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
	// Arrange
	srv := newTestServer(t)
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "10.0.0.%d"}`, i, i)
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer closeBody(t, r)
	}

	// Act
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

	// Assert
	if first != second {
		t.Errorf("expected consistent lookup, got %s then %s", first, second)
	}
}

func TestE2EGetStats(t *testing.T) {
	// Arrange
	srv := newTestServer(t)
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "10.0.0.%d"}`, i, i)
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer closeBody(t, r)
	}

	// Act
	resp, err := http.Get(fmt.Sprintf("%s/stats", srv.URL))
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	defer closeBody(t, resp)

	// Assert
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
	// Arrange
	srv := newTestServer(t)
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "10.0.0.%d"}`, i, i)
		r := postNode(t, fmt.Sprintf("%s/nodes", srv.URL), body)
		defer closeBody(t, r)
	}

	// Act
	body := `{"key": "somekey", "factor": 3}`
	resp, err := http.Post(fmt.Sprintf("%s/replicate", srv.URL), "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to get replication nodes: %v", err)
	}
	defer closeBody(t, resp)

	// Assert
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