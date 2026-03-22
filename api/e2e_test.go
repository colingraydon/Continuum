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

func TestE2EAddAndGetNode(t *testing.T) {
	// Arrange
	srv := newTestServer(t)

	// Act
	body := `{"id": "node1", "address": "10.0.0.1"}`
	resp, err := http.Post(fmt.Sprintf("%s/nodes", srv.URL), "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("failed to add node: %v", err)
	}
	defer resp.Body.Close()

	// Assert
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}

	// Act
	resp2, err := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	defer resp2.Body.Close()

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
	body := `{"id": "node1", "address": "10.0.0.1"}`
	http.Post(fmt.Sprintf("%s/nodes", srv.URL), "application/json", bytes.NewBufferString(body))

	// Act
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/nodes/node1", srv.URL), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to remove node: %v", err)
	}
	defer resp.Body.Close()

	// Assert
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}

	// Assert
	resp2, _ := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 after removal, got %d", resp2.StatusCode)
	}
}

func TestE2EGetNodes(t *testing.T) {
	// Arrange
	srv := newTestServer(t)
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"id": "node%d", "address": "10.0.0.%d"}`, i, i)
		http.Post(fmt.Sprintf("%s/nodes", srv.URL), "application/json", bytes.NewBufferString(body))
	}

	// Act
	resp, err := http.Get(fmt.Sprintf("%s/nodes", srv.URL))
	if err != nil {
		t.Fatalf("failed to get nodes: %v", err)
	}
	defer resp.Body.Close()

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
		http.Post(fmt.Sprintf("%s/nodes", srv.URL), "application/json", bytes.NewBufferString(body))
	}

	// Act
	getNode := func() string {
		resp, _ := http.Get(fmt.Sprintf("%s/keys/mykey", srv.URL))
		defer resp.Body.Close()
		var node NodeResponse
		json.NewDecoder(resp.Body).Decode(&node)
		return node.ID
	}

	first := getNode()
	second := getNode()

	// Assert
	if first != second {
		t.Errorf("expected consistent lookup, got %s then %s", first, second)
	}
}