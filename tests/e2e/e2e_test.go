//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var binaryPath string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "continuum-e2e-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	binaryPath = filepath.Join(tmp, "continuum")
	out, err := exec.Command("go", "build", "-o", binaryPath, "github.com/colingraydon/continuum/cmd/continuum").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s\n", err, out)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// nodeResponse mirrors the subset of api.NodeResponse used in assertions.
type nodeResponse struct {
	ID       string            `json:"id"`
	Value    string            `json:"value"`
	Siblings []siblingResponse `json:"siblings"`
}

type siblingResponse struct {
	Value  string            `json:"value"`
	Clocks map[string]uint64 `json:"clocks"`
}

type testNode struct {
	id      string
	addr    string
	baseURL string
	cmd     *exec.Cmd
}

// freePort returns a free TCP port on localhost.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	defer l.Close()
	return fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)
}

// startNode starts a Continuum process with the provided extra env vars
// appended to the base config. Callers can override REPLICATION_FACTOR,
// WRITE_QUORUM, READ_QUORUM, etc. The node is stopped on test cleanup.
func startNode(t *testing.T, extraEnv ...string) *testNode {
	t.Helper()
	httpPort := freePort(t)
	gossipPort := freePort(t)
	id := "node-" + httpPort
	addr := "127.0.0.1:" + httpPort

	env := append(os.Environ(),
		"SELF_ID="+id,
		"SELF_ADDRESS="+addr,
		"GOSSIP_PORT="+gossipPort,
		"REPLICATION_FACTOR=1",
		"WRITE_QUORUM=1",
		"READ_QUORUM=1",
		"REPLICA_TIMEOUT_MS=2000",
	)
	env = append(env, extraEnv...)

	cmd := exec.Command(binaryPath)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node %s: %v", id, err)
	}

	n := &testNode{id: id, addr: addr, baseURL: "http://" + addr, cmd: cmd}

	t.Cleanup(func() {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
	})

	n.waitHealthy(t)
	return n
}

func (n *testNode) waitHealthy(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(n.baseURL + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %s never became healthy", n.id)
}

func (n *testNode) registerPeer(t *testing.T, peer *testNode) {
	t.Helper()
	body := fmt.Sprintf(`{"id": %q, "address": %q}`, peer.id, peer.addr)
	resp, err := http.Post(n.baseURL+"/nodes", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("registerPeer %s -> %s: %v", n.id, peer.id, err)
	}
	resp.Body.Close()
}

func (n *testNode) put(t *testing.T, key, value string) int {
	t.Helper()
	body := fmt.Sprintf(`{"value": %q}`, value)
	req, err := http.NewRequest(http.MethodPut, n.baseURL+"/keys/"+key, strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to build PUT: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /keys/%s on %s: %v", key, n.id, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func (n *testNode) get(t *testing.T, key string) (nodeResponse, int) {
	t.Helper()
	resp, err := http.Get(n.baseURL + "/keys/" + key)
	if err != nil {
		t.Fatalf("GET /keys/%s on %s: %v", key, n.id, err)
	}
	defer resp.Body.Close()
	var nr nodeResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
			t.Fatalf("decode GET response: %v", err)
		}
	}
	return nr, resp.StatusCode
}

// mesh registers every node with every other node in the slice.
func mesh(t *testing.T, nodes ...*testNode) {
	t.Helper()
	for _, n := range nodes {
		for _, peer := range nodes {
			if n != peer {
				n.registerPeer(t, peer)
			}
		}
	}
}

// TestProcessSingleNodeHealthy verifies the server starts and reports healthy.
func TestProcessSingleNodeHealthy(t *testing.T) {
	n := startNode(t)

	resp, err := http.Get(n.baseURL + "/health")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

// TestProcessSingleNodeWriteRead verifies that a value written to a node
// can be read back from the same node.
func TestProcessSingleNodeWriteRead(t *testing.T) {
	n := startNode(t)
	n.registerPeer(t, n) // register self so the ring has a node to route to

	if code := n.put(t, "greeting", "hello"); code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", code)
	}

	nr, code := n.get(t, "greeting")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if nr.Value != "hello" {
		t.Errorf("expected 'hello', got %q", nr.Value)
	}
}

// TestProcessThreeNodeReplication verifies that a quorum write reaches all
// replicas so any node can serve the value immediately after the write.
func TestProcessThreeNodeReplication(t *testing.T) {
	rf := "REPLICATION_FACTOR=3"
	wq := "WRITE_QUORUM=3" // all nodes ack before write returns
	rq := "READ_QUORUM=1"

	n1 := startNode(t, rf, wq, rq)
	n2 := startNode(t, rf, wq, rq)
	n3 := startNode(t, rf, wq, rq)
	mesh(t, n1, n2, n3)

	if code := n1.put(t, "fruit", "mango"); code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", code)
	}

	// WQ=3 guarantees all nodes ack'd before the write returned, so no sleep needed.
	for _, n := range []*testNode{n1, n2, n3} {
		nr, code := n.get(t, "fruit")
		if code != http.StatusOK {
			t.Errorf("node %s: expected 200, got %d", n.id, code)
			continue
		}
		if nr.Value != "mango" {
			t.Errorf("node %s: expected 'mango', got %q", n.id, nr.Value)
		}
	}
}

// TestProcessQuorumWriteFailure verifies that a write returns 503 when a
// required replica is unreachable and quorum cannot be met.
func TestProcessQuorumWriteFailure(t *testing.T) {
	n := startNode(t, "REPLICATION_FACTOR=2", "WRITE_QUORUM=2")
	n.registerPeer(t, n)
	// Register an unreachable second node to make the ring think RF=2 is possible.
	body := `{"id": "ghost", "address": "127.0.0.1:19999"}`
	resp, err := http.Post(n.baseURL+"/nodes", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to register ghost node: %v", err)
	}
	resp.Body.Close()

	if code := n.put(t, "key", "value"); code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when quorum unreachable, got %d", code)
	}
}

// TestProcessGracefulShutdown verifies that the server exits cleanly on
// SIGTERM and stops accepting connections after shutdown completes.
func TestProcessGracefulShutdown(t *testing.T) {
	httpPort := freePort(t)
	gossipPort := freePort(t)
	id := "shutdown-node"
	addr := "127.0.0.1:" + httpPort

	cmd := exec.Command(binaryPath)
	cmd.Env = append(os.Environ(),
		"SELF_ID="+id,
		"SELF_ADDRESS="+addr,
		"GOSSIP_PORT="+gossipPort,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	n := &testNode{id: id, addr: addr, baseURL: "http://" + addr}
	n.waitHealthy(t)

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send SIGTERM: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected clean exit after SIGTERM, got: %v", err)
		}
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		t.Fatal("process did not exit within 10 seconds after SIGTERM")
	}

	// HTTP should no longer be available.
	resp, err := http.Get(n.baseURL + "/health")
	if err == nil {
		resp.Body.Close()
		t.Error("expected connection refused after shutdown, but got a response")
	}
}
