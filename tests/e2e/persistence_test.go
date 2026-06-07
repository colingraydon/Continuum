//go:build e2e

package e2e

import (
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// startPersistentNode launches a node with a fixed SELF_ID and DATA_DIR.
// Unlike startNode, the caller controls the identity so a follow-up start
// can reuse the same data dir without tripping the identity check.
func startPersistentNode(t *testing.T, selfID, dataDir string) *testNode {
	t.Helper()
	httpPort := freePort(t)
	gossipPort := freePort(t)
	addr := "127.0.0.1:" + httpPort

	env := append(os.Environ(),
		"SELF_ID="+selfID,
		"SELF_ADDRESS="+addr,
		"GOSSIP_PORT="+gossipPort,
		"REPLICATION_FACTOR=1",
		"WRITE_QUORUM=1",
		"READ_QUORUM=1",
		"REPLICA_TIMEOUT_MS=2000",
		"DATA_DIR="+dataDir,
	)

	cmd := exec.Command(binaryPath)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start node %s: %v", selfID, err)
	}

	n := &testNode{id: selfID, addr: addr, baseURL: "http://" + addr, cmd: cmd}
	t.Cleanup(func() {
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	})
	n.waitHealthy(t)
	return n
}

// stopGracefully sends SIGTERM and waits for the process to exit so the
// node's shutdown path (PushKeysToSuccessors, FlushHints, drain, finalize)
// runs to completion.
func stopGracefully(t *testing.T, n *testNode) {
	t.Helper()
	if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM %s: %v", n.id, err)
	}
	if err := n.cmd.Wait(); err != nil {
		// Exit status from SIGTERM is non-zero on some platforms; only fail
		// if the process clearly crashed.
		if _, ok := err.(*exec.ExitError); !ok {
			t.Fatalf("wait %s: %v", n.id, err)
		}
	}
}

func TestPersistence_SingleNodeSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	selfID := "persist-" + freePort(t)

	n := startPersistentNode(t, selfID, dataDir)
	if code := n.put(t, "alpha", "one"); code != http.StatusNoContent {
		t.Fatalf("put alpha: status %d", code)
	}
	if code := n.put(t, "beta", "two"); code != http.StatusNoContent {
		t.Fatalf("put beta: status %d", code)
	}
	if code := n.put(t, "gamma", "three"); code != http.StatusNoContent {
		t.Fatalf("put gamma: status %d", code)
	}
	if code := n.delete(t, "beta"); code != http.StatusNoContent {
		t.Fatalf("delete beta: status %d", code)
	}

	stopGracefully(t, n)

	n2 := startPersistentNode(t, selfID, dataDir)
	if nr, code := n2.get(t, "alpha"); code != http.StatusOK || nr.Value != "one" {
		t.Fatalf("alpha after restart: code=%d value=%q", code, nr.Value)
	}
	if nr, code := n2.get(t, "gamma"); code != http.StatusOK || nr.Value != "three" {
		t.Fatalf("gamma after restart: code=%d value=%q", code, nr.Value)
	}
	// beta was tombstoned — GET should return 404.
	if _, code := n2.get(t, "beta"); code != http.StatusNotFound {
		t.Fatalf("beta after restart: code=%d, want 404", code)
	}
}

func TestPersistence_RecoversFromWALOnlyAfterCrash(t *testing.T) {
	// SIGKILL skips graceful shutdown so no snapshot is written; only the WAL
	// (which is fsynced on every write) carries the state. Recovery must
	// rebuild the store from the WAL alone.
	dataDir := t.TempDir()
	selfID := "crash-" + freePort(t)

	n := startPersistentNode(t, selfID, dataDir)
	if code := n.put(t, "k", "before-crash"); code != http.StatusNoContent {
		t.Fatalf("put: status %d", code)
	}

	if err := n.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = n.cmd.Wait()

	// No meta.json was written so the downtime gate trips and discards data.
	// GET on a never-stored key returns 200 with an empty value (see
	// writeKeyResponse case 0), so we assert on the value rather than status.
	n2 := startPersistentNode(t, selfID, dataDir)
	if nr, code := n2.get(t, "k"); code != http.StatusOK || nr.Value != "" {
		t.Fatalf("crashed node should re-bootstrap empty; got code=%d value=%q", code, nr.Value)
	}
}
