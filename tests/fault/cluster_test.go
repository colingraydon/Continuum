//go:build fault

package fault

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var binaryPath string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "continuum-fault-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "temp dir: %v\n", err)
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

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)
}

// clusterConfig parameterizes a fault-injection cluster. Zero values take the
// defaults noted per field.
type clusterConfig struct {
	nodes             int // default 3
	replicationFactor int // default 3
	writeQuorum       int // default 2
	readQuorum        int // default 2
	replicaTimeoutMS  int // default 750
	syncIntervalMS    int // default 2000 (fast anti-entropy so tests converge quickly)
	hintDeliveryMS    int // default 1000 (fast hint-delivery sweep so tests observe it)
	memtableMaxBytes  int // default 8192 (tiny, to force flush/compaction churn under load)
}

func (c clusterConfig) withDefaults() clusterConfig {
	if c.nodes == 0 {
		c.nodes = 3
	}
	if c.replicationFactor == 0 {
		c.replicationFactor = 3
	}
	if c.writeQuorum == 0 {
		c.writeQuorum = 2
	}
	if c.readQuorum == 0 {
		c.readQuorum = 2
	}
	if c.replicaTimeoutMS == 0 {
		c.replicaTimeoutMS = 750
	}
	if c.syncIntervalMS == 0 {
		c.syncIntervalMS = 2000
	}
	if c.hintDeliveryMS == 0 {
		c.hintDeliveryMS = 1000
	}
	if c.memtableMaxBytes == 0 {
		c.memtableMaxBytes = 8192
	}
	return c
}

// node is one Continuum process plus the proxies its peers reach it through
// and the persistent state that survives restarts (identity, ports, DATA_DIR).
type node struct {
	id         string
	dataDir    string
	bindPort   string // real HTTP listener; harness talks to this directly
	gossipPort string // real UDP listener
	httpProxy  *tcpProxy
	udpProxy   *udpProxy

	mu      sync.Mutex
	cmd     *exec.Cmd
	running bool
	paused  bool
}

func (n *node) baseURL() string        { return "http://127.0.0.1:" + n.bindPort }
func (n *node) advertisedAddr() string { return n.httpProxy.Addr() }

func (n *node) isRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running && !n.paused
}

type cluster struct {
	t      *testing.T
	cfg    clusterConfig
	nodes  []*node
	client *http.Client
}

// newCluster starts cfg.nodes Continuum processes behind fault proxies,
// fully meshes their membership (with gossip addresses, so gossip keeps
// membership fresh from then on), and waits until every node sees the whole
// ring.
func newCluster(t *testing.T, cfg clusterConfig) *cluster {
	t.Helper()
	cfg = cfg.withDefaults()
	c := &cluster{t: t, cfg: cfg, client: &http.Client{Timeout: 3 * time.Second}}

	for i := 0; i < cfg.nodes; i++ {
		bindPort := freePort(t)
		gossipPort := freePort(t)
		n := &node{
			id:         fmt.Sprintf("node%d", i+1),
			dataDir:    t.TempDir(),
			bindPort:   bindPort,
			gossipPort: gossipPort,
			httpProxy:  newTCPProxy(t, "127.0.0.1:"+bindPort),
			udpProxy:   newUDPProxy(t, "127.0.0.1:"+gossipPort),
		}
		c.nodes = append(c.nodes, n)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			c.killQuiet(n)
		}
	})
	for _, n := range c.nodes {
		c.start(n)
	}
	for _, n := range c.nodes {
		c.waitHealthy(n)
	}
	c.mesh()
	c.waitFullRing(30 * time.Second)
	return c
}

func (c *cluster) start(n *node) {
	c.t.Helper()
	env := append(os.Environ(),
		"SELF_ID="+n.id,
		"SELF_ADDRESS="+n.advertisedAddr(),
		"HTTP_BIND_PORT="+n.bindPort,
		"GOSSIP_PORT="+n.gossipPort,
		"GOSSIP_ADVERTISE_ADDR="+n.udpProxy.Addr(),
		"DATA_DIR="+n.dataDir,
		// Few vnodes: anti-entropy round-robins one primary vnode per round,
		// so a full keyspace pass takes vnodes x interval. 8 vnodes at a 2s
		// interval is a 16s pass; the default 150 would take 5 minutes even
		// at test cadence.
		"REPLICAS=8",
		fmt.Sprintf("REPLICATION_FACTOR=%d", c.cfg.replicationFactor),
		fmt.Sprintf("WRITE_QUORUM=%d", c.cfg.writeQuorum),
		fmt.Sprintf("READ_QUORUM=%d", c.cfg.readQuorum),
		fmt.Sprintf("REPLICA_TIMEOUT_MS=%d", c.cfg.replicaTimeoutMS),
		fmt.Sprintf("SYNC_INTERVAL_MS=%d", c.cfg.syncIntervalMS),
		fmt.Sprintf("HINT_DELIVERY_INTERVAL_MS=%d", c.cfg.hintDeliveryMS),
		fmt.Sprintf("MEMTABLE_MAX_BYTES=%d", c.cfg.memtableMaxBytes),
	)
	cmd := exec.Command(binaryPath)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", n.id, err)
	}
	n.mu.Lock()
	n.cmd = cmd
	n.running = true
	n.paused = false
	n.mu.Unlock()
}

func (c *cluster) waitHealthy(n *node) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.client.Get(n.baseURL() + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("%s never became healthy", n.id)
}

// mesh registers every running node with every other, including gossip
// addresses so UDP gossip flows through the fault proxies afterwards.
func (c *cluster) mesh() {
	for _, a := range c.nodes {
		for _, b := range c.nodes {
			if a != b && a.isRunning() && b.isRunning() {
				c.register(a, b)
			}
		}
	}
}

// register tells a about b.
func (c *cluster) register(a, b *node) {
	body := fmt.Sprintf(`{"id":%q,"address":%q,"gossip_address":%q}`,
		b.id, b.advertisedAddr(), b.udpProxy.Addr())
	resp, err := c.client.Post(a.baseURL()+"/nodes", "application/json", strings.NewReader(body))
	if err != nil {
		c.t.Logf("register %s -> %s: %v", b.id, a.id, err)
		return
	}
	resp.Body.Close()
}

// ringIDs returns the node IDs currently in n's ring.
func (c *cluster) ringIDs(n *node) map[string]bool {
	resp, err := c.client.Get(n.baseURL() + "/nodes")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var nodes []nodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil
	}
	ids := make(map[string]bool, len(nodes))
	for _, nr := range nodes {
		ids[nr.ID] = true
	}
	return ids
}

// waitFullRing waits until every running node's ring contains every running
// node, re-registering missing members along the way. Re-registration matters
// after a restart: a rejoining node's heartbeat restarts at zero, so peers
// that remember its old (higher) heartbeat ignore its gossip until it catches
// up; a fresh POST /nodes resets the stored entry.
func (c *cluster) waitFullRing(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if c.fullRingOnce() {
			return
		}
		if time.Now().After(deadline) {
			for _, n := range c.nodes {
				if n.isRunning() {
					c.t.Logf("%s ring: %v", n.id, c.ringIDs(n))
				}
			}
			c.t.Fatalf("cluster did not converge to a full ring within %v", timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *cluster) fullRingOnce() bool {
	ok := true
	for _, a := range c.nodes {
		if !a.isRunning() {
			continue
		}
		ids := c.ringIDs(a)
		for _, b := range c.nodes {
			if !b.isRunning() {
				continue
			}
			if !ids[b.id] {
				ok = false
				if a != b {
					c.register(a, b)
				}
			}
		}
	}
	return ok
}

// --- fault operations -------------------------------------------------------

// kill SIGKILLs the node: no graceful shutdown, no push-on-leave, no meta.json.
func (c *cluster) kill(n *node) {
	c.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return
	}
	_ = n.cmd.Process.Kill()
	_ = n.cmd.Wait()
	n.running = false
	n.paused = false
}

func (c *cluster) killQuiet(n *node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return
	}
	if n.paused {
		_ = n.cmd.Process.Signal(syscall.SIGCONT)
	}
	_ = n.cmd.Process.Kill()
	_ = n.cmd.Wait()
	n.running = false
	n.paused = false
}

// shutdown SIGTERMs the node and waits for its graceful exit (push-on-leave,
// hint flush, WAL finalize, meta.json).
func (c *cluster) shutdown(n *node) {
	c.t.Helper()
	n.mu.Lock()
	cmd := n.cmd
	n.mu.Unlock()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		c.t.Fatalf("SIGTERM %s: %v", n.id, err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		c.t.Fatalf("%s did not exit within 45s of SIGTERM", n.id)
	}
	n.mu.Lock()
	n.running = false
	n.mu.Unlock()
}

// pause SIGSTOPs the process: it stays alive but is completely unresponsive,
// simulating a hung node. Connections reach the kernel backlog and time out
// instead of being refused.
func (c *cluster) pause(n *node) {
	c.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		c.t.Fatalf("SIGSTOP %s: %v", n.id, err)
	}
	n.paused = true
}

func (c *cluster) resume(n *node) {
	c.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		c.t.Fatalf("SIGCONT %s: %v", n.id, err)
	}
	n.paused = false
}

// restart brings a stopped node back with the same identity, ports, proxies,
// and DATA_DIR, then re-meshes membership until every node sees the full ring.
func (c *cluster) restart(n *node) {
	c.t.Helper()
	c.start(n)
	c.waitHealthy(n)
	c.waitFullRing(60 * time.Second)
}

// isolate blackholes all inbound traffic to n (HTTP and gossip). n's own
// outbound traffic still flows: this is an asymmetric partition.
func (c *cluster) isolate(n *node) {
	n.httpProxy.Blackhole()
	n.udpProxy.Blackhole()
}

func (c *cluster) heal(n *node) {
	n.httpProxy.Heal()
	n.udpProxy.Heal()
}

// --- client helpers ---------------------------------------------------------

// nodeResponse mirrors the subset of api.NodeResponse the harness asserts on.
type nodeResponse struct {
	ID       string            `json:"id"`
	Status   string            `json:"status"`
	Value    string            `json:"value"`
	Clocks   map[string]uint64 `json:"clocks"`
	Deleted  bool              `json:"deleted"`
	Siblings []siblingResponse `json:"siblings"`
}

type siblingResponse struct {
	Value   string            `json:"value"`
	Clocks  map[string]uint64 `json:"clocks"`
	Deleted bool              `json:"deleted"`
}

func (c *cluster) alive() []*node {
	var out []*node
	for _, n := range c.nodes {
		if n.isRunning() {
			out = append(out, n)
		}
	}
	return out
}

func (c *cluster) get(n *node, key string) (nodeResponse, int, error) {
	resp, err := c.client.Get(n.baseURL() + "/keys/" + key)
	if err != nil {
		return nodeResponse{}, 0, err
	}
	defer resp.Body.Close()
	var nr nodeResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
			return nodeResponse{}, resp.StatusCode, err
		}
	}
	return nr, resp.StatusCode, nil
}

func (c *cluster) put(n *node, key, value string, clocks map[string]uint64) (int, error) {
	payload := struct {
		Value  string            `json:"value"`
		Clocks map[string]uint64 `json:"clocks,omitempty"`
	}{Value: value, Clocks: clocks}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest(http.MethodPut, n.baseURL()+"/keys/"+key, strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// getSerial reads key through n as coordinator at ?consistency=serial: a
// linearizable read served by the paxos prepare phase. The second return
// value is the merged clock from the X-Session-Clock response header — the
// context a client sends back to chain a CAS write off this read.
func (c *cluster) getSerial(n *node, key string) (nodeResponse, map[string]uint64, int, error) {
	resp, err := c.client.Get(n.baseURL() + "/keys/" + key + "?consistency=serial")
	if err != nil {
		return nodeResponse{}, nil, 0, err
	}
	defer resp.Body.Close()
	var sessionClock map[string]uint64
	if raw := resp.Header.Get("X-Session-Clock"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &sessionClock); err != nil {
			return nodeResponse{}, nil, resp.StatusCode, err
		}
	}
	var nr nodeResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
			return nodeResponse{}, nil, resp.StatusCode, err
		}
	}
	return nr, sessionClock, resp.StatusCode, nil
}

// casPut issues a conditional write (?cas=true) through n as coordinator.
// clocks is the precondition context; nil means "expect no current value".
func (c *cluster) casPut(n *node, key, value string, clocks map[string]uint64) (int, error) {
	payload := struct {
		Value  string            `json:"value"`
		Clocks map[string]uint64 `json:"clocks,omitempty"`
	}{Value: value, Clocks: clocks}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest(http.MethodPut, n.baseURL()+"/keys/"+key+"?cas=true", strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// putConsistency is put with a per-request ?consistency= level.
func (c *cluster) putConsistency(n *node, key, value, consistency string) (int, error) {
	body, err := json.Marshal(struct {
		Value string `json:"value"`
	}{Value: value})
	if err != nil {
		return 0, err
	}
	url := n.baseURL() + "/keys/" + key + "?consistency=" + consistency
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// memberStatus returns id's gossip status ("alive", "suspect", "dead") as seen
// by n, or "" when n does not list it.
func (c *cluster) memberStatus(n *node, id string) string {
	resp, err := c.client.Get(n.baseURL() + "/nodes")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var nodes []nodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return ""
	}
	for _, nr := range nodes {
		if nr.ID == id {
			return nr.Status
		}
	}
	return ""
}

// waitMemberStatus polls until observer sees id with the given gossip status,
// failing the test if it never happens within timeout.
func (c *cluster) waitMemberStatus(t *testing.T, observer *node, id, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for c.memberStatus(observer, id) != status {
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached status %q on %s", id, status, observer.id)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// mustPut writes through n as coordinator and fails the test on anything but 204.
func mustPut(t *testing.T, c *cluster, n *node, key, value string) {
	t.Helper()
	if code, err := c.put(n, key, value, nil); err != nil || code != http.StatusNoContent {
		t.Fatalf("put %s=%s via %s: code=%d err=%v", key, value, n.id, code, err)
	}
}

// mustDelete deletes through n as coordinator and fails the test on anything but 204.
func mustDelete(t *testing.T, c *cluster, n *node, key string) {
	t.Helper()
	if code, err := c.deleteKey(n, key); err != nil || code != http.StatusNoContent {
		t.Fatalf("delete %s via %s: code=%d err=%v", key, n.id, code, err)
	}
}

// putReplica writes directly to n's local store via X-Proxied-From, bypassing
// coordinator fan-out. Used to seed state onto a specific replica.
func (c *cluster) putReplica(n *node, key, value string, clocks map[string]uint64) {
	c.t.Helper()
	payload := struct {
		Value  string            `json:"value"`
		Clocks map[string]uint64 `json:"clocks"`
	}{Value: value, Clocks: clocks}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPut, n.baseURL()+"/keys/"+key, strings.NewReader(string(body)))
	if err != nil {
		c.t.Fatalf("build putReplica: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Proxied-From", "fault-harness")
	resp, err := c.client.Do(req)
	if err != nil {
		c.t.Fatalf("putReplica %s on %s: %v", key, n.id, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		c.t.Fatalf("putReplica %s on %s: status %d", key, n.id, resp.StatusCode)
	}
}

func (c *cluster) deleteKey(n *node, key string) (int, error) {
	req, err := http.NewRequest(http.MethodDelete, n.baseURL()+"/keys/"+key, strings.NewReader("{}"))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// directGet reads n's local entry for key without coordinator fan-out.
func (c *cluster) directGet(n *node, key string) (nodeResponse, int, error) {
	req, err := http.NewRequest(http.MethodGet, n.baseURL()+"/keys/"+key, nil)
	if err != nil {
		return nodeResponse{}, 0, err
	}
	req.Header.Set("X-Proxied-From", "fault-harness")
	resp, err := c.client.Do(req)
	if err != nil {
		return nodeResponse{}, 0, err
	}
	defer resp.Body.Close()
	var nr nodeResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
			return nodeResponse{}, resp.StatusCode, err
		}
	}
	return nr, resp.StatusCode, nil
}

// replicaSet asks an alive node which nodes own key.
func (c *cluster) replicaSet(key string) []*node {
	byID := make(map[string]*node, len(c.nodes))
	for _, n := range c.nodes {
		byID[n.id] = n
	}
	for _, n := range c.alive() {
		body := fmt.Sprintf(`{"key":%q,"factor":%d}`, key, c.cfg.replicationFactor)
		resp, err := c.client.Post(n.baseURL()+"/replicate", "application/json", strings.NewReader(body))
		if err != nil {
			continue
		}
		var rr struct {
			Nodes []nodeResponse `json:"nodes"`
		}
		err = json.NewDecoder(resp.Body).Decode(&rr)
		resp.Body.Close()
		if err != nil {
			continue
		}
		var out []*node
		for _, nr := range rr.Nodes {
			if rn, ok := byID[nr.ID]; ok {
				out = append(out, rn)
			}
		}
		return out
	}
	return nil
}
