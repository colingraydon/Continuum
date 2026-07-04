package benchmarks

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// benchCluster is an in-process 3-node cluster: real handlers, rings, member
// lists, and HTTP servers, without gossip/anti-entropy loops. Measures the
// coordinator round trip including replica fan-out over loopback HTTP.
type benchCluster struct {
	servers []*httptest.Server
}

func newBenchCluster(b *testing.B, rf, wq, rq int) *benchCluster {
	b.Helper()
	const nodes = 3
	rings := make([]*ring.Ring, nodes)
	mls := make([]*gossip.MemberList, nodes)
	servers := make([]*httptest.Server, nodes)
	for i := 0; i < nodes; i++ {
		r := ring.NewRing(50)
		id := fmt.Sprintf("bench-node-%d", i)
		ml := gossip.NewMemberList(id, "", func(m *gossip.Member, status gossip.MemberStatus) {
			switch status {
			case gossip.MemberAlive:
				r.AddNode(m.ID, m.Address)
			case gossip.MemberDead:
				r.RemoveNode(m.ID)
			}
		})
		s := store.New()
		srv := httptest.NewServer(api.NewServer(r, ml, s, api.HandlerConfig{
			SelfID: id, ReplicationFactor: rf, WriteQuorum: wq, ReadQuorum: rq,
			ReplicaTimeout: time.Second,
		}, nil))
		b.Cleanup(srv.Close)
		rings[i], mls[i], servers[i] = r, ml, srv
	}
	// Full mesh: every node knows every node's real listener address.
	for i := 0; i < nodes; i++ {
		selfAddr := strings.TrimPrefix(servers[i].URL, "http://")
		for j := 0; j < nodes; j++ {
			id := fmt.Sprintf("bench-node-%d", i)
			mls[j].Add(id, selfAddr)
		}
	}
	return &benchCluster{servers: servers}
}

func (c *benchCluster) put(b *testing.B, key, consistency string) {
	b.Helper()
	url := c.servers[0].URL + "/keys/" + key
	if consistency != "" {
		url += "?consistency=" + consistency
	}
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(`{"value":"bench-value"}`))
	if err != nil {
		b.Fatalf("build PUT: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		b.Fatalf("PUT: %v", err)
	}
	drainClose(resp)
	if resp.StatusCode != http.StatusNoContent {
		b.Fatalf("PUT %s: got %d", key, resp.StatusCode)
	}
}

// drainClose consumes the response body before closing so the underlying
// connection returns to the keep-alive pool. Without this, every request
// opens a fresh socket and the benchmark exhausts ephemeral ports.
func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (c *benchCluster) get(b *testing.B, key, consistency string) {
	b.Helper()
	url := c.servers[0].URL + "/keys/" + key + "?consistency=" + consistency
	resp, err := http.Get(url)
	if err != nil {
		b.Fatalf("GET: %v", err)
	}
	drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("GET %s: got %d", key, resp.StatusCode)
	}
}

// benchmarkClusterPut measures full coordinator write latency (local store
// write + WAL-less fan-out + quorum wait) at a given consistency level.
func benchmarkClusterPut(b *testing.B, consistency string) {
	c := newBenchCluster(b, 3, 2, 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.put(b, fmt.Sprintf("wkey-%08d", i), consistency)
	}
}

func BenchmarkClusterPutConsistencyOne(b *testing.B)    { benchmarkClusterPut(b, "one") }
func BenchmarkClusterPutConsistencyQuorum(b *testing.B) { benchmarkClusterPut(b, "quorum") }
func BenchmarkClusterPutConsistencyAll(b *testing.B)    { benchmarkClusterPut(b, "all") }

// benchmarkClusterGet measures full coordinator read latency (fan-out, R
// waits, sibling merge) at a given consistency level.
func benchmarkClusterGet(b *testing.B, consistency string) {
	c := newBenchCluster(b, 3, 3, 2) // W=3 so every replica holds every key
	const keys = 1000
	for i := 0; i < keys; i++ {
		c.put(b, fmt.Sprintf("rkey-%08d", i), "all")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.get(b, fmt.Sprintf("rkey-%08d", i%keys), consistency)
	}
}

func BenchmarkClusterGetConsistencyOne(b *testing.B)    { benchmarkClusterGet(b, "one") }
func BenchmarkClusterGetConsistencyQuorum(b *testing.B) { benchmarkClusterGet(b, "quorum") }
func BenchmarkClusterGetConsistencyAll(b *testing.B)    { benchmarkClusterGet(b, "all") }

// BenchmarkClusterScanPage: one coordinator scan page (scatter to all three
// nodes, dominance merge, horizon pagination) over a 5k-key prefix space.
func BenchmarkClusterScanPage(b *testing.B) {
	c := newBenchCluster(b, 3, 3, 1)
	for i := 0; i < 5000; i++ {
		c.put(b, fmt.Sprintf("skey-%08d", i), "all")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Get(c.servers[0].URL + "/keys?prefix=skey-&limit=100")
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
		drainClose(resp)
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("scan: got %d", resp.StatusCode)
		}
	}
}
