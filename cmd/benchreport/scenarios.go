package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/antientropy"
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/sstable"
	"github.com/colingraydon/continuum/internal/store"
	"github.com/colingraydon/continuum/internal/wal"
)

// benchValue is the payload written by every write scenario - sized like a
// plausible small value rather than a one-byte toy.
const benchValue = "report-value-of-plausible-size"

// scenario is one measured workload: setup builds fixtures, op runs a single
// measured operation. samples is the default count, scaled by -scale.
type scenario struct {
	name        string
	description string
	samples     int
	run         func(samples int) ([]time.Duration, time.Duration, error)
}

// measure times op once per sample after warmup ops, returning per-op
// latencies and total wall time.
func measure(samples int, op func(int) error) ([]time.Duration, time.Duration, error) {
	warmup := samples / 20
	if warmup < 3 {
		warmup = 3
	}
	for i := 0; i < warmup; i++ {
		if err := op(i); err != nil {
			return nil, 0, err
		}
	}
	latencies := make([]time.Duration, samples)
	wallStart := time.Now()
	for i := 0; i < samples; i++ {
		start := time.Now()
		if err := op(warmup + i); err != nil {
			return nil, 0, err
		}
		latencies[i] = time.Since(start)
	}
	return latencies, time.Since(wallStart), nil
}

// --- cluster fixture (mirrors benchmarks/replication_bench_test.go) ---------

type cluster struct {
	servers []*httptest.Server
	client  *http.Client
}

func newCluster(rf, wq, rq int) *cluster {
	const nodes = 3
	mls := make([]*gossip.MemberList, nodes)
	servers := make([]*httptest.Server, nodes)
	for i := 0; i < nodes; i++ {
		r := ring.NewRing(50)
		id := fmt.Sprintf("report-node-%d", i)
		ml := gossip.NewMemberList(id, "", func(m *gossip.Member, status gossip.MemberStatus) {
			switch status {
			case gossip.MemberAlive:
				r.AddNode(m.ID, m.Address)
			case gossip.MemberDead:
				r.RemoveNode(m.ID)
			}
		})
		s := store.New()
		servers[i] = httptest.NewServer(api.NewServer(r, ml, s, api.HandlerConfig{
			SelfID: id, ReplicationFactor: rf, WriteQuorum: wq, ReadQuorum: rq,
			ReplicaTimeout: time.Second,
		}, nil))
		mls[i] = ml
	}
	for i := 0; i < nodes; i++ {
		addr := strings.TrimPrefix(servers[i].URL, "http://")
		for j := 0; j < nodes; j++ {
			mls[j].Add(fmt.Sprintf("report-node-%d", i), addr)
		}
	}
	return &cluster{servers: servers, client: &http.Client{Timeout: 5 * time.Second}}
}

func (c *cluster) close() {
	for _, s := range c.servers {
		s.Close()
	}
}

func (c *cluster) request(method, path string, body string, wantStatus int) error {
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, c.servers[0].URL+path, rdr)
	if err != nil {
		return err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain for keep-alive reuse
	if resp.StatusCode != wantStatus {
		return fmt.Errorf("%s %s: got %d, want %d", method, path, resp.StatusCode, wantStatus)
	}
	return nil
}

func (c *cluster) put(key, consistency string) error {
	path := "/keys/" + key
	if consistency != "" {
		path += "?consistency=" + consistency
	}
	return c.request(http.MethodPut, path, `{"value":"report-value"}`, http.StatusNoContent)
}

func (c *cluster) get(key, consistency string) error {
	return c.request(http.MethodGet, "/keys/"+key+"?consistency="+consistency, "", http.StatusOK)
}

// --- durable store fixture ---------------------------------------------------

func newDurableStore(dir string) (*store.Store, func(), error) {
	tablesDir := filepath.Join(dir, "tables")
	if err := os.MkdirAll(tablesDir, 0o755); err != nil {
		return nil, nil, err
	}
	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, nil, err
	}
	s := store.New()
	s.SetWAL(w)
	s.SetFlushPolicy(tablesDir, 0)
	cleanup := func() {
		_ = s.CloseTables()
		_ = w.Close()
		_ = os.RemoveAll(dir)
	}
	return s, cleanup, nil
}

func clock(n uint64) store.VectorClockVersion {
	return store.VectorClockVersion{Clocks: map[string]uint64{"report": n}}
}

// --- scenario list -----------------------------------------------------------

// scenarios returns the measured workloads. tmpDir hosts durable fixtures.
func scenarios(tmpDir string) []scenario {
	list := []scenario{
		{
			name:        "ring_lookup_batch1k",
			description: "Consistent-hash key lookup (ring.GetNode). Per-op latency derived from 1000-op batches: single ops are ~140ns, below timer granularity, so the distribution is over batch means.",
			samples:     2000,
			run:         runRingLookupBatches,
		},
		{
			name:        "store_get_sstable",
			description: "Point read served from an SSTable: bloom filter, index binary search, one block read (no block cache).",
			samples:     100_000,
			run:         runStoreGetSSTable(tmpDir, 0),
		},
		{
			name:        "store_get_sstable_cached",
			description: "Point read served from an SSTable through the shared block cache; steady state, so blocks arrive decompressed from memory with no disk read.",
			samples:     100_000,
			run:         runStoreGetSSTable(tmpDir, 16<<20),
		},
		{
			name:        "durable_put_sequential",
			description: "WAL-durable write from a single writer: each write pays a full fsync (no group-commit batching).",
			samples:     400,
			run:         runDurablePutSequential(tmpDir),
		},
		{
			name:        "durable_put_concurrent8",
			description: "WAL-durable writes from 8 concurrent writers sharing fsyncs via group commit. Per-op latency includes time waiting on the shared fsync; throughput reflects the batching win.",
			samples:     4000,
			run:         runDurablePutConcurrent(tmpDir),
		},
		{
			name:        "sync_state_serve",
			description: "Anti-entropy GET /sync served from the incrementally maintained Merkle tree over a 10k-key store.",
			samples:     10_000,
			run:         runSyncStateServe,
		},
	}
	list = append(list, httpScenarios()...)
	return list
}

// httpScenarios measures full coordinator round trips (in-process 3-node
// cluster, RF=3) per consistency level - the user-facing key lookup numbers.
func httpScenarios() []scenario {
	var out []scenario
	for _, level := range []string{"one", "quorum", "all"} {
		out = append(out,
			scenario{
				name:        "http_put_" + level,
				description: fmt.Sprintf("HTTP PUT /keys/{key}?consistency=%s through a 3-node in-process cluster (RF=3): coordinator write, replica fan-out, quorum wait.", level),
				samples:     20_000,
				run:         runClusterPut(level),
			},
			scenario{
				name:        "http_get_" + level,
				description: fmt.Sprintf("HTTP GET /keys/{key}?consistency=%s through a 3-node in-process cluster (RF=3): replica fan-out, R-quorum wait, sibling merge.", level),
				samples:     20_000,
				run:         runClusterGet(level),
			},
		)
	}
	out = append(out, scenario{
		name:        "http_scan_page100",
		description: "HTTP GET /keys?prefix= scan page (100 keys of a 5k-key prefix): scatter to all 3 nodes, dominance merge, horizon pagination.",
		samples:     500,
		run:         runClusterScan,
	})
	return out
}

func runRingLookupBatches(samples int) ([]time.Duration, time.Duration, error) {
	r := ring.NewRing(150)
	// Addresses are ring metadata only - lookups never dial them. The
	// reserved .invalid TLD makes that explicit.
	r.AddNode("node1", "node1.invalid:0")
	r.AddNode("node2", "node2.invalid:0")
	r.AddNode("node3", "node3.invalid:0")
	const batch = 1000
	keys := make([]string, batch)
	for i := range keys {
		keys[i] = fmt.Sprintf("lk-%06d", i)
	}
	lats, wall, err := measure(samples, func(int) error {
		for _, k := range keys {
			r.GetNode(k)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	// Each measured sample covers a whole batch: scale latencies to per-op
	// means, and scale wall the same way so throughput comes out in ops/sec
	// (summarize computes len(latencies)/wall).
	for i := range lats {
		lats[i] /= batch
	}
	return lats, wall / batch, nil
}

// runStoreGetSSTable measures table point reads; cacheBytes > 0 installs a
// shared block cache of that size (and names the fixture dir after it so the
// two scenarios don't collide).
func runStoreGetSSTable(tmpDir string, cacheBytes int64) func(int) ([]time.Duration, time.Duration, error) {
	return func(samples int) ([]time.Duration, time.Duration, error) {
		dir := filepath.Join(tmpDir, fmt.Sprintf("get-%d", cacheBytes))
		s, cleanup, err := newDurableStore(dir)
		if err != nil {
			return nil, 0, err
		}
		defer cleanup()
		s.SetBlockCache(sstable.NewCache(cacheBytes))
		const keys = 10_000
		for i := 0; i < keys; i++ {
			if err := s.Put(fmt.Sprintf("gk-%08d", i), benchValue, clock(uint64(i+1))); err != nil {
				return nil, 0, err
			}
		}
		if err := s.Flush(); err != nil {
			return nil, 0, err
		}
		return measure(samples, func(i int) error {
			_, ok, err := s.Get(fmt.Sprintf("gk-%08d", i%keys))
			if err != nil || !ok {
				return fmt.Errorf("get: ok=%v err=%v", ok, err)
			}
			return nil
		})
	}
}

func runDurablePutSequential(tmpDir string) func(int) ([]time.Duration, time.Duration, error) {
	return func(samples int) ([]time.Duration, time.Duration, error) {
		s, cleanup, err := newDurableStore(filepath.Join(tmpDir, "seq"))
		if err != nil {
			return nil, 0, err
		}
		defer cleanup()
		return measure(samples, func(i int) error {
			return s.Put(fmt.Sprintf("sq-%08d", i), benchValue, clock(uint64(i+1)))
		})
	}
}

// runDurablePutConcurrent hand-rolls the measurement loop: 8 workers record
// their own per-op latencies while sharing group-commit fsyncs.
func runDurablePutConcurrent(tmpDir string) func(int) ([]time.Duration, time.Duration, error) {
	return func(samples int) ([]time.Duration, time.Duration, error) {
		s, cleanup, err := newDurableStore(filepath.Join(tmpDir, "conc"))
		if err != nil {
			return nil, 0, err
		}
		defer cleanup()
		const workers = 8
		per := samples / workers
		if per < 1 {
			per = 1
		}
		latencies := make([][]time.Duration, workers)
		errs := make([]error, workers)
		var wg sync.WaitGroup
		wallStart := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				latencies[w], errs[w] = durableWriterLoop(s, w, per)
			}(w)
		}
		wg.Wait()
		wall := time.Since(wallStart)
		return mergeWorkerResults(latencies, errs, wall)
	}
}

// durableWriterLoop is one worker's timed write sequence.
func durableWriterLoop(s *store.Store, worker, per int) ([]time.Duration, error) {
	lats := make([]time.Duration, 0, per)
	for i := 0; i < per; i++ {
		key := fmt.Sprintf("cc-%d-%08d", worker, i)
		start := time.Now()
		if err := s.Put(key, benchValue, clock(uint64(i+1))); err != nil {
			return nil, err
		}
		lats = append(lats, time.Since(start))
	}
	return lats, nil
}

// mergeWorkerResults flattens per-worker latencies, surfacing the first error.
func mergeWorkerResults(latencies [][]time.Duration, errs []error, wall time.Duration) ([]time.Duration, time.Duration, error) {
	var all []time.Duration
	for w := range latencies {
		if errs[w] != nil {
			return nil, 0, errs[w]
		}
		all = append(all, latencies[w]...)
	}
	return all, wall, nil
}

func runSyncStateServe(samples int) ([]time.Duration, time.Duration, error) {
	r := ring.NewRing(8)
	r.AddNode("self", "self.invalid:0") // ring metadata only; never dialed
	s := store.New()
	for i := 0; i < 10_000; i++ {
		if err := s.Put(fmt.Sprintf("sy-%08d", i), benchValue, clock(uint64(i+1))); err != nil {
			return nil, 0, err
		}
	}
	m := antientropy.New(r, s, "self", 2, time.Second)
	ranges := r.GetPrimaryVnodeRanges("self")
	if len(ranges) == 0 {
		return nil, 0, fmt.Errorf("no vnode ranges")
	}
	vnode := ranges[0].End
	return measure(samples, func(int) error {
		if _, _, ok := m.SyncState(vnode); !ok {
			return fmt.Errorf("no tree for vnode")
		}
		return nil
	})
}

func runClusterPut(consistency string) func(int) ([]time.Duration, time.Duration, error) {
	return func(samples int) ([]time.Duration, time.Duration, error) {
		c := newCluster(3, 2, 2)
		defer c.close()
		return measure(samples, func(i int) error {
			return c.put(fmt.Sprintf("wk-%s-%08d", consistency, i), consistency)
		})
	}
}

func runClusterGet(consistency string) func(int) ([]time.Duration, time.Duration, error) {
	return func(samples int) ([]time.Duration, time.Duration, error) {
		c := newCluster(3, 3, 2) // W=3 so every replica holds every key
		defer c.close()
		const keys = 1000
		for i := 0; i < keys; i++ {
			if err := c.put(fmt.Sprintf("rk-%08d", i), "all"); err != nil {
				return nil, 0, err
			}
		}
		return measure(samples, func(i int) error {
			return c.get(fmt.Sprintf("rk-%08d", i%keys), consistency)
		})
	}
}

func runClusterScan(samples int) ([]time.Duration, time.Duration, error) {
	c := newCluster(3, 3, 1)
	defer c.close()
	for i := 0; i < 5000; i++ {
		if err := c.put(fmt.Sprintf("sk-%08d", i), "all"); err != nil {
			return nil, 0, err
		}
	}
	return measure(samples, func(int) error {
		return c.request(http.MethodGet, "/keys?prefix=sk-&limit=100", "", http.StatusOK)
	})
}
