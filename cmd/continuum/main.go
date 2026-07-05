package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/antientropy"
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/hintstore"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

type config struct {
	replicas             int
	replicationFactor    int
	writeQuorum          int
	readQuorum           int
	selfID               string
	selfAddress          string
	httpBindPort         string
	gossipPort           string
	gossipAdvertise      string
	seedNodes            []string
	replicaTimeout       time.Duration
	syncInterval         time.Duration
	hintDeliveryInterval time.Duration
	selfWeight           float64
	dataDir              string
	memtableMaxBytes     int64
	blockCacheBytes      int64
}

func getEnvInt(key string, dflt int) int {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return dflt
}

func getEnvPositiveInt(key string, dflt int) int {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			return parsed
		}
	}
	return dflt
}

func getEnvFloat64(key string, dflt float64) float64 {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed > 0 {
			return parsed
		}
	}
	return dflt
}

func getEnvDurationMs(key string, dflt time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			return time.Duration(parsed) * time.Millisecond
		}
	}
	return dflt
}

func getEnvString(key, dflt string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return dflt
}

func loadConfig() config {
	replicationFactor := getEnvInt("REPLICATION_FACTOR", 3)
	defaultQuorum := replicationFactor/2 + 1
	writeQuorum := getEnvPositiveInt("WRITE_QUORUM", defaultQuorum)
	readQuorum := getEnvPositiveInt("READ_QUORUM", defaultQuorum)

	if writeQuorum > replicationFactor {
		log.Fatalf("WRITE_QUORUM (%d) exceeds REPLICATION_FACTOR (%d): writes will always fail", writeQuorum, replicationFactor)
	}
	if readQuorum > replicationFactor {
		log.Fatalf("READ_QUORUM (%d) exceeds REPLICATION_FACTOR (%d): reads will always fail", readQuorum, replicationFactor)
	}

	selfAddress := getEnvString("SELF_ADDRESS", "localhost:8080")
	var seedNodes []string
	if val := os.Getenv("SEED_NODES"); val != "" {
		seedNodes = strings.Split(val, ",")
	}

	gossipPort := getEnvString("GOSSIP_PORT", "8081")

	return config{
		replicas:             getEnvInt("REPLICAS", 150),
		replicationFactor:    replicationFactor,
		writeQuorum:          writeQuorum,
		readQuorum:           readQuorum,
		selfID:               getEnvString("SELF_ID", selfAddress),
		selfAddress:          selfAddress,
		httpBindPort:         getEnvString("HTTP_BIND_PORT", ""),
		gossipPort:           gossipPort,
		gossipAdvertise:      getEnvString("GOSSIP_ADVERTISE_ADDR", net.JoinHostPort(advertiseHost(selfAddress), gossipPort)),
		seedNodes:            seedNodes,
		replicaTimeout:       getEnvDurationMs("REPLICA_TIMEOUT_MS", 500*time.Millisecond),
		syncInterval:         getEnvDurationMs("SYNC_INTERVAL_MS", 30*time.Second),
		hintDeliveryInterval: getEnvDurationMs("HINT_DELIVERY_INTERVAL_MS", 30*time.Second),
		selfWeight:           getEnvFloat64("SELF_WEIGHT", 1.0),
		dataDir:              getEnvString("DATA_DIR", ""),
		memtableMaxBytes:     int64(getEnvPositiveInt("MEMTABLE_MAX_BYTES", 16<<20)),
		blockCacheBytes:      int64(getEnvInt("BLOCK_CACHE_BYTES", 16<<20)), // <= 0 disables the cache
	}
}

// advertiseHost extracts the host part of SELF_ADDRESS for building the
// default gossip advertise address; an address without a port is used as-is.
func advertiseHost(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}

// makeMemberChangeCallback returns the gossip onChange handler. hptr is an
// atomic pointer so the callback can reference the Handler before it exists —
// the pointer is populated after NewHandler returns.
func makeMemberChangeCallback(r *ring.Ring, hptr *atomic.Pointer[api.Handler]) func(*gossip.Member, gossip.MemberStatus) {
	return func(m *gossip.Member, status gossip.MemberStatus) {
		log.Printf("member %s status changed to %s", m.ID, status)
		switch status {
		case gossip.MemberAlive:
			r.AddWeightedNode(m.ID, m.Address, m.Weight)
			if h := hptr.Load(); h != nil {
				go h.DeliverHints(m.ID, m.Address)
			}
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
			log.Printf("removed dead member %s from ring", m.ID)
		case gossip.MemberBootstrapped:
			if h := hptr.Load(); h != nil {
				go h.CleanupStaleKeys()
			}
		}
	}
}

// restorePersistedIncarnation loads this node's last gossip incarnation from
// DATA_DIR, advances past it, persists the new value, and installs it on the
// member list — so a crash-restarted node's first gossip already supersedes any
// stale entry peers remember instead of waiting to refute. A refutation-driven
// advance at runtime is persisted through the same store via the sink. A
// persist failure is fatal: continuing would risk reusing an epoch and losing
// to a stale entry, defeating the point of persistence.
func restorePersistedIncarnation(dataDir string, ml *gossip.MemberList) {
	incStore := newIncarnationStore(dataDir)
	next := incStore.load() + 1
	if err := incStore.store(next); err != nil {
		log.Fatalf("gossip: persist incarnation: %v", err)
	}
	ml.SetSelfIncarnation(next)
	ml.SetIncarnationSink(func(v uint64) {
		if err := incStore.store(v); err != nil {
			log.Printf("gossip: persist incarnation %d: %v", v, err)
		}
	})
	log.Printf("gossip: restored incarnation %d", next)
}

// hintCapPerNode bounds buffered hints per target node; hintTTL bounds how
// long an undelivered hint is retained before anti-entropy takes over.
const (
	hintCapPerNode = 10_000
	hintTTL        = time.Hour
)

// openHintStore returns a crash-durable hint store backed by DATA_DIR/hints
// when persistence is enabled, or a memory-only store otherwise. A failure to
// open the persistent log is fatal: silently falling back to memory would
// reintroduce the durability gap persistence is meant to close.
func openHintStore(dataDir string) *hintstore.HintStore {
	if dataDir == "" {
		return hintstore.New(hintCapPerNode, hintTTL)
	}
	hs, err := hintstore.NewPersistent(filepath.Join(dataDir, "hints"), hintCapPerNode, hintTTL)
	if err != nil {
		log.Fatalf("hintstore: open failed: %v", err)
	}
	return hs
}

func runHintExpiry(ctx context.Context, hs *hintstore.HintStore) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			hs.ExpireOld()
		case <-ctx.Done():
			return
		}
	}
}

// runHintDelivery periodically sweeps buffered hints and delivers them to any
// currently-alive target. The gossip alive-transition callback already delivers
// on a dead→alive edge; this sweep is the backstop for targets that never
// present that edge — most importantly an asymmetric partition, where the
// isolated node keeps gossiping (so it looks alive) while inbound writes are
// dropped, so its hints would otherwise wait on anti-entropy. Undelivered hints
// are re-buffered, so a sweep against a still-unreachable target is a no-op
// beyond the failed delivery attempts. Interval is HINT_DELIVERY_INTERVAL_MS.
func runHintDelivery(ctx context.Context, h *api.Handler, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.DeliverPendingHints()
		case <-ctx.Done():
			return
		}
	}
}

// compactionInterval is how often the store is checked for a size-tiered
// compaction opportunity.
const compactionInterval = 30 * time.Second

// runCompaction periodically compacts the store's SSTables, cascading within a
// tick until no run qualifies. It uses the anti-entropy GC window so the
// bottom-level drop of aged tombstones matches GCTombstones. Returns on ctx
// cancellation; main waits for it before finalizing so no merge is in flight
// when the tables are closed.
func runCompaction(ctx context.Context, s *store.Store) {
	ticker := time.NewTicker(compactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			compactFully(ctx, s)
		}
	}
}

// compactFully cascades compaction within one tick until no run qualifies,
// bailing out promptly on ctx cancellation so a backlog of merges cannot
// stall shutdown past the drain window.
func compactFully(ctx context.Context, s *store.Store) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		merged, err := s.Compact(antientropy.GCTTL)
		if err != nil {
			log.Printf("store: compaction failed: %v", err)
			return
		}
		if !merged {
			return
		}
	}
}

func main() {
	cfg := loadConfig()

	// Recover persisted state before anything else touches the store. The
	// downtime gate may discard local data and force a fresh bootstrap; the
	// seed-node bootstrap path below handles that case naturally.
	s, persist, err := recoverStore(cfg.dataDir, cfg.selfID, antientropy.GCTTL, cfg.memtableMaxBytes, cfg.blockCacheBytes)
	if err != nil {
		log.Fatalf("persist: recover failed: %v", err)
	}
	api.RegisterBlockCacheMetrics(s.BlockCacheStats)

	r := ring.NewRing(cfg.replicas)
	r.SetUpdateCallback(func(nodeCount, vnodeCount int) {
		api.UpdateRingMetrics(nodeCount, vnodeCount)
	})

	hs := openHintStore(cfg.dataDir)

	var hptr atomic.Pointer[api.Handler]

	ml := gossip.NewMemberList(cfg.selfID, cfg.selfAddress, makeMemberChangeCallback(r, &hptr))
	ml.SetSelfWeight(cfg.selfWeight)
	ml.SetSelfGossipAddr(cfg.gossipAdvertise)
	if cfg.dataDir != "" {
		restorePersistedIncarnation(cfg.dataDir, ml)
	}

	r.SetHealthFilter(func(id string) bool {
		m, ok := ml.Get(id)
		return ok && m.Status == gossip.MemberAlive
	})

	transport, err := gossip.NewTransport(cfg.gossipPort)
	if err != nil {
		log.Fatalf("failed to create gossip transport: %v", err)
	}

	g := gossip.NewGossiper(cfg.selfID, cfg.gossipPort, ml, transport)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g.Start(ctx)

	// Mark self as bootstrapping before the gossip exchange so seed nodes
	// exclude us from read replica sets until data migration is complete.
	if len(cfg.seedNodes) > 0 {
		ml.SetBootstrapping(cfg.selfID, true)
		log.Printf("bootstrapping from seed nodes: %v", cfg.seedNodes)
		g.Bootstrap(cfg.seedNodes)
	}

	// add self to ring
	r.AddWeightedNode(cfg.selfID, cfg.selfAddress, cfg.selfWeight)

	_, httpPort, err := net.SplitHostPort(cfg.selfAddress)
	if err != nil {
		log.Fatalf("invalid SELF_ADDRESS %q: %v", cfg.selfAddress, err)
	}

	// With persistence enabled, a clean restart restores the Merkle trees from
	// the shutdown snapshot instead of rescanning every table; a crash (WAL
	// tail replayed past the snapshot) or missing snapshot rebuilds as before.
	merklePath := ""
	if cfg.dataDir != "" {
		merklePath = filepath.Join(cfg.dataDir, "merkle.json")
	}
	ae := antientropy.NewWithSnapshot(r, s, cfg.selfID, cfg.replicationFactor, cfg.replicaTimeout, merklePath)
	ae.SetSyncInterval(cfg.syncInterval)
	s.SetOnUpdate(ae.Update)
	s.SetOnEvict(ae.RemoveFromTrees)
	ae.Start(ctx)

	h := api.NewHandler(r, ml, s, api.HandlerConfig{
		SelfID:            cfg.selfID,
		ReplicationFactor: cfg.replicationFactor,
		WriteQuorum:       cfg.writeQuorum,
		ReadQuorum:        cfg.readQuorum,
		ReplicaTimeout:    cfg.replicaTimeout,
	}, hs)
	// Serve anti-entropy sync state from the manager's incrementally-maintained
	// Merkle trees instead of rescanning the store on every sync request.
	h.SetSyncTreeProvider(ae)
	hptr.Store(h)

	go runHintExpiry(ctx, hs)
	go runHintDelivery(ctx, h, cfg.hintDeliveryInterval)

	// Background compaction only runs when persistence is enabled (otherwise
	// there are no tables). Joined before finalize so no merge is mid-flight
	// when the tables are closed.
	var compactionDone chan struct{}
	if persist != nil {
		compactionDone = make(chan struct{})
		go func() {
			defer close(compactionDone)
			runCompaction(ctx, s)
		}()
	}

	// HTTP_BIND_PORT lets the listener bind a different port from the one in
	// the advertised SELF_ADDRESS (e.g. behind NAT, port mapping, or a fault-
	// injection proxy). Defaults to the advertised port.
	bindPort := cfg.httpBindPort
	if bindPort == "" {
		bindPort = httpPort
	}

	mux := api.BuildMux(h)
	srv := &http.Server{Addr: ":" + bindPort, Handler: mux}

	go func() {
		log.Printf("starting server on :%s (gossip on :%s) as %s", bindPort, cfg.gossipPort, cfg.selfID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Pull primary vnode ranges from existing replicas, then exit bootstrapping.
	if len(cfg.seedNodes) > 0 {
		go func() {
			h.Bootstrap()
			ml.SetBootstrapping(cfg.selfID, false)
		}()
	}

	<-ctx.Done()
	stop()
	log.Printf("shutdown: pushing keys to successors")
	h.PushKeysToSuccessors()

	log.Printf("shutdown: notifying peers")
	g.NotifyDead()

	log.Printf("shutdown: flushing pending hints to alive nodes")
	h.DeliverPendingHints()

	// Persist remove records from the flush and compact the hint log before exit.
	if err := hs.Close(); err != nil {
		log.Printf("shutdown: hintstore close error: %v", err)
	}

	log.Printf("shutdown: draining in-flight requests")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("shutdown: drain error: %v", err)
	}

	if compactionDone != nil {
		log.Printf("shutdown: waiting for in-flight compaction")
		<-compactionDone
	}

	log.Printf("shutdown: finalizing persistence")
	if err := persist.finalize(); err != nil {
		log.Printf("shutdown: finalize error: %v", err)
	}

	// Snapshot the Merkle trees so the next clean start skips the full store
	// scan. Requests are drained, so the trees and the store's LastSeq are
	// final. Best-effort: a failure just means the next start rebuilds.
	if merklePath != "" {
		if err := ae.SaveSnapshot(merklePath); err != nil {
			log.Printf("shutdown: merkle snapshot error: %v", err)
		}
	}

	g.Stop()
	log.Printf("shutdown: complete")
}
