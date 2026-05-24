package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
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
	replicas          int
	replicationFactor int
	writeQuorum       int
	readQuorum        int
	selfID            string
	selfAddress       string
	gossipPort        string
	seedNodes         []string
	replicaTimeout    time.Duration
	selfWeight        float64
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

	return config{
		replicas:          getEnvInt("REPLICAS", 150),
		replicationFactor: replicationFactor,
		writeQuorum:       writeQuorum,
		readQuorum:        readQuorum,
		selfID:            getEnvString("SELF_ID", selfAddress),
		selfAddress:       selfAddress,
		gossipPort:        getEnvString("GOSSIP_PORT", "8081"),
		seedNodes:         seedNodes,
		replicaTimeout:    getEnvDurationMs("REPLICA_TIMEOUT_MS", 500*time.Millisecond),
		selfWeight:        getEnvFloat64("SELF_WEIGHT", 1.0),
	}
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

func main() {
	cfg := loadConfig()

	r := ring.NewRing(cfg.replicas)
	r.SetUpdateCallback(func(nodeCount, vnodeCount int) {
		api.UpdateRingMetrics(nodeCount, vnodeCount)
	})

	hs := hintstore.New(10_000, time.Hour)

	var hptr atomic.Pointer[api.Handler]

	ml := gossip.NewMemberList(cfg.selfID, cfg.selfAddress, makeMemberChangeCallback(r, &hptr))
	ml.SetSelfWeight(cfg.selfWeight)

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

	s := store.New()
	ae := antientropy.New(r, s, cfg.selfID, cfg.replicationFactor, cfg.replicaTimeout)
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
	hptr.Store(h)

	go runHintExpiry(ctx, hs)

	mux := api.BuildMux(h)
	srv := &http.Server{Addr: ":" + httpPort, Handler: mux}

	go func() {
		log.Printf("starting server on :%s (gossip on :%s) as %s", httpPort, cfg.gossipPort, cfg.selfID)
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
	h.FlushHints()

	log.Printf("shutdown: draining in-flight requests")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("shutdown: drain error: %v", err)
	}

	g.Stop()
	log.Printf("shutdown: complete")
}