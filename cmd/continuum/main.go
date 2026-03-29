package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

type config struct {
	replicas          int
	replicationFactor int
	selfID            string
	selfAddress       string
	gossipPort        string
	seedNodes         []string
}

func loadConfig() config {
	replicas := 150
	if val := os.Getenv("REPLICAS"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			replicas = parsed
		}
	}

	replicationFactor := 3
	if val := os.Getenv("REPLICATION_FACTOR"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			replicationFactor = parsed
		}
	}

	selfAddress := os.Getenv("SELF_ADDRESS")
	if selfAddress == "" {
		selfAddress = "localhost:8080"
	}

	selfID := os.Getenv("SELF_ID")
	if selfID == "" {
		selfID = selfAddress
	}

	gossipPort := os.Getenv("GOSSIP_PORT")
	if gossipPort == "" {
		gossipPort = "8081"
	}

	var seedNodes []string
	if val := os.Getenv("SEED_NODES"); val != "" {
		seedNodes = strings.Split(val, ",")
	}

	return config{
		replicas:          replicas,
		replicationFactor: replicationFactor,
		selfID:            selfID,
		selfAddress:       selfAddress,
		gossipPort:        gossipPort,
		seedNodes:         seedNodes,
	}
}

func main() {
	cfg := loadConfig()

	r := ring.NewRing(cfg.replicas)
	r.SetUpdateCallback(func(nodeCount, vnodeCount int) {
		api.UpdateRingMetrics(nodeCount, vnodeCount)
	})

	ml := gossip.NewMemberList(cfg.selfID, cfg.selfAddress, func(m *gossip.Member, status gossip.MemberStatus) {
		log.Printf("member %s status changed to %s", m.ID, status)
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
			log.Printf("removed dead member %s from ring", m.ID)
		}
	})

	r.SetHealthFilter(func(id string) bool {
		m, ok := ml.Get(id)
		return ok && m.Status == gossip.MemberAlive
	})

	transport, err := gossip.NewTransport(cfg.gossipPort)
	if err != nil {
		log.Fatalf("failed to create gossip transport: %v", err)
	}

	g := gossip.NewGossiper(cfg.selfID, cfg.gossipPort, ml, transport)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g.Start(ctx)

	if len(cfg.seedNodes) > 0 {
		log.Printf("bootstrapping from seed nodes: %v", cfg.seedNodes)
		g.Bootstrap(cfg.seedNodes)
	}

	// add self to ring
	r.AddNode(cfg.selfID, cfg.selfAddress)

	s := store.New()
	mux := api.NewServer(r, ml, g, s, cfg.selfID, cfg.replicationFactor)
	log.Printf("starting server on :8080 (gossip on :%s) as %s", cfg.gossipPort, cfg.selfID)
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("server failed to start: %v", err)
	}
}