package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/health"
	"github.com/colingraydon/continuum/internal/ring"
)

type config struct {
	replicas     int
	defaultNodes []*ring.Node
}

func loadConfig() config {
	replicas := 150
	if val := os.Getenv("REPLICAS"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			replicas = parsed
		}
	}

	return config{
		replicas: replicas,
		defaultNodes: []*ring.Node{},
	}
}

func main() {
	cfg := loadConfig()

	r := ring.NewRing(cfg.replicas)
	r.SetUpdateCallback(func(nodeCount, vnodeCount int) {
		api.UpdateRingMetrics(nodeCount, vnodeCount)
	})

	checker := health.NewChecker(health.DefaultConfig(), func(nodeID string, status health.NodeStatus) {
		log.Printf("node %s status changed to %s", nodeID, status)
		if status == health.StatusDead {
			r.RemoveNode(nodeID)
			log.Printf("removed dead node %s from ring", nodeID)
		}
	})

	for _, n := range cfg.defaultNodes {
		r.AddNode(n.ID, n.Address)
		checker.AddNode(n.ID, n.Address)
	}

	ctx := context.Background()
	checker.Start(ctx)
	defer checker.Stop()

	mux := api.NewServer(r, checker)

	log.Printf("Starting server on :8080 with %d replicas and %d default nodes", cfg.replicas, len(cfg.defaultNodes))
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Server failed to start with err: %v", err)
	}
}