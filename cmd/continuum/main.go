package main

import (
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/ring"
)

type config struct {
	replicas     int
	defaultNodes []ring.Node
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
		defaultNodes: []ring.Node{
			{ID: "node1", Address: "10.0.0.1"},
			{ID: "node2", Address: "10.0.0.2"},
			{ID: "node3", Address: "10.0.0.3"},
		},
	}
}

func main() {
	cfg := loadConfig()

	r := ring.NewRing(cfg.replicas)
	r.SetUpdateCallback(func(nodeCount, vnodeCount int) {
		api.UpdateRingMetrics(nodeCount, vnodeCount)
	})

	for _, n := range cfg.defaultNodes {
		r.AddNode(n.ID, n.Address)
	}

	mux := api.NewServer(r)

	log.Printf("Starting server on :8080 with %d replicas and %d default nodes", cfg.replicas, len(cfg.defaultNodes))
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Server failed to start with err: %v", err)
	}
}