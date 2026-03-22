package main

import (
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/ring"
)

func main() {
	replicas := 150
	if val := os.Getenv("REPLICAS"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			replicas = parsed
		}
	}

	r := ring.NewRing(replicas)
	mux := api.NewServer(r)

	log.Printf("Starting server on :8080 with %d replicas", replicas)
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Server failed to start with err: %v", err)
	}
}