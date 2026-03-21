package main

import (
	"log"
	"net/http"

	"github.com/colingraydon/continuum/api"
)


func main() {
	mux := api.NewServer();
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Server failed to start with err: %v", err);
	}
}