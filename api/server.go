package api

import (
	"fmt"
	"net/http"
)

func NewServer() *http.ServeMux {
	fmt.Println("Starting server on 8080...");
	mux := http.NewServeMux();
	mux.HandleFunc("/hello", hello);
	return mux;
}