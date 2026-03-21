package api

import (
	"fmt"
	"net/http"
)

func hello(w http.ResponseWriter, req *http.Request) {
	
	if _, err := fmt.Fprintf(w, "Hello world!"); err != nil {
		http.Error(w, "Failed to write response", http.StatusInternalServerError);
	}
}

