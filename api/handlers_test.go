package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHello(t *testing.T) {
	//Arrange
	req := httptest.NewRequest(http.MethodGet, "/hello", nil);
	w := httptest.NewRecorder();

	// Act
	hello(w, req)

	// Assert
	expected := "Hello world!";
	if w.Body.String() != expected {
		t.Errorf("Expected body %q, got %q", expected, w.Body.String());
	}
}