package api

import (
	"fmt"
	"net/http"
	"time"
)

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()

		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, req)

		duration := time.Since(start).Seconds()
		status := fmt.Sprintf("%d", rec.status)

		httpRequestsTotal.WithLabelValues(req.Method, req.URL.Path, status).Inc()
		httpRequestDuration.WithLabelValues(req.Method, req.URL.Path).Observe(duration)
	})
}