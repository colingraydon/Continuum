package main

import (
	"fmt"
	"sort"
	"time"
)

// ScenarioResult is one scenario's latency distribution, all durations in
// nanoseconds so consumers format units themselves.
type ScenarioResult struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Samples     int     `json:"samples"`
	Throughput  float64 `json:"throughput_ops_per_sec"`
	MeanNs      int64   `json:"mean_ns"`
	MinNs       int64   `json:"min_ns"`
	P50Ns       int64   `json:"p50_ns"`
	P90Ns       int64   `json:"p90_ns"`
	P99Ns       int64   `json:"p99_ns"`
	P999Ns      int64   `json:"p999_ns"`
	MaxNs       int64   `json:"max_ns"`
}

// summarize computes the distribution summary for one scenario's recorded
// latencies. wall is the total wall-clock time the samples took, which drives
// throughput (for concurrent scenarios it is shorter than the latency sum).
func summarize(name, description string, latencies []time.Duration, wall time.Duration) ScenarioResult {
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	n := len(sorted)
	throughput := 0.0
	if wall > 0 {
		throughput = float64(n) / wall.Seconds()
	}
	return ScenarioResult{
		Name:        name,
		Description: description,
		Samples:     n,
		Throughput:  throughput,
		MeanNs:      int64(sum) / int64(n),
		MinNs:       int64(sorted[0]),
		P50Ns:       int64(percentile(sorted, 0.50)),
		P90Ns:       int64(percentile(sorted, 0.90)),
		P99Ns:       int64(percentile(sorted, 0.99)),
		P999Ns:      int64(percentile(sorted, 0.999)),
		MaxNs:       int64(sorted[n-1]),
	}
}

// percentile returns the value at rank p (0..1) of an ascending-sorted slice
// using the nearest-rank method - exact over the recorded samples, no
// interpolation or approximation.
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// csvHeader and csvRow render the flat CSV companion to the JSON snapshot.
func csvHeader() string {
	return "name,samples,throughput_ops_per_sec,mean_ns,min_ns,p50_ns,p90_ns,p99_ns,p999_ns,max_ns"
}

func csvRow(r ScenarioResult) string {
	return fmt.Sprintf("%s,%d,%.1f,%d,%d,%d,%d,%d,%d,%d",
		r.Name, r.Samples, r.Throughput, r.MeanNs, r.MinNs, r.P50Ns, r.P90Ns, r.P99Ns, r.P999Ns, r.MaxNs)
}
