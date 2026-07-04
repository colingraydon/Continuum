package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	// 1..100 ms ascending: nearest-rank percentiles are exact.
	sorted := make([]time.Duration, 100)
	for i := range sorted {
		sorted[i] = time.Duration(i+1) * time.Millisecond
	}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{0.0, 1 * time.Millisecond},
		{0.50, 50 * time.Millisecond},
		{0.90, 90 * time.Millisecond},
		{0.99, 99 * time.Millisecond},
		{1.0, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		if got := percentile(sorted, tc.p); got != tc.want {
			t.Errorf("p%.0f = %v, want %v", tc.p*100, got, tc.want)
		}
	}
}

func TestSummarize(t *testing.T) {
	latencies := []time.Duration{
		3 * time.Millisecond, 1 * time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond,
	}
	r := summarize("s", "d", latencies, 20*time.Millisecond)

	if r.Samples != 4 || r.MinNs != int64(time.Millisecond) || r.MaxNs != int64(4*time.Millisecond) {
		t.Errorf("summary bounds wrong: %+v", r)
	}
	if r.MeanNs != int64(2500*time.Microsecond) {
		t.Errorf("mean = %d, want 2.5ms", r.MeanNs)
	}
	// 4 samples over 20ms wall = 200 ops/sec.
	if r.Throughput < 199 || r.Throughput > 201 {
		t.Errorf("throughput = %f, want ~200", r.Throughput)
	}
	if r.P50Ns != int64(2*time.Millisecond) {
		t.Errorf("p50 = %d, want 2ms", r.P50Ns)
	}
}

func TestCSVRow(t *testing.T) {
	r := ScenarioResult{Name: "x", Samples: 10, Throughput: 123.45, MeanNs: 1, MinNs: 2, P50Ns: 3, P90Ns: 4, P99Ns: 5, P999Ns: 6, MaxNs: 7}
	if got, want := csvRow(r), "x,10,123.5,1,2,3,4,5,6,7"; got != want {
		t.Errorf("csvRow = %q, want %q", got, want)
	}
	if !strings.HasPrefix(csvHeader(), "name,samples,") {
		t.Errorf("csvHeader = %q", csvHeader())
	}
}

// TestGenerateSmoke runs the entire report pipeline end to end at minimal
// sample counts: every scenario (real cluster, durable store, sync trees)
// must produce sane, positive numbers, and the three output files must land.
func TestGenerateSmoke(t *testing.T) {
	report, err := generate(0.001, t.TempDir()) // clamps to 8 samples per scenario
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(report.Scenarios) == 0 {
		t.Fatal("no scenarios ran")
	}
	for _, s := range report.Scenarios {
		if s.Samples < 8 || s.MinNs <= 0 || s.P50Ns < s.MinNs || s.P99Ns < s.P50Ns || s.MaxNs < s.P99Ns {
			t.Errorf("scenario %s: implausible summary %+v", s.Name, s)
		}
		if s.Throughput <= 0 {
			t.Errorf("scenario %s: non-positive throughput", s.Name)
		}
	}
	if report.GoVersion == "" || report.OS == "" || report.GeneratedAt == "" {
		t.Errorf("incomplete provenance: %+v", report)
	}

	out := t.TempDir()
	if err := writeOutputs(out, report); err != nil {
		t.Fatalf("writeOutputs: %v", err)
	}
	blob, err := os.ReadFile(filepath.Join(out, "benchmarks.json"))
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var back Report
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatalf("snapshot does not round-trip: %v", err)
	}
	if len(back.Scenarios) != len(report.Scenarios) {
		t.Errorf("round-trip lost scenarios: %d != %d", len(back.Scenarios), len(report.Scenarios))
	}
	for _, name := range []string{"benchmarks.csv", "history.ndjson"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Errorf("missing output %s: %v", name, err)
		}
	}

	// History appends: a second write adds a second line.
	if err := writeOutputs(out, report); err != nil {
		t.Fatalf("second writeOutputs: %v", err)
	}
	hist, err := os.ReadFile(filepath.Join(out, "history.ndjson"))
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	if lines := strings.Count(string(hist), "\n"); lines != 2 {
		t.Errorf("history has %d lines, want 2 (append-only)", lines)
	}
}
