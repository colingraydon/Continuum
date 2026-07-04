// benchreport generates the published benchmark dataset: it runs a curated
// set of latency scenarios with per-operation timing, computes exact
// percentiles over the recorded samples, and writes a provenance-stamped
// JSON snapshot (plus a flat CSV and an append-only history line) for a
// static frontend to consume.
//
// Percentiles come from a dedicated harness rather than `go test -bench`
// because the bench framework reports only means. Scenarios are chosen to be
// microsecond-scale or slower, where individual timer readings are
// meaningful; the one nanosecond-scale entry (ring lookup) is measured over
// 1000-op batches and labeled as such.
//
// Run on a known machine (not shared CI) so the published numbers are
// reproducible and citable:
//
//	make bench-report
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Report is the published snapshot: provenance plus one entry per scenario.
type Report struct {
	GeneratedAt string           `json:"generated_at"`
	GitCommit   string           `json:"git_commit"`
	GoVersion   string           `json:"go_version"`
	OS          string           `json:"os"`
	Arch        string           `json:"arch"`
	CPUs        int              `json:"cpus"`
	CPUModel    string           `json:"cpu_model"`
	Scenarios   []ScenarioResult `json:"scenarios"`
}

func main() {
	out := flag.String("out", "docs/data", "output directory for benchmarks.json, benchmarks.csv, history.ndjson")
	scale := flag.Float64("scale", 1.0, "sample-count multiplier (use <1 for quick runs)")
	flag.Parse()

	report, err := generate(*scale, os.TempDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchreport: %v\n", err)
		os.Exit(1)
	}
	if err := writeOutputs(*out, report); err != nil {
		fmt.Fprintf(os.Stderr, "benchreport: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s/benchmarks.json (%d scenarios, commit %s)\n", *out, len(report.Scenarios), report.GitCommit)
}

// generate runs every scenario at the given sample scale and assembles the
// provenance-stamped report. tmpParent hosts durable-store fixtures.
func generate(scale float64, tmpParent string) (*Report, error) {
	tmpDir, err := os.MkdirTemp(tmpParent, "benchreport-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	report := &Report{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		GitCommit:   gitCommit(),
		GoVersion:   runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CPUs:        runtime.NumCPU(),
		CPUModel:    cpuModel(),
	}
	for _, sc := range scenarios(tmpDir) {
		samples := int(float64(sc.samples) * scale)
		if samples < 8 {
			samples = 8
		}
		fmt.Fprintf(os.Stderr, "running %-24s (%d samples)\n", sc.name, samples)
		latencies, wall, err := sc.run(samples)
		if err != nil {
			return nil, fmt.Errorf("scenario %s: %w", sc.name, err)
		}
		report.Scenarios = append(report.Scenarios, summarize(sc.name, sc.description, latencies, wall))
	}
	return report, nil
}

// writeOutputs writes the JSON snapshot, the CSV companion, and appends the
// snapshot as one line to the history file.
func writeOutputs(dir string, report *Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	blob, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "benchmarks.json"), append(blob, '\n'), 0o644); err != nil {
		return err
	}

	var csv strings.Builder
	csv.WriteString(csvHeader() + "\n")
	for _, r := range report.Scenarios {
		csv.WriteString(csvRow(r) + "\n")
	}
	if err := os.WriteFile(filepath.Join(dir, "benchmarks.csv"), []byte(csv.String()), 0o644); err != nil {
		return err
	}

	line, err := json.Marshal(report)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "history.ndjson"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Write(append(line, '\n'))
	return err
}

// gitCommit returns the short HEAD hash by reading the .git files directly
// (no subprocess), or "unknown" outside a git checkout.
func gitCommit() string {
	head, err := os.ReadFile(filepath.Join(".git", "HEAD"))
	if err != nil {
		return "unknown"
	}
	ref := strings.TrimSpace(string(head))
	if !strings.HasPrefix(ref, "ref: ") {
		return shortHash(ref) // detached HEAD holds the hash itself
	}
	refPath := strings.TrimPrefix(ref, "ref: ")
	if hash, err := os.ReadFile(filepath.Join(".git", filepath.FromSlash(refPath))); err == nil {
		return shortHash(strings.TrimSpace(string(hash)))
	}
	// The ref may only exist in packed-refs.
	packed, err := os.ReadFile(filepath.Join(".git", "packed-refs"))
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(packed), "\n") {
		if hash, ok := strings.CutSuffix(line, " "+refPath); ok {
			return shortHash(hash)
		}
	}
	return "unknown"
}

func shortHash(hash string) string {
	if len(hash) < 7 {
		return "unknown"
	}
	return hash[:7]
}

// cpuModel returns a human-readable CPU name where the platform exposes one,
// without shelling out (see cpumodel_*.go).
func cpuModel() string {
	if model := platformCPUModel(); model != "" {
		return model
	}
	return "unknown"
}
