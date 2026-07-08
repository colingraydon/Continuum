package histcheck

import (
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRecorderCollectsHistory(t *testing.T) {
	r := NewRecorder()

	// Now advances with wall time and never precedes the base.
	if r.Now() < 0 {
		t.Fatalf("Now() must be non-negative, got %d", r.Now())
	}
	// At offsets from the same base Now counts from.
	if got := r.At(time.Second).Sub(r.At(0)); got != time.Second {
		t.Fatalf("At offset = %v, want 1s", got)
	}

	// Concurrent Add is safe and every op is retained.
	const writers, each = 4, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				r.Add(Op{Client: w, Key: "k", Kind: CASPut})
			}
		}(w)
	}
	wg.Wait()

	hist := r.History()
	if len(hist) != writers*each {
		t.Fatalf("recorded %d ops, want %d", len(hist), writers*each)
	}
	// History returns a copy: mutating it does not affect the recorder.
	hist[0].Key = "mutated"
	if again := r.History(); again[0].Key == "mutated" {
		t.Fatal("History must return a copy")
	}
}

func TestCASStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code int
		want Status
	}{
		{"ack", nil, http.StatusNoContent, StatusOK},
		{"conflict", nil, http.StatusPreconditionFailed, StatusConflict},
		{"server error", nil, http.StatusServiceUnavailable, StatusUnknown},
		{"transport error", http.ErrHandlerTimeout, 0, StatusUnknown},
	}
	for _, c := range cases {
		if got := CASStatus(c.err, c.code); got != c.want {
			t.Errorf("%s: CASStatus = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestCheckAndVisualize(t *testing.T) {
	t.Setenv("CONTINUUM_HISTORY_DIR", t.TempDir())

	// A linearizable history: no visualization, no path.
	ok := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 0, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
	})
	verdict, ops, path := CheckAndVisualize(ok, 10*time.Second, "ok-history")
	if verdict != VerdictLinearizable || ops != len(ok) || path != "" {
		t.Fatalf("linearizable: verdict=%v ops=%d path=%q", verdict, ops, path)
	}

	// A violating history: a visualization file is written to the configured dir.
	bad := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 1, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
		{Client: 1, Key: "k", Kind: CASPut, Expected: "v1", Value: "v2", Status: StatusOK},
		{Client: 0, Key: "k", Kind: Read, Found: true, ReadValue: "v1"}, // stale
	})
	verdict, _, path = CheckAndVisualize(bad, 10*time.Second, "bad/history")
	if verdict != VerdictNotLinearizable {
		t.Fatalf("expected NotLinearizable, got %v", verdict)
	}
	if path == "" {
		t.Fatal("a violation must produce a visualization path")
	}
	if st, err := os.Stat(path); err != nil || st.Size() == 0 {
		t.Fatalf("visualization file missing or empty: %v", err)
	}

	// With no configured dir, the file falls back to the OS temp dir.
	os.Unsetenv("CONTINUUM_HISTORY_DIR")
	_, _, path = CheckAndVisualize(bad, 10*time.Second, "fallback-history")
	if path == "" || !strings.HasPrefix(path, os.TempDir()) {
		t.Fatalf("expected a temp-dir path, got %q", path)
	}
}
