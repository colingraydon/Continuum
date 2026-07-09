package histcheck

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CASStatus classifies a conditional-write client outcome into the model's
// Status. A 204 committed; a 412 provably did not (no side effects); anything
// else — a transport error or 5xx — is unknown, since a 503 for missed quorum
// can be returned after the local commit, so the write may or may not have
// taken effect.
func CASStatus(err error, code int) Status {
	switch {
	case err == nil && code == http.StatusNoContent:
		return StatusOK
	case err == nil && code == http.StatusPreconditionFailed:
		return StatusConflict
	default:
		return StatusUnknown
	}
}

// Recorder collects a concurrent operation history on one monotonic
// timescale, safe for use from many client goroutines. Both the process-level
// fault harness and the in-process simulation harness record histories
// through it before checking them.
type Recorder struct {
	base time.Time
	mu   sync.Mutex
	ops  []Op
}

// NewRecorder returns a recorder whose timestamps count from now.
func NewRecorder() *Recorder { return &Recorder{base: time.Now()} }

// Now returns nanoseconds since the recorder's base — the value to stamp on
// an operation's Call and Return.
func (r *Recorder) Now() int64 { return time.Since(r.base).Nanoseconds() }

// At returns the absolute time offset from the recorder's base, for pacing a
// fault schedule against the same clock the operations are stamped on.
func (r *Recorder) At(offset time.Duration) time.Time { return r.base.Add(offset) }

// Add appends a completed operation to the history.
func (r *Recorder) Add(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// History returns a copy of the operations recorded so far.
func (r *Recorder) History() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Op, len(r.ops))
	copy(out, r.ops)
	return out
}

// Verdict is the coarse outcome of a linearizability check, for test
// assertions: proven linearizable, proven not, or undecided (the NP-hard
// search timed out).
type Verdict int

const (
	VerdictLinearizable Verdict = iota
	VerdictNotLinearizable
	VerdictUndecided
)

// CheckAndVisualize checks ops for per-key linearizability and, on a
// violation, writes porcupine's interactive HTML visualization so the
// failure can be inspected. It returns the verdict, the operation count, and
// the visualization path (empty unless a file was written). The output
// directory is CONTINUUM_HISTORY_DIR, or the OS temp dir; label names the
// file. Both harnesses share this so the check-and-report flow lives in one
// place.
func CheckAndVisualize(ops []Op, timeout time.Duration, label string) (Verdict, int, string) {
	res := Check(ops, timeout)
	switch {
	case res.Linearizable():
		return VerdictLinearizable, res.Ops(), ""
	case res.Undecided():
		return VerdictUndecided, res.Ops(), ""
	default:
		dir := os.Getenv("CONTINUUM_HISTORY_DIR")
		if dir == "" {
			dir = os.TempDir()
		}
		path := filepath.Join(dir, strings.ReplaceAll(label, "/", "_")+".html")
		if err := res.Visualize(path); err != nil {
			path = ""
		}
		return VerdictNotLinearizable, res.Ops(), path
	}
}
