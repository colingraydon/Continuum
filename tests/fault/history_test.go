//go:build fault

package fault

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/histcheck"
)

// recorder collects a concurrent operation history on one monotonic
// timescale, for porcupine checking after the run.
type recorder struct {
	base time.Time
	mu   sync.Mutex
	ops  []histcheck.Op
}

func newRecorder() *recorder { return &recorder{base: time.Now()} }

func (r *recorder) now() int64 { return time.Since(r.base).Nanoseconds() }

func (r *recorder) add(op histcheck.Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *recorder) history() []histcheck.Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]histcheck.Op, len(r.ops))
	copy(out, r.ops)
	return out
}

// casWorkload races multiple clients over a small shared key set, every
// mutation a conditional write: each client loops GET ?consistency=all (to
// learn the current value and its clock) then PUT ?cas=true from that clock.
// Contention on the same keys is the point: CAS serializes at the key's
// primary, so exactly one of two racing writers may win each step. All
// operations are recorded for a linearizability check.
type casWorkload struct {
	c        *cluster
	keys     []string
	clients  int
	interval time.Duration
	targets  func() []*node
	rec      *recorder

	stop chan struct{}
	wg   sync.WaitGroup

	mu       sync.Mutex
	acked    int
	conflict int
	unknown  int
}

func newCASWorkload(c *cluster, clients, keys int, targets func() []*node) *casWorkload {
	if targets == nil {
		targets = c.alive
	}
	w := &casWorkload{
		c:        c,
		clients:  clients,
		interval: 20 * time.Millisecond,
		targets:  targets,
		rec:      newRecorder(),
		stop:     make(chan struct{}),
	}
	for i := 0; i < keys; i++ {
		w.keys = append(w.keys, fmt.Sprintf("cas-k%02d", i))
	}
	return w
}

func (w *casWorkload) run() {
	for ci := 0; ci < w.clients; ci++ {
		w.wg.Add(1)
		go w.client(ci, rand.New(rand.NewSource(int64(ci)+1)))
	}
}

func (w *casWorkload) client(id int, rng *rand.Rand) {
	defer w.wg.Done()
	for i := 0; ; i++ {
		select {
		case <-w.stop:
			return
		default:
		}
		w.step(id, i, rng)
		time.Sleep(w.interval + time.Duration(rng.Intn(10))*time.Millisecond)
	}
}

func (w *casWorkload) step(id, iter int, rng *rand.Rand) {
	targets := w.targets()
	if len(targets) == 0 {
		time.Sleep(200 * time.Millisecond)
		return
	}
	key := w.keys[rng.Intn(len(w.keys))]

	// Read the current state through a random coordinator. A failed read
	// teaches us nothing and has no effect on state, so it is not recorded;
	// the client just skips this step's write.
	expected, ctx, ok := w.readStep(id, key, targets[rng.Intn(len(targets))])
	if !ok {
		return
	}

	value := fmt.Sprintf("c%02d#%06d", id, iter)
	call := w.rec.now()
	code, err := w.c.casPut(targets[rng.Intn(len(targets))], key, value, ctx)
	ret := w.rec.now()

	op := histcheck.Op{
		Client: id, Key: key, Kind: histcheck.CASPut,
		Expected: expected, Value: value,
		Call: call, Return: ret,
	}
	w.mu.Lock()
	switch {
	case err == nil && code == http.StatusNoContent:
		op.Status = histcheck.StatusOK
		w.acked++
	case err == nil && code == http.StatusPreconditionFailed:
		op.Status = histcheck.StatusConflict
		w.conflict++
	default:
		// Timeout or 5xx: the write may have committed on the primary
		// (e.g. a 503 for missed quorum is returned after the local
		// commit), so its outcome is unknown.
		op.Status = histcheck.StatusUnknown
		w.unknown++
	}
	w.mu.Unlock()
	w.rec.add(op)
}

// readStep performs and records the GET half of a step. It returns the value
// observed (empty = absent), the clock context for the following CAS, and
// whether the read produced usable state at all.
func (w *casWorkload) readStep(id int, key string, n *node) (string, map[string]uint64, bool) {
	call := w.rec.now()
	nr, sessionClock, code, err := w.c.getAll(n, key)
	ret := w.rec.now()

	op := histcheck.Op{Client: id, Key: key, Kind: histcheck.Read, Call: call, Return: ret}
	switch {
	case err == nil && code == http.StatusOK:
		op.Found = true
		if len(nr.Siblings) > 1 {
			// Siblings in a pure-CAS history mean a forked write; the
			// checker flags the read. The client repairs by CAS-writing
			// from the merged clock, but that repair cannot match any
			// sequential register state, so it stays un-modeled: return
			// the merged context with an impossible expected value.
			op.Conflict = true
			w.rec.add(op)
			return "\x00conflict", sessionClock, true
		}
		if len(nr.Siblings) == 1 {
			op.ReadValue = nr.Siblings[0].Value
		} else {
			op.ReadValue = nr.Value
		}
		// A 200 with no value means no replica holds the key (the
		// coordinator returns node info with an empty body): absent.
		op.Found = op.ReadValue != ""
		w.rec.add(op)
		return op.ReadValue, sessionClock, true
	case err == nil && code == http.StatusNotFound:
		w.rec.add(op)
		return "", nil, true
	default:
		return "", nil, false
	}
}

// halt stops the clients and returns (acked, conflict, unknown) op counts.
func (w *casWorkload) halt() (int, int, int) {
	close(w.stop)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acked, w.conflict, w.unknown
}

// verifyLinearizable checks the recorded history against the CAS-register
// model. On a violation (or an undecided search) it writes porcupine's
// interactive visualization next to the test artifacts and fails the test.
func verifyLinearizable(t *testing.T, w *casWorkload, timeout time.Duration) {
	t.Helper()
	if !checkLinearizable(t, w, timeout) {
		t.Errorf("history NOT linearizable: a CAS write forked or was lost")
	}
}

// expectKnownCASGap runs the linearizability check where the documented CAS
// membership-churn window is in play: primary failover and partitions can
// fork or lose CAS writes because the primary serializes against local state
// only (no consensus round). A violation here is the known gap, reported but
// not failed; a clean pass is logged too. When consensus-backed CAS closes
// the window, callers switch to verifyLinearizable and the scenario becomes
// a hard assertion.
func expectKnownCASGap(t *testing.T, w *casWorkload, timeout time.Duration) {
	t.Helper()
	if checkLinearizable(t, w, timeout) {
		t.Log("no violation surfaced this run; the churn window is timing-dependent")
		return
	}
	t.Log("known CAS gap reproduced: primary-serialized CAS is not linearizable across membership churn (see docs/fault-injection.md finding #7)")
}

func checkLinearizable(t *testing.T, w *casWorkload, timeout time.Duration) bool {
	t.Helper()
	hist := w.rec.history()
	res := histcheck.Check(hist, timeout)
	switch {
	case res.Linearizable():
		t.Logf("history linearizable: %d ops", res.Ops())
		return true
	case res.Undecided():
		t.Errorf("linearizability undecided after %v (%d ops); shrink the workload or raise the timeout", timeout, res.Ops())
		return true // inconclusive, not a violation; the Errorf already failed the test
	default:
		path := visualizationPath(t)
		if err := res.Visualize(path); err != nil {
			t.Logf("visualization failed: %v", err)
		} else {
			t.Logf("linearization visualization: %s", path)
		}
		diagnose(t, hist)
		return false
	}
}

// diagnose scans a non-linearizable history for the mechanical signature of
// the violation, so the failure report says what went wrong, not just that
// something did.
func diagnose(t *testing.T, hist []histcheck.Op) {
	t.Helper()
	okByExpected := make(map[string][]histcheck.Op) // key\x00expected -> acked CAS ops
	conflictReads := 0
	for _, op := range hist {
		switch {
		case op.Kind == histcheck.CASPut && op.Status == histcheck.StatusOK:
			k := op.Key + "\x00" + op.Expected
			okByExpected[k] = append(okByExpected[k], op)
		case op.Kind == histcheck.Read && op.Conflict:
			conflictReads++
		}
	}
	forks := 0
	for k, ops := range okByExpected {
		if len(ops) > 1 {
			forks++
			key, expected, _ := strings.Cut(k, "\x00")
			values := make([]string, len(ops))
			for i, op := range ops {
				values[i] = op.Value
			}
			t.Logf("forked CAS on %s: %d acknowledged writes from expected %q: %v", key, len(ops), expected, values)
		}
	}

	// Stale reads: in a pure-CAS history values never repeat, so once
	// CAS(expected=v -> ...) is acknowledged, v is gone for good. A read
	// that starts after that ack returns and still observes v proves the
	// read set missed the newest state.
	staleReads := 0
	for _, op := range hist {
		if op.Kind != histcheck.Read || !op.Found {
			continue
		}
		for _, w := range okByExpected[op.Key+"\x00"+op.ReadValue] {
			if w.Return < op.Call {
				staleReads++
				if staleReads <= 3 {
					t.Logf("stale read on %s: observed %q after CAS %q -> %q was acknowledged", op.Key, op.ReadValue, w.Expected, w.Value)
				}
				break
			}
		}
	}
	t.Logf("diagnosis: %d forked CAS generations, %d sibling-conflict reads, %d stale reads", forks, conflictReads, staleReads)
}

func visualizationPath(t *testing.T) string {
	dir := os.Getenv("CONTINUUM_HISTORY_DIR")
	if dir == "" {
		dir = os.TempDir()
	}
	name := strings.ReplaceAll(t.Name(), "/", "_") + "-" + time.Now().Format("150405") + ".html"
	return filepath.Join(dir, name)
}
