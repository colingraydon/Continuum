//go:build fault

package fault

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// keyState is the single-writer history for one key. Each key is owned by
// exactly one worker, so sequence numbers give a total order per key and the
// verifier knows the exact last acknowledged write.
type keyState struct {
	key       string
	seq       uint64            // last attempted sequence
	lastAcked uint64            // last sequence that got a 204
	clocks    map[string]uint64 // causal context carried between writes
}

// seqOf extracts the sequence number a workload value encodes ("key#00000042").
func seqOf(value string) uint64 {
	i := strings.LastIndexByte(value, '#')
	if i < 0 {
		return 0
	}
	n, err := strconv.ParseUint(value[i+1:], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func valueFor(key string, seq uint64) string {
	return fmt.Sprintf("%s#%08d", key, seq)
}

// workload drives concurrent causal read-modify-write clients against the
// cluster: each worker owns a disjoint set of keys and loops GET (to pick up
// the current vector clocks) then PUT. 5xx responses and transport errors are
// tolerated and recorded; the verifier later checks that every acknowledged
// write survived.
type workload struct {
	c        *cluster
	keys     []*keyState
	perWorker int
	interval time.Duration
	targets  func() []*node

	stop chan struct{}
	wg   sync.WaitGroup

	mu     sync.Mutex
	acked  int
	failed int
}

// newWorkload creates workers*keysPerWorker keys. targets picks the candidate
// coordinator nodes each iteration; nil means every non-paused running node.
func newWorkload(c *cluster, workers, keysPerWorker int, targets func() []*node) *workload {
	if targets == nil {
		targets = c.alive
	}
	w := &workload{
		c:         c,
		perWorker: keysPerWorker,
		interval:  15 * time.Millisecond,
		targets:   targets,
		stop:      make(chan struct{}),
	}
	for wi := 0; wi < workers; wi++ {
		for ki := 0; ki < keysPerWorker; ki++ {
			w.keys = append(w.keys, &keyState{key: fmt.Sprintf("w%02d-k%02d", wi, ki)})
		}
	}
	return w
}

func (w *workload) run() {
	workers := len(w.keys) / w.perWorker
	for wi := 0; wi < workers; wi++ {
		owned := w.keys[wi*w.perWorker : (wi+1)*w.perWorker]
		w.wg.Add(1)
		go w.worker(owned, rand.New(rand.NewSource(int64(wi)+1)))
	}
}

func (w *workload) worker(owned []*keyState, rng *rand.Rand) {
	defer w.wg.Done()
	for i := 0; ; i++ {
		select {
		case <-w.stop:
			return
		default:
		}
		k := owned[i%len(owned)]
		w.step(k, rng)
		time.Sleep(w.interval + time.Duration(rng.Intn(10))*time.Millisecond)
	}
}

func (w *workload) step(k *keyState, rng *rand.Rand) {
	targets := w.targets()
	if len(targets) == 0 {
		time.Sleep(200 * time.Millisecond)
		return
	}
	n := targets[rng.Intn(len(targets))]

	// Refresh the causal context; a consistent read also triggers read repair
	// on divergent replicas as a side effect, which is part of the system
	// behavior under test.
	if nr, code, err := w.c.get(n, k.key); err == nil && code == http.StatusOK {
		k.clocks = mergedClocks(nr)
	}

	k.seq++
	code, err := w.c.put(n, k.key, valueFor(k.key, k.seq), k.clocks)
	w.mu.Lock()
	if err == nil && code == http.StatusNoContent {
		k.lastAcked = k.seq
		w.acked++
	} else {
		w.failed++
	}
	w.mu.Unlock()
}

// mergedClocks folds all clocks in a read response into one causal context
// that dominates everything the coordinator returned.
func mergedClocks(nr nodeResponse) map[string]uint64 {
	out := make(map[string]uint64)
	fold := func(clocks map[string]uint64) {
		for id, v := range clocks {
			if out[id] < v {
				out[id] = v
			}
		}
	}
	fold(nr.Clocks)
	for _, s := range nr.Siblings {
		fold(s.Clocks)
	}
	return out
}

// halt stops the workers and returns (acked, failed) op counts.
func (w *workload) halt() (int, int) {
	close(w.stop)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acked, w.failed
}

func (w *workload) keyNames() []string {
	names := make([]string, len(w.keys))
	for i, k := range w.keys {
		names[i] = k.key
	}
	return names
}
