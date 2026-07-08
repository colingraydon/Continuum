//go:build sim

package sim

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/colingraydon/continuum/internal/histcheck"
)

// --- client helpers (through the sim net's client edge) ---------------------

type nodeResponse struct {
	ID       string            `json:"id"`
	Value    string            `json:"value"`
	Clocks   map[string]uint64 `json:"clocks"`
	Deleted  bool              `json:"deleted"`
	Siblings []struct {
		Value   string            `json:"value"`
		Clocks  map[string]uint64 `json:"clocks"`
		Deleted bool              `json:"deleted"`
	} `json:"siblings"`
}

func (c *simCluster) get(n *simNode, key string) (nodeResponse, int, error) {
	resp, err := c.client.Get("http://" + n.httpAddr + "/keys/" + key)
	if err != nil {
		return nodeResponse{}, 0, err
	}
	defer resp.Body.Close()
	var nr nodeResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
			return nodeResponse{}, resp.StatusCode, err
		}
	}
	return nr, resp.StatusCode, nil
}

// getAll reads at ?consistency=all, returning the body, the merged clock from
// X-Session-Clock, and the status.
func (c *simCluster) getAll(n *simNode, key string) (nodeResponse, map[string]uint64, int, error) {
	resp, err := c.client.Get("http://" + n.httpAddr + "/keys/" + key + "?consistency=all")
	if err != nil {
		return nodeResponse{}, nil, 0, err
	}
	defer resp.Body.Close()
	var session map[string]uint64
	if raw := resp.Header.Get("X-Session-Clock"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &session); err != nil {
			return nodeResponse{}, nil, resp.StatusCode, err
		}
	}
	var nr nodeResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
			return nodeResponse{}, nil, resp.StatusCode, err
		}
	}
	return nr, session, resp.StatusCode, nil
}

func (c *simCluster) put(n *simNode, key, value string, clocks map[string]uint64, cas bool) (int, error) {
	payload := struct {
		Value  string            `json:"value"`
		Clocks map[string]uint64 `json:"clocks,omitempty"`
	}{Value: value, Clocks: clocks}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	url := "http://" + n.httpAddr + "/keys/" + key
	if cas {
		url += "?cas=true"
	}
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// --- causal read-modify-write workload (durability + convergence) -----------

// keyState is the single-writer history for one key; sequence numbers give a
// total order per key so the durability check knows the last acknowledged
// write exactly.
type keyState struct {
	key       string
	seq       uint64
	lastAcked uint64
	clocks    map[string]uint64
}

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

// rmwWorkload drives concurrent causal read-modify-write clients: each worker
// owns a disjoint key set, GETs to pick up current clocks, PUTs with them.
type rmwWorkload struct {
	c         *simCluster
	keys      []*keyState
	perWorker int
	interval  time.Duration

	stop chan struct{}
	wg   sync.WaitGroup

	mu     sync.Mutex
	acked  int
	failed int
}

func newRMWWorkload(c *simCluster, workers, keysPerWorker int, interval time.Duration) *rmwWorkload {
	w := &rmwWorkload{c: c, perWorker: keysPerWorker, interval: interval, stop: make(chan struct{})}
	for wi := 0; wi < workers; wi++ {
		for ki := 0; ki < keysPerWorker; ki++ {
			w.keys = append(w.keys, &keyState{key: fmt.Sprintf("w%02d-k%02d", wi, ki)})
		}
	}
	return w
}

func (w *rmwWorkload) run() {
	workers := len(w.keys) / w.perWorker
	for wi := 0; wi < workers; wi++ {
		owned := w.keys[wi*w.perWorker : (wi+1)*w.perWorker]
		w.wg.Add(1)
		go w.worker(owned, rand.New(rand.NewSource(int64(wi)+1)))
	}
}

func (w *rmwWorkload) worker(owned []*keyState, rng *rand.Rand) {
	defer w.wg.Done()
	for i := 0; ; i++ {
		select {
		case <-w.stop:
			return
		default:
		}
		w.step(owned[i%len(owned)], rng)
		time.Sleep(w.interval + time.Duration(rng.Intn(int(w.interval))))
	}
}

func (w *rmwWorkload) step(k *keyState, rng *rand.Rand) {
	targets := w.c.running()
	if len(targets) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}
	n := targets[rng.Intn(len(targets))]
	if nr, code, err := w.c.get(n, k.key); err == nil && code == http.StatusOK {
		k.clocks = mergedClocks(nr)
	}
	k.seq++
	code, err := w.c.put(n, k.key, fmt.Sprintf("%s#%08d", k.key, k.seq), k.clocks, false)
	w.mu.Lock()
	if err == nil && code == http.StatusNoContent {
		k.lastAcked = k.seq
		w.acked++
	} else {
		w.failed++
	}
	w.mu.Unlock()
}

func (w *rmwWorkload) halt() (int, int) {
	close(w.stop)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acked, w.failed
}

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

// --- racing CAS workload (linearizability) -----------------------------------

// casWorkload races clients over shared keys, every mutation a conditional
// write chained off a ?consistency=all read, recording everything for the
// porcupine check. Mirrors the fault harness's CAS workload.
type casWorkload struct {
	c        *simCluster
	keys     []string
	clients  int
	interval time.Duration
	rec      *histcheck.Recorder

	stop chan struct{}
	wg   sync.WaitGroup

	mu       sync.Mutex
	acked    int
	conflict int
	unknown  int
}

func newCASWorkload(c *simCluster, clients, keys int, interval time.Duration) *casWorkload {
	w := &casWorkload{c: c, clients: clients, interval: interval, rec: histcheck.NewRecorder(), stop: make(chan struct{})}
	for i := 0; i < keys; i++ {
		w.keys = append(w.keys, fmt.Sprintf("cas-k%02d", i))
	}
	return w
}

func (w *casWorkload) run() {
	for ci := 0; ci < w.clients; ci++ {
		w.wg.Add(1)
		go w.client(ci, rand.New(rand.NewSource(int64(ci)+100)))
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
		time.Sleep(w.interval + time.Duration(rng.Intn(int(w.interval))))
	}
}

func (w *casWorkload) step(id, iter int, rng *rand.Rand) {
	targets := w.c.running()
	if len(targets) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}
	key := w.keys[rng.Intn(len(w.keys))]
	expected, ctx, ok := w.readStep(id, key, targets[rng.Intn(len(targets))])
	if !ok {
		return
	}

	value := fmt.Sprintf("c%02d#%06d", id, iter)
	call := w.rec.Now()
	code, err := w.c.put(targets[rng.Intn(len(targets))], key, value, ctx, true)
	ret := w.rec.Now()

	op := histcheck.Op{
		Client: id, Key: key, Kind: histcheck.CASPut,
		Expected: expected, Value: value,
		Status: histcheck.CASStatus(err, code),
		Call:   call, Return: ret,
	}
	w.tally(op.Status)
	w.rec.Add(op)
}

// tally increments the per-outcome counters under the workload lock.
func (w *casWorkload) tally(s histcheck.Status) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch s {
	case histcheck.StatusOK:
		w.acked++
	case histcheck.StatusConflict:
		w.conflict++
	default:
		w.unknown++
	}
}

func (w *casWorkload) readStep(id int, key string, n *simNode) (string, map[string]uint64, bool) {
	call := w.rec.Now()
	nr, session, code, err := w.c.getAll(n, key)
	ret := w.rec.Now()

	op := histcheck.Op{Client: id, Key: key, Kind: histcheck.Read, Call: call, Return: ret}
	switch {
	case err == nil && code == http.StatusOK:
		if len(nr.Siblings) > 1 {
			op.Found = true
			op.Conflict = true
			w.rec.Add(op)
			return "\x00conflict", session, true
		}
		if len(nr.Siblings) == 1 {
			op.ReadValue = nr.Siblings[0].Value
		} else {
			op.ReadValue = nr.Value
		}
		op.Found = op.ReadValue != ""
		w.rec.Add(op)
		return op.ReadValue, session, true
	case err == nil && code == http.StatusNotFound:
		w.rec.Add(op)
		return "", nil, true
	default:
		return "", nil, false
	}
}

func (w *casWorkload) halt() (int, int, int) {
	close(w.stop)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acked, w.conflict, w.unknown
}
