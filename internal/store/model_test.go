package store

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
)

// TestStoreRandomOpsAgainstModel drives the full LSM stack (WAL, memtable
// flush, SSTables, compaction, crash-reopen) with a randomized single-writer
// workload and checks every read against a trivial in-memory model. Each
// key's writes use strictly increasing single-node clocks, so the store's
// visible state for a key must always be exactly the model's last write (or
// a lone tombstone after a delete): any divergence is a correctness bug in
// merge, flush, compaction, replay, or recovery.
//
// "Crash" reopens abandon the store without Flush or checkpoint, so recovery
// runs from the manifest'd tables plus the WAL tail alone, exactly like a
// process kill.
func TestStoreRandomOpsAgainstModel(t *testing.T) {
	seeds, ops := []int64{1, 7, 42}, 1500
	if testing.Short() {
		seeds, ops = []int64{1}, 400
	}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runModelWorkload(t, seed, ops)
		})
	}
}

type modelHarness struct {
	t         *testing.T
	dir       string
	tablesDir string
	walDir    string
	s         *Store
	w         *wal.Writer

	model  map[string]string // key -> expected visible value; absent = deleted or never written
	clocks map[string]uint64 // per-key single-writer counter
	seen   map[string]bool   // every key ever touched
}

func newModelHarness(t *testing.T, dir string) *modelHarness {
	h := &modelHarness{
		t:         t,
		dir:       dir,
		tablesDir: filepath.Join(dir, "tables"),
		walDir:    filepath.Join(dir, "wal"),
		model:     make(map[string]string),
		clocks:    make(map[string]uint64),
		seen:      make(map[string]bool),
	}
	mkdir(t, h.tablesDir)
	mkdir(t, h.walDir)
	h.open()
	return h
}

func mkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
}

// open attaches a fresh Store to the on-disk state, mirroring the recovery
// flow in cmd/continuum: attach tables, replay the WAL tail, install a new
// WAL writer and the flush policy.
func (h *modelHarness) open() {
	h.t.Helper()
	s := New()
	skipBelow, err := s.OpenTables(h.tablesDir)
	if err != nil {
		h.t.Fatalf("open tables: %v", err)
	}
	r, err := wal.NewReader(h.walDir)
	if err != nil {
		h.t.Fatalf("wal reader: %v", err)
	}
	if _, err := s.Replay(r, skipBelow); err != nil && !errors.Is(err, io.EOF) {
		h.t.Fatalf("replay: %v", err)
	}
	_ = r.Close()
	w, err := wal.Open(h.walDir)
	if err != nil {
		h.t.Fatalf("wal open: %v", err)
	}
	s.SetWAL(w)
	s.SetFlushPolicy(h.tablesDir, 2048) // tiny threshold: force frequent flushes
	h.s = s
	h.w = w
}

// crashReopen simulates a process kill: no final Flush, no clean shutdown.
// The old WAL writer is closed only to release the file handle; every applied
// write was already fsynced, so closing loses nothing a crash would keep.
func (h *modelHarness) crashReopen() {
	h.t.Helper()
	_ = h.s.CloseTables()
	_ = h.w.Close()
	h.open()
}

func (h *modelHarness) put(key string) {
	h.t.Helper()
	h.clocks[key]++
	val := fmt.Sprintf("%s@%d", key, h.clocks[key])
	v := VectorClockVersion{Clocks: map[string]uint64{"w": h.clocks[key]}}
	if err := h.s.Put(key, val, v); err != nil {
		h.t.Fatalf("put %s: %v", key, err)
	}
	h.model[key] = val
	h.seen[key] = true
}

func (h *modelHarness) del(key string) {
	h.t.Helper()
	h.clocks[key]++
	v := VectorClockVersion{Clocks: map[string]uint64{"w": h.clocks[key]}}
	if err := h.s.Delete(key, v); err != nil {
		h.t.Fatalf("delete %s: %v", key, err)
	}
	delete(h.model, key)
	h.seen[key] = true
}

// verify checks one key's visible state against the model. A deleted key may
// be entirely absent or visible as a lone tombstone; both read as deleted.
func (h *modelHarness) verify(key, when string) {
	h.t.Helper()
	e, ok, err := h.s.Get(key)
	if err != nil {
		h.t.Fatalf("%s: get %s: %v", when, key, err)
	}
	want, exists := h.model[key]
	if exists {
		if !ok || len(e.Siblings) != 1 || e.Siblings[0].Deleted || e.Siblings[0].Value != want {
			h.t.Fatalf("%s: key %s: want value %q, got ok=%t entry=%+v", when, key, want, ok, e)
		}
		return
	}
	if ok && (len(e.Siblings) != 1 || !e.Siblings[0].Deleted) {
		h.t.Fatalf("%s: key %s: want deleted/absent, got entry=%+v", when, key, e)
	}
}

func (h *modelHarness) verifyAll(when string) {
	h.t.Helper()
	for key := range h.seen {
		h.verify(key, when)
	}
}

func runModelWorkload(t *testing.T, seed int64, ops int) {
	rng := rand.New(rand.NewSource(seed))
	h := newModelHarness(t, t.TempDir())

	key := func() string { return fmt.Sprintf("k%03d", rng.Intn(60)) }

	for i := 0; i < ops; i++ {
		switch p := rng.Intn(100); {
		case p < 60:
			h.put(key())
		case p < 75:
			h.del(key())
		case p < 83:
			if err := h.s.Flush(); err != nil {
				t.Fatalf("op %d: flush: %v", i, err)
			}
		case p < 91:
			if _, err := h.s.Compact(time.Hour); err != nil {
				t.Fatalf("op %d: compact: %v", i, err)
			}
		default:
			h.crashReopen()
			h.verifyAll(fmt.Sprintf("op %d: after crash-reopen", i))
		}
		// Spot-check a random touched key every op; full sweeps happen on
		// reopen and at the end.
		h.verify(key(), fmt.Sprintf("op %d", i))
	}

	h.crashReopen()
	h.verifyAll("final recovery")
	t.Logf("seed %d: %d ops, %d keys, %d tables at end", seed, ops, len(h.seen), h.s.TableCount())
}
