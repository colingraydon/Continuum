//go:build sim

package sim

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// Node logs (gossip transitions, sync failures) are production log.Printf
	// calls; a multi-node in-process cluster makes them a firehose. SIM_LOG=1
	// keeps them for debugging a specific seed.
	if os.Getenv("SIM_LOG") == "" {
		log.SetOutput(io.Discard)
	}
	os.Exit(m.Run())
}

const (
	workloadInterval = 4 * time.Millisecond
	faultWindow      = 2500 * time.Millisecond
	linCheckTimeout  = 30 * time.Second
)

// event is one scheduled fault action, resolved against the cluster at
// execution time (a restart replaces node instances, so closures must not
// capture them at generation time).
type event struct {
	at   time.Duration
	desc string
	do   func()
}

// schedule generates the run's fault plan from the seed: a sequence of
// non-overlapping fault episodes (partition, isolation, drop burst, delay
// burst, or crash+restart), each healed before the next begins. At most one
// crash per run: the store is memory-only, so a crash is total state loss for
// that node, and two overlapping losses could legitimately destroy an
// acknowledged W=2 write — that would test the schedule, not the system.
func schedule(rng *rand.Rand, c *simCluster, window time.Duration) ([]event, bool) {
	var events []event
	crashUsed := false
	t := 200 * time.Millisecond
	for t < window {
		dur := time.Duration(300+rng.Intn(500)) * time.Millisecond
		kind := rng.Intn(5)
		if kind == 4 && crashUsed {
			kind = rng.Intn(4)
		}
		switch kind {
		case 0: // symmetric partition: one node vs the rest
			id := c.nodes[rng.Intn(len(c.nodes))].id
			var rest []string
			for _, n := range c.nodes {
				if n.id != id {
					rest = append(rest, n.id)
				}
			}
			events = append(events,
				event{t, fmt.Sprintf("partition %s | %v", id, rest), func() { c.net.partition([]string{id}, rest) }},
				event{t + dur, "heal partition", func() { c.net.healAll() }},
			)
		case 1: // asymmetric partition: inbound blackhole
			id := c.nodes[rng.Intn(len(c.nodes))].id
			events = append(events,
				event{t, "isolate " + id, func() { c.net.isolate(id) }},
				event{t + dur, "heal isolation", func() { c.net.healAll() }},
			)
		case 2: // message drop burst
			p := 0.2 + rng.Float64()*0.4
			events = append(events,
				event{t, fmt.Sprintf("drop %.0f%% of messages", p*100), func() { c.net.setDropProb(p) }},
				event{t + dur, "stop dropping", func() { c.net.setDropProb(0) }},
			)
		case 3: // latency burst
			d := time.Duration(20+rng.Intn(50)) * time.Millisecond
			events = append(events,
				event{t, fmt.Sprintf("add %v latency", d), func() { c.net.setDelay(d) }},
				event{t + dur, "remove latency", func() { c.net.setDelay(0) }},
			)
		case 4: // crash + restart (empty store, same identity)
			crashUsed = true
			idx := rng.Intn(len(c.nodes))
			events = append(events,
				event{t, "crash " + c.nodes[idx].id, func() { c.crash(c.node(idx)) }},
				event{t + dur, "restart " + c.nodes[idx].id, func() { c.restart(c.node(idx)) }},
			)
		}
		t += dur + time.Duration(200+rng.Intn(300))*time.Millisecond
	}
	return events, crashUsed
}

// runSim executes one seeded simulation: start the cluster, run the RMW and
// CAS workloads, fire the fault schedule, heal, and verify. Returns the
// workloads for the caller's checks plus whether the schedule crashed a
// node (total state loss, including its paxos promises — see the CAS check
// in TestSimSeededFaults).
func runSim(t *testing.T, seed int64, faults bool) (*simCluster, *rmwWorkload, *casWorkload, bool) {
	t.Helper()
	c := newSimCluster(t, simConfig{}, seed)
	rmw := newRMWWorkload(c, 4, 3, workloadInterval)
	cas := newCASWorkload(c, 3, 3, 2*workloadInterval)
	rmw.run()
	cas.run()

	crashed := false
	if faults {
		rng := rand.New(rand.NewSource(seed*31 + 7))
		var events []event
		events, crashed = schedule(rng, c, faultWindow)
		for _, ev := range events {
			time.Sleep(time.Until(cas.rec.At(ev.at)))
			t.Logf("t=%v %s", ev.at, ev.desc)
			ev.do()
		}
		time.Sleep(time.Until(cas.rec.At(faultWindow)))
		c.net.healAll()
		for _, n := range c.nodes {
			if !n.running.Load() {
				t.Logf("restarting crashed %s after window", n.id)
				c.restart(n)
			}
		}
		c.waitFullRing(10 * time.Second)
	} else {
		time.Sleep(faultWindow)
	}

	ackedRMW, failedRMW := rmw.halt()
	ackedCAS, conflict, unknown := cas.halt()
	t.Logf("rmw: %d acked, %d failed; cas: %d acked, %d conflict, %d unknown",
		ackedRMW, failedRMW, ackedCAS, conflict, unknown)
	if ackedRMW == 0 || ackedCAS == 0 {
		t.Fatal("workloads never acknowledged a write; harness is broken")
	}
	return c, rmw, cas, crashed
}

func rmwKeyNames(w *rmwWorkload) []string {
	names := make([]string, len(w.keys))
	for i, k := range w.keys {
		names[i] = k.key
	}
	return names
}

// seeds returns the seed list for a run: SIM_SEED pins one seed for replay,
// SIM_SEEDS sets how many sequential seeds to sweep (default 3).
func seeds(t *testing.T, base int64) []int64 {
	if v := os.Getenv("SIM_SEED"); v != "" {
		s, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("invalid SIM_SEED %q: %v", v, err)
		}
		return []int64{s}
	}
	n := 3
	if v := os.Getenv("SIM_SEEDS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 1 {
			t.Fatalf("invalid SIM_SEEDS %q", v)
		}
		n = parsed
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = base + int64(i)
	}
	return out
}

// TestSimHealthy: no faults. Everything the system promises must hold, and
// the CAS history must linearize — a hard assertion, same as the fault
// harness's healthy scenario.
func TestSimHealthy(t *testing.T) {
	for _, seed := range seeds(t, 1) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c, rmw, cas, _ := runSim(t, seed, false)
			verifyLinearizable(t, cas, linCheckTimeout)
			verifyDurability(t, c, rmw, 3*time.Second)
			verifyConvergence(t, c, rmwKeyNames(rmw), 5*time.Second)
		})
	}
}

// TestSimSeededFaults: the seed generates a fault schedule (partitions,
// isolation, drops, latency, one crash+restart) fired under load. After
// heal: no acknowledged write may be lost, replicas must converge, and CAS
// histories must linearize — all hard assertions, except when the schedule
// crashed a node. Simulated nodes are memory-only, so a crash erases the
// acceptor's paxos promises along with everything else, and a forgotten
// promise legitimately breaks the majority-intersection argument (in
// production the acceptor log in DATA_DIR survives crashes; the fault
// harness asserts that case). Crash schedules therefore run the CAS check
// in detector mode.
func TestSimSeededFaults(t *testing.T) {
	for _, seed := range seeds(t, 42) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c, rmw, cas, crashed := runSim(t, seed, true)
			verifyDurability(t, c, rmw, 6*time.Second)
			verifyConvergence(t, c, rmwKeyNames(rmw), 8*time.Second)
			if crashed {
				expectKnownCASGap(t, cas, linCheckTimeout)
			} else {
				verifyLinearizable(t, cas, linCheckTimeout)
			}
		})
	}
}
