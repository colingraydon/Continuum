//go:build fault

package fault

import (
	"testing"
	"time"
)

const linCheckTimeout = 60 * time.Second

// Scenario 11: concurrent CAS writers on a healthy cluster. Conditional
// writes serialize through each key's primary replica, so a history of
// racing read-then-CAS clients must be linearizable per key: for every
// generation of a value, exactly one of the writers racing from it wins and
// the rest get 412. This is the formal upgrade of "CAS gives lock-like
// semantics" from a claim to a checked property.
func TestFault_LinearizableCASHealthy(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	w := newCASWorkload(c, 4, 3, nil)
	w.run()

	time.Sleep(10 * time.Second)

	acked, conflict, unknown := w.halt()
	t.Logf("cas workload: %d acked, %d conflict, %d unknown", acked, conflict, unknown)
	if acked == 0 {
		t.Fatal("workload never acknowledged a CAS write; harness is broken")
	}
	verifyLinearizable(t, w, linCheckTimeout)
}

// Scenario 12: concurrent CAS writers while a node is killed and later
// restarted. While a key's primary is down, CAS for that key fails closed
// with 503 (recorded as unknown-outcome ops); once gossip converges the ring
// walk names a new primary and CAS resumes there. The check asserts that the
// full history — across the kill, the failover, and the restart — still
// linearizes per key.
//
// This scenario reliably reproduces finding #7 (docs/fault-injection.md):
// diverging ring views during churn can name two primaries and fork a CAS
// history, and a new primary may also serve CAS from state that misses a
// write it never received (delivered later by hints or anti-entropy). The
// checker runs in detector mode — it reports the violation with a porcupine
// visualization instead of failing — until consensus-backed CAS closes the
// window, at which point this becomes a hard assertion (verifyLinearizable).
func TestFault_LinearizableCASAcrossPrimaryFailover(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	w := newCASWorkload(c, 4, 3, nil)
	w.run()

	time.Sleep(3 * time.Second)
	victim := c.nodes[1]
	t.Logf("killing %s under CAS load", victim.id)
	c.kill(victim)

	time.Sleep(6 * time.Second) // long enough for suspect -> dead -> new primary

	t.Logf("restarting %s", victim.id)
	c.restart(victim)
	time.Sleep(4 * time.Second) // old primary reclaims its ranges

	acked, conflict, unknown := w.halt()
	t.Logf("cas workload: %d acked, %d conflict, %d unknown", acked, conflict, unknown)
	if acked == 0 {
		t.Fatal("workload never acknowledged a CAS write; harness is broken")
	}
	expectKnownCASGap(t, w, linCheckTimeout)
}

// Scenario 13: concurrent CAS writers across an asymmetric partition. The
// isolated node still believes it is primary for its ranges (its outbound
// traffic flows, so it never suspects itself), but coordinators cannot reach
// it: forwarded CAS requests fail closed with 503 until the survivors'
// member lists mark it dead and the ring walk moves on. After heal, the
// rejoining node must not serve CAS from its stale state in a way that loses
// an acknowledged write. Like scenario 12 this runs the checker in detector
// mode for finding #7 until consensus-backed CAS closes the churn window.
func TestFault_LinearizableCASAcrossPartition(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	w := newCASWorkload(c, 4, 3, nil)
	w.run()

	time.Sleep(3 * time.Second)
	victim := c.nodes[2]
	t.Logf("isolating %s under CAS load", victim.id)
	c.isolate(victim)

	time.Sleep(6 * time.Second)

	t.Logf("healing %s", victim.id)
	c.heal(victim)
	time.Sleep(4 * time.Second)

	acked, conflict, unknown := w.halt()
	t.Logf("cas workload: %d acked, %d conflict, %d unknown", acked, conflict, unknown)
	if acked == 0 {
		t.Fatal("workload never acknowledged a CAS write; harness is broken")
	}
	expectKnownCASGap(t, w, linCheckTimeout)
}
