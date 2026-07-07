//go:build fault

package fault

import (
	"testing"
	"time"
)

// linCheckTimeout bounds the porcupine search. Generous because proving a
// churn history linearizable explores many interleavings of unknown-outcome
// ops, and the suite may be sharing the machine with the cluster processes.
const linCheckTimeout = 180 * time.Second

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
// restarted. While a replica is down the remaining two of three still form a
// majority, so CAS stays available. Under the primary-serialized design this
// scenario reproduced finding #7; paxos closed that, and the checker then
// surfaced finding #10, which this scenario now detects: the SIGKILL restart
// trips the downtime gate, so the node rejoins with an empty store while
// still voting in CAS quorums. Commits ack at majority, so a committed value
// can survive on a single replica after the wipe — and a serial-read
// majority of {wiped node, the replica the commit never reached} honestly
// merges to stale state. The fix (a wiped node rejoins as bootstrapping and
// stays out of CAS quorums until repaired) is on the roadmap; until then
// this runs the checker in detector mode.
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
	expectKnownCASGap(t, w, linCheckTimeout,
		"finding #10: a downtime-gate wipe leaves committed CAS values under-replicated until anti-entropy repairs (docs/fault-injection.md)")
}

// Scenario 13: concurrent CAS writers across an asymmetric partition. The
// isolated node still believes it is primary for its ranges (its outbound
// traffic flows, so it never suspects itself), but coordinators cannot reach
// it: forwarded CAS requests fail closed with 503 until the survivors'
// member lists mark it dead and the ring walk moves on. After heal, the
// rejoining node must not serve CAS from its stale state in a way that loses
// an acknowledged write. Like scenario 12 this reproduced finding #7 before
// paxos-backed CAS; it asserts linearizability now — the isolated node
// cannot assemble a majority, so its rounds fail closed instead of forking.
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
	verifyLinearizable(t, w, linCheckTimeout)
}
