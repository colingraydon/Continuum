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
// majority, so CAS stays available, and the full history — across the kill,
// the downtime-gate wipe, and the repair — must linearize per key.
//
// This scenario has earned its keep twice. Under the primary-serialized
// design it reproduced finding #7 (churn moved the primary role without
// moving the state). After paxos closed that, it surfaced finding #10: the
// SIGKILL restart trips the downtime gate, so the node rejoined with an
// empty store while still voting in CAS quorums — and with commits acked at
// majority, a committed value could be down to one store copy, letting a
// serial-read majority of {wiped node, the replica the commit never
// reached} honestly merge to stale state. Wiped nodes now rejoin as
// bootstrapping (out of read sets and paxos quorums) until they have pulled
// their replica ranges back, so this asserts linearizability again.
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
	verifyLinearizable(t, w, linCheckTimeout)
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
