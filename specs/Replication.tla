---------------------------- MODULE Replication ----------------------------
(***************************************************************************)
(* A model of Continuum's replication and repair path for a single key:    *)
(* quorum writes, crash and recovery, hinted handoff, anti-entropy,        *)
(* tombstones, tombstone GC, and the downtime gate.                        *)
(*                                                                         *)
(* The fault and simulation harnesses sample executions of this path -     *)
(* they run the real system and observe whatever the schedule happens to   *)
(* produce. TLC instead enumerates every interleaving of the model within  *)
(* the configured bounds, which is how it can make a claim about states no *)
(* test happened to reach.                                                 *)
(*                                                                         *)
(* Two properties are checked:                                             *)
(*                                                                         *)
(*   NoResurrection - once a delete's tombstone has been garbage           *)
(*     collected, no serving node ever again holds the value that delete   *)
(*     superseded. This is the safety argument in docs/persistence.md,     *)
(*     which the downtime gate exists to enforce.                          *)
(*                                                                         *)
(*   Durability - the newest acknowledged write survives, so long as no    *)
(*     more than Cardinality(Nodes) - WriteQuorum replicas have lost their *)
(*     data. "Lost" counts both permanent failure and a downtime-gate      *)
(*     wipe; that the second kind has to be counted is something the model *)
(*     checker established rather than something assumed - see the "What   *)
(*     the gate costs" finding in docs/tla-spec.md. Durability is FALSE    *)
(*     when ClampQuorum is TRUE, and that is not a bug either: see the     *)
(*     comment on ClampQuorum below.                                       *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,        \* the replica set for the single key being modelled
    Values,       \* the client values that may be written
    WriteQuorum,  \* W: acknowledgements required before replying to a client
    MaxOps,       \* bound on client operations, to keep the model finite
    MaxFailures,  \* bound on permanent node loss
    ClampQuorum   \* see below

(***************************************************************************)
(* ClampQuorum models fault-harness finding #4: the write quorum clamps to *)
(* the live replica set, so a cluster that has lost most of its replicas   *)
(* acknowledges writes with fewer copies than W rather than refusing them. *)
(* That is a deliberate availability-over-durability trade, pinned by      *)
(* QuorumLossThenClampedAvailability in the fault suite.                   *)
(*                                                                         *)
(* Setting it TRUE therefore makes Durability genuinely violable, and TLC  *)
(* will produce the trace. That trace is the point: it turns a prose       *)
(* caveat into a mechanically-checked statement of exactly what the clamp  *)
(* costs. Continuum.cfg checks the honest configuration (clamp on,         *)
(* NoResurrection only); Strict.cfg turns the clamp off and additionally   *)
(* checks Durability.                                                      *)
(***************************************************************************)

ASSUME WriteQuorum \in 1..Cardinality(Nodes)
ASSUME MaxFailures \in 0..Cardinality(Nodes)
ASSUME ClampQuorum \in BOOLEAN

Tombstone == "tombstone"

(* An absent key. Version 0 sorts below every real write. *)
NoEntry == [ver |-> 0, val |-> "absent"]

Entries == [ver : 1..MaxOps, val : Values \cup {Tombstone}]

VARIABLES
    store,    \* [Nodes -> entry] each node's local copy
    up,       \* SUBSET Nodes: reachable, serving
    failed,   \* SUBSET Nodes: permanently lost, never returning
    hints,    \* [Nodes -> SUBSET entry] writes buffered for an unreachable target
    acked,    \* SUBSET entry: writes acknowledged to a client
    nextVer,  \* next version to mint
    ops,      \* client operations so far
    gcVer,    \* version of the delete whose tombstone was collected (0 = none)
    stale,    \* SUBSET Nodes: were down across a GC, so must wipe before serving
    wiped     \* SUBSET Nodes: the downtime gate has destroyed their data

vars == <<store, up, failed, hints, acked, nextVer, ops, gcVer, stale, wiped>>

TypeOK ==
    /\ store \in [Nodes -> Entries \cup {NoEntry}]
    /\ up \subseteq Nodes
    /\ failed \subseteq Nodes
    /\ hints \in [Nodes -> SUBSET Entries]
    /\ acked \subseteq Entries
    /\ nextVer \in 1..(MaxOps + 1)
    /\ ops \in 0..MaxOps
    /\ gcVer \in 0..MaxOps
    /\ stale \subseteq Nodes
    /\ wiped \subseteq Nodes

Init ==
    /\ store = [n \in Nodes |-> NoEntry]
    /\ up = Nodes
    /\ failed = {}
    /\ hints = [n \in Nodes |-> {}]
    /\ acked = {}
    /\ nextVer = 1
    /\ ops = 0
    /\ gcVer = 0
    /\ stale = {}
    /\ wiped = {}

(* Reconciliation is last-writer-wins on version, matching the store's
   merge: a strictly newer version replaces what is held. This abstracts
   vector clocks into a total order - see the "Abstractions" section of
   docs/tla-spec.md for why that is sound for these two properties and
   what it deliberately gives up. *)
Merge(current, incoming) ==
    IF incoming.ver > current.ver THEN incoming ELSE current

(* How many acknowledgements this write actually needs. With the clamp, a
   cluster with fewer live nodes than W settles for what it has. *)
Required ==
    IF ClampQuorum
    THEN IF Cardinality(up) < WriteQuorum THEN Cardinality(up) ELSE WriteQuorum
    ELSE WriteQuorum

(* A client write or delete. The coordinator fans out to every reachable
   replica, acknowledges once Required of them hold it, and buffers a hint
   for each replica that is merely unreachable (not permanently failed). *)
Apply(v) ==
    /\ ops < MaxOps
    /\ Cardinality(up) >= Required
    /\ Required > 0
    /\ LET e == [ver |-> nextVer, val |-> v] IN
        /\ store' = [n \in Nodes |-> IF n \in up THEN Merge(store[n], e) ELSE store[n]]
        /\ hints' = [n \in Nodes |->
                        IF n \in (Nodes \ up) \ failed THEN hints[n] \cup {e} ELSE hints[n]]
        /\ acked' = acked \cup {e}
    /\ nextVer' = nextVer + 1
    /\ ops' = ops + 1
    /\ UNCHANGED <<up, failed, gcVer, stale, wiped>>

Write == \E v \in Values : Apply(v)

Delete == Apply(Tombstone)

(* A node becomes unreachable but keeps its data - a crash or a partition.
   Its store persists, which is exactly what makes resurrection possible. *)
Crash(n) ==
    /\ n \in up
    /\ up' = up \ {n}
    /\ UNCHANGED <<store, failed, hints, acked, nextVer, ops, gcVer, stale, wiped>>

(* A node returns. The downtime gate: a node that was down across a GC pass
   cannot trust its local data, so it discards it and re-bootstraps rather
   than serving writes the cluster has already collected. Removing this
   single conjunct is what makes NoResurrection fail - see
   docs/tla-spec.md. *)
Recover(n) ==
    /\ n \notin up
    /\ n \notin failed
    /\ up' = up \cup {n}
    /\ IF n \in stale
       THEN /\ store' = [store EXCEPT ![n] = NoEntry]
            /\ hints' = [hints EXCEPT ![n] = {}]
            /\ stale' = stale \ {n}
            /\ wiped' = wiped \cup {n}
       ELSE UNCHANGED <<store, hints, stale, wiped>>
    /\ UNCHANGED <<failed, acked, nextVer, ops, gcVer>>

(* Permanent loss: the node is gone and its data with it. *)
Fail(n) ==
    /\ n \in up
    /\ Cardinality(failed) < MaxFailures
    /\ failed' = failed \cup {n}
    /\ up' = up \ {n}
    /\ store' = [store EXCEPT ![n] = NoEntry]
    /\ hints' = [hints EXCEPT ![n] = {}]
    /\ UNCHANGED <<acked, nextVer, ops, gcVer, stale, wiped>>

(* Hinted handoff: a buffered write is replayed to its target once the
   target is reachable again. *)
DeliverHint(n) ==
    /\ n \in up
    /\ hints[n] # {}
    /\ \E e \in hints[n] :
        /\ store' = [store EXCEPT ![n] = Merge(store[n], e)]
        /\ hints' = [hints EXCEPT ![n] = hints[n] \ {e}]
    /\ UNCHANGED <<up, failed, acked, nextVer, ops, gcVer, stale, wiped>>

(* Anti-entropy: two reachable replicas reconcile, newest version winning. *)
AntiEntropy(a, b) ==
    /\ a \in up /\ b \in up /\ a # b
    /\ store' = [store EXCEPT ![b] = Merge(store[b], store[a])]
    /\ UNCHANGED <<up, failed, hints, acked, nextVer, ops, gcVer, stale, wiped>>

(* Tombstone GC has two guards, and the model checker says both are load
   bearing. Delete either and NoResurrection fails; see the "Why both
   guards" section of docs/tla-spec.md for the traces.

   Guard 1 - reachable replicas have converged. A tombstone is only
   collected once every reachable node holds it or something newer.
   This is what the TTL buys: at a 24h GC TTL against a 30s sync interval,
   docs/antientropy.md counts ~2,880 anti-entropy cycles of headroom for
   the tombstone to reach every live replica. The model has no clock, so
   the consequence of that headroom is stated directly as a precondition
   rather than being derived from the interval arithmetic.

   Guard 2 - the downtime gate. Every node that is unreachable right now
   is marked stale, because it may hold the superseded value and has just
   missed its last chance to learn about the delete. It must discard its
   data before serving again.

   Hints at or below the collected version are dropped, which in the
   implementation falls out of the hint TTL (1h) being far shorter than
   the GC TTL (24h). Modelling it keeps a stale hint from carrying a
   superseded value across the GC boundary. *)
Converged(d) == \A m \in up : store[m].ver >= d \/ store[m] = NoEntry

CollectTombstone ==
    /\ \E n \in up :
        /\ store[n].val = Tombstone
        /\ store[n].ver > gcVer
        /\ Converged(store[n].ver)
        /\ LET d == store[n].ver IN
            /\ gcVer' = d
            /\ store' = [m \in Nodes |->
                            IF m \in up /\ store[m].val = Tombstone /\ store[m].ver <= d
                            THEN NoEntry ELSE store[m]]
            /\ hints' = [m \in Nodes |-> {e \in hints[m] : e.ver > d}]
    /\ stale' = stale \cup ((Nodes \ up) \ failed)
    /\ UNCHANGED <<up, failed, acked, nextVer, ops, wiped>>

Next ==
    \/ Write
    \/ Delete
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : Recover(n)
    \/ \E n \in Nodes : Fail(n)
    \/ \E n \in Nodes : DeliverHint(n)
    \/ \E a, b \in Nodes : AntiEntropy(a, b)
    \/ CollectTombstone

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties                                                              *)

(* Once a delete has been collected, no serving node holds a live value it
   superseded. A node that is down may still carry one on disk - that is
   unavoidable and harmless, because the downtime gate makes it discard the
   data before it can serve or spread it. *)
NoResurrection ==
    \A n \in up :
        ~(store[n].val \in Values /\ store[n].ver < gcVer)

(* The newest acknowledged write is still held somewhere that has not been
   permanently lost. Deletes are excluded: a collected tombstone is
   supposed to vanish. *)
LatestAck ==
    CHOOSE e \in acked : \A f \in acked : f.ver <= e.ver

(* Replicas whose data is gone: permanently failed, or destroyed by the
   downtime gate. The model checker's contribution here was showing that the
   second kind must be counted. A gate wipe is not a mere availability blip -
   it consumes the same durability budget a permanent failure does, and a
   quorum-acknowledged write can be lost to one gate wipe plus one permanent
   failure. See the "What the gate costs" finding in docs/tla-spec.md. *)
Lost == failed \cup wiped

Tolerance == Cardinality(Nodes) - WriteQuorum

Durability ==
    \/ acked = {}
    \/ Cardinality(Lost) > Tolerance   \* past the tolerated loss, nothing is promised
    \/ LET e == LatestAck IN
        \/ e.val = Tombstone
        \/ e.ver <= gcVer
        \/ \E n \in Nodes \ failed : store[n].ver >= e.ver

=============================================================================
