package ring

// zoneKey scopes a zone label to the data center it sits in. Zone uniqueness
// is per-DC: two racks both named "rack1" in different DCs are genuinely
// distinct failure domains, so they must not be treated as a placement
// conflict. With no DC labels anywhere this collapses to the plain zone, which
// is why the single-DC walk uses it too.
func zoneKey(n *Node) string {
	return n.DC + "\x00" + n.Zone
}

// SetDCReplication installs per-DC replica targets, promoting the data center
// to the outermost placement dimension: a key keeps factors[dc] replicas in
// each listed DC, with zone spreading applied independently inside each one.
//
// A DC absent from the table holds no replicas, so the table must name every
// DC that should carry data — including the empty DC ("") if unlabeled nodes
// are meant to. Non-positive targets are dropped for the same reason. Passing
// nil or an empty map restores cluster-wide zone-aware placement.
//
// The table is static config, identical on every node, so placement stays a
// deterministic pure function of the ring and membership.
func (r *Ring) SetDCReplication(factors map[string]int) {
	cloned := make(map[string]int, len(factors))
	for dc, n := range factors {
		if n > 0 {
			cloned[dc] = n
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(cloned) == 0 {
		r.dcFactors = nil
		return
	}
	r.dcFactors = cloned
}

// DCReplicationTotal returns the sum of the configured per-DC replica targets,
// or 0 when no per-DC table is installed. This is the cluster-wide replication
// factor implied by the table, which quorum sizing is derived from.
func (r *Ring) DCReplicationTotal() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sumFactors(r.dcFactors)
}

func sumFactors(factors map[string]int) int {
	total := 0
	for _, n := range factors {
		total += n
	}
	return total
}

// dcSelection accumulates a per-DC replica set during a ring walk. offer is
// the walkDistinct callback; fillDeferred then replays the zone-conflicting
// nodes it set aside, for any DC that never reached its target.
type dcSelection struct {
	targets  map[string]int
	taken    map[string]int  // dc -> replicas selected so far
	zones    map[string]bool // zoneKey -> represented in the set
	deferred []*Node
	result   []*Node
	total    int
}

// newDCSelection starts an empty selection against the ring's per-DC targets.
// Must be called with r.mu held.
func (r *Ring) newDCSelection() *dcSelection {
	total := sumFactors(r.dcFactors)
	return &dcSelection{
		targets: r.dcFactors,
		taken:   make(map[string]int, len(r.dcFactors)),
		zones:   make(map[string]bool, total),
		result:  make([]*Node, 0, total),
		total:   total,
	}
}

// wants reports whether n's DC is configured and still short of its target.
func (s *dcSelection) wants(n *Node) bool {
	target, ok := s.targets[n.DC]
	return ok && s.taken[n.DC] < target
}

// take adds n to the set and records its DC and zone as covered.
func (s *dcSelection) take(n *Node) {
	s.zones[zoneKey(n)] = true
	s.taken[n.DC]++
	s.result = append(s.result, n)
}

// offer considers n for the set, deferring it when its DC has already covered
// that zone. Returns false once every DC's target is met, ending the walk.
func (s *dcSelection) offer(n *Node) bool {
	if !s.wants(n) {
		return true
	}
	if n.Zone != "" && s.zones[zoneKey(n)] {
		s.deferred = append(s.deferred, n)
		return true
	}
	s.take(n)
	return len(s.result) < s.total
}

// fillDeferred hands the zone-conflicting nodes the slots their own DC could
// not fill from distinct zones, in ring order. This is the per-DC analogue of
// the single-DC walk's fallback: a DC with fewer zones than its target still
// reaches that target rather than silently under-replicating.
func (s *dcSelection) fillDeferred() {
	for _, n := range s.deferred {
		if len(s.result) == s.total {
			return
		}
		if s.wants(n) {
			s.take(n)
		}
	}
}

// walkRingDC walks the ring clockwise from hash, filling each configured DC's
// replica target independently. A DC never borrows a slot from another: when a
// DC holds fewer nodes than its target the set comes back short, because
// topping it up from across the WAN would relocate that DC's durability to the
// far side of exactly the link this feature exists to survive. Nodes in a DC
// absent from the table are skipped entirely. Must be called with r.mu held.
func (r *Ring) walkRingDC(hash uint32) []*Node {
	sel := r.newDCSelection()
	r.walkDistinct(hash, sel.offer)
	sel.fillDeferred()
	return sel.result
}

// healthyReplicasDC is the sloppy-quorum variant of walkRingDC: it returns the
// healthy members of the per-DC replica set topped up with healthy substitutes,
// plus the unhealthy intended owners whose slots those substitutes took (the
// nodes to buffer hints for). Substitutes are drawn from the skipped owner's
// own DC, preferring zones that DC's healthy set does not already cover, so a
// node outage is absorbed locally instead of quietly shifting a replica across
// the WAN. Must be called with r.mu held.
func (r *Ring) healthyReplicasDC(hash uint32) (nodes, skipped []*Node) {
	owners := r.walkRingDC(hash)

	sel := r.newDCSelection()
	taken := make(map[string]bool, len(owners))
	for _, n := range owners {
		taken[n.ID] = true
		if r.isHealthy(n) {
			sel.take(n)
		} else {
			skipped = append(skipped, n)
		}
	}
	if len(skipped) == 0 {
		return sel.result, nil
	}

	// Re-walk for substitutes. offer only accepts nodes whose DC is still
	// short of its target, so healthy DCs are left untouched.
	r.walkDistinct(hash, func(n *Node) bool {
		if taken[n.ID] || !r.isHealthy(n) {
			return true
		}
		return sel.offer(n)
	})
	sel.fillDeferred()
	return sel.result, skipped
}
