package ring

import "math"

type NodeStats struct {
	NodeID     string  `json:"node_id"`
	Address    string  `json:"address"`
	VNodeCount int     `json:"vnode_count"`
	KeyCount   int     `json:"key_count"`
	Percentage float64 `json:"percentage"`
}

type RingStats struct {
	TotalNodes   int         `json:"total_nodes"`
	TotalVNodes  int         `json:"total_vnodes"`
	HealthyNodes int         `json:"healthy_nodes"`
	SuspectNodes int         `json:"suspect_nodes"`
	DeadNodes    int         `json:"dead_nodes"`
	Distribution []NodeStats `json:"distribution"`
	MostLoaded   string      `json:"most_loaded"`
	LeastLoaded  string      `json:"least_loaded"`
	Variance     float64     `json:"variance"`
}

func (r *Ring) GetStats() RingStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	totalVNodes := r.tree.Tree.Size()
	totalNodes := len(r.nodes)

	vnodeCounts := make(map[string]int)
	it := r.tree.Tree.Iterator()
	for it.Next() {
		vnode := it.Value().(*VNode)
		vnodeCounts[vnode.Node.ID]++
	}

	totalKeys := 0
	for _, c := range r.keyCounts {
		totalKeys += int(c.Load())
	}

	distribution := make([]NodeStats, 0, totalNodes)
	for id, node := range r.nodes {
		count := vnodeCounts[id]
		keyCount := int(r.keyCounts[id].Load())
		percentage := 0.0
		if totalKeys > 0 {
			percentage = math.Round((float64(keyCount)/float64(totalKeys))*10000) / 100
		}
		distribution = append(distribution, NodeStats{
			NodeID:     id,
			Address:    node.Address,
			VNodeCount: count,
			KeyCount: keyCount,
			Percentage: percentage,
		})
	}

	mostLoaded := ""
	leastLoaded := ""
	maxCount := 0
	minCount := math.MaxInt64

	for id, count := range vnodeCounts {
		if count > maxCount {
			maxCount = count
			mostLoaded = id
		}
		if count < minCount {
			minCount = count
			leastLoaded = id
		}
	}

	mean := 0.0
	if totalNodes > 0 {
		mean = 100.0 / float64(totalNodes)
	}
	variance := 0.0
	for _, stats := range distribution {
		diff := stats.Percentage - mean
		variance += diff * diff
	}
	if totalNodes > 0 {
		variance = math.Round((variance/float64(totalNodes))*100) / 100
	}

	return RingStats{
		TotalNodes:   totalNodes,
		TotalVNodes:  totalVNodes,
		Distribution: distribution,
		MostLoaded:   mostLoaded,
		LeastLoaded:  leastLoaded,
		Variance:     variance,
	}
}