package benchmarks

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/colingraydon/continuum/internal/gossip"
)

// gossipMessage builds a push-pull message carrying n members - the payload
// every gossip tick sends to each of its fanout peers.
func gossipMessage(n int) *gossip.GossipMessage {
	members := make([]*gossip.Member, n)
	for i := range members {
		members[i] = &gossip.Member{
			ID:          fmt.Sprintf("node-%03d", i),
			Address:     fmt.Sprintf("10.0.0.%d:8080", i%250),
			GossipAddr:  fmt.Sprintf("10.0.0.%d:8081", i%250),
			Incarnation: uint64(i),
			Heartbeat:   uint64(i * 100),
			Status:      gossip.MemberAlive,
		}
	}
	return &gossip.GossipMessage{Type: gossip.MessagePushPull, From: "node-000", Members: members}
}

func benchmarkGossipMarshal(b *testing.B, members int) {
	msg := gossipMessage(members)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(msg); err != nil {
			b.Fatalf("marshal: %v", err)
		}
	}
}

func benchmarkGossipUnmarshal(b *testing.B, members int) {
	data, err := json.Marshal(gossipMessage(members))
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var msg gossip.GossipMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			b.Fatalf("unmarshal: %v", err)
		}
	}
}

func BenchmarkGossipMarshal10Members(b *testing.B)    { benchmarkGossipMarshal(b, 10) }
func BenchmarkGossipMarshal100Members(b *testing.B)   { benchmarkGossipMarshal(b, 100) }
func BenchmarkGossipUnmarshal10Members(b *testing.B)  { benchmarkGossipUnmarshal(b, 10) }
func BenchmarkGossipUnmarshal100Members(b *testing.B) { benchmarkGossipUnmarshal(b, 100) }

// BenchmarkMemberListMerge100: merging a 100-member view into a local list -
// the receive-side work of one gossip exchange.
func BenchmarkMemberListMerge100(b *testing.B) {
	incoming := gossipMessage(100).Members
	ml := gossip.NewMemberList("node-000", "10.0.0.0:8080", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ml.Merge(incoming)
	}
}
