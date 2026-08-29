package main

import (
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
)

func TestLoadConfig_SelfDC(t *testing.T) {
	t.Setenv("SELF_DC", "us-west")

	cfg := loadConfig()

	if cfg.selfDC != "us-west" {
		t.Errorf("expected selfDC=us-west, got %q", cfg.selfDC)
	}
}

func TestApplySelfMetadata_StampsLabels(t *testing.T) {
	ml := gossip.NewMemberList("n1", "10.0.0.1:8080", nil)

	applySelfMetadata(ml, config{
		selfWeight:      2.0,
		selfZone:        "rack1",
		selfDC:          "us-east",
		gossipAdvertise: "10.0.0.1:9000",
	})

	self, ok := ml.Get("n1")
	if !ok {
		t.Fatal("self member missing")
	}
	if self.DC != "us-east" {
		t.Errorf("dc = %q, want us-east", self.DC)
	}
	if self.Zone != "rack1" {
		t.Errorf("zone = %q, want rack1", self.Zone)
	}
	if self.Weight != 2.0 {
		t.Errorf("weight = %v, want 2.0", self.Weight)
	}
	if self.GossipAddr != "10.0.0.1:9000" {
		t.Errorf("gossipAddr = %q, want 10.0.0.1:9000", self.GossipAddr)
	}
}

func TestApplySelfMetadata_SkipsEmptyLabels(t *testing.T) {
	// An unlabeled node must not carry a stray zone/DC after configuration.
	ml := gossip.NewMemberList("n1", "10.0.0.1:8080", nil)

	applySelfMetadata(ml, config{selfWeight: 1.0, gossipAdvertise: "10.0.0.1:9000"})

	self, _ := ml.Get("n1")
	if self.DC != "" {
		t.Errorf("expected empty dc, got %q", self.DC)
	}
	if self.Zone != "" {
		t.Errorf("expected empty zone, got %q", self.Zone)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	// Clear all env vars that loadConfig reads by setting them via t.Setenv
	// with empty strings, then relying on the unset-after-test cleanup.
	for _, key := range []string{
		"REPLICAS", "REPLICATION_FACTOR", "WRITE_QUORUM", "READ_QUORUM",
		"SELF_ADDRESS", "SELF_ID", "GOSSIP_PORT", "SEED_NODES",
		"REPLICA_TIMEOUT_MS", "SELF_WEIGHT",
	} {
		t.Setenv(key, "")
	}

	cfg := loadConfig()

	if cfg.replicas != 150 {
		t.Errorf("expected replicas=150, got %d", cfg.replicas)
	}
	if cfg.replicationFactor != 3 {
		t.Errorf("expected replicationFactor=3, got %d", cfg.replicationFactor)
	}
	if cfg.writeQuorum != 2 {
		t.Errorf("expected writeQuorum=2, got %d", cfg.writeQuorum)
	}
	if cfg.readQuorum != 2 {
		t.Errorf("expected readQuorum=2, got %d", cfg.readQuorum)
	}
	if cfg.selfAddress != "localhost:8080" {
		t.Errorf("expected selfAddress=localhost:8080, got %s", cfg.selfAddress)
	}
	if cfg.selfID != "localhost:8080" {
		t.Errorf("expected selfID=localhost:8080, got %s", cfg.selfID)
	}
	if cfg.gossipPort != "8081" {
		t.Errorf("expected gossipPort=8081, got %s", cfg.gossipPort)
	}
	if len(cfg.seedNodes) != 0 {
		t.Errorf("expected no seed nodes, got %v", cfg.seedNodes)
	}
	if cfg.replicaTimeout != 500*time.Millisecond {
		t.Errorf("expected replicaTimeout=500ms, got %v", cfg.replicaTimeout)
	}
	if cfg.selfWeight != 1.0 {
		t.Errorf("expected selfWeight=1.0, got %f", cfg.selfWeight)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("REPLICAS", "50")
	t.Setenv("REPLICATION_FACTOR", "1")
	t.Setenv("WRITE_QUORUM", "1")
	t.Setenv("READ_QUORUM", "1")
	t.Setenv("SELF_ADDRESS", "10.0.0.1:9090")
	t.Setenv("SELF_ID", "my-node")
	t.Setenv("GOSSIP_PORT", "9091")
	t.Setenv("SEED_NODES", "10.0.0.2:9090,10.0.0.3:9090")
	t.Setenv("REPLICA_TIMEOUT_MS", "250")
	t.Setenv("SELF_WEIGHT", "2.5")

	cfg := loadConfig()

	if cfg.replicas != 50 {
		t.Errorf("expected replicas=50, got %d", cfg.replicas)
	}
	if cfg.replicationFactor != 1 {
		t.Errorf("expected replicationFactor=1, got %d", cfg.replicationFactor)
	}
	if cfg.writeQuorum != 1 {
		t.Errorf("expected writeQuorum=1, got %d", cfg.writeQuorum)
	}
	if cfg.readQuorum != 1 {
		t.Errorf("expected readQuorum=1, got %d", cfg.readQuorum)
	}
	if cfg.selfAddress != "10.0.0.1:9090" {
		t.Errorf("expected selfAddress=10.0.0.1:9090, got %s", cfg.selfAddress)
	}
	if cfg.selfID != "my-node" {
		t.Errorf("expected selfID=my-node, got %s", cfg.selfID)
	}
	if cfg.gossipPort != "9091" {
		t.Errorf("expected gossipPort=9091, got %s", cfg.gossipPort)
	}
	if len(cfg.seedNodes) != 2 {
		t.Errorf("expected 2 seed nodes, got %v", cfg.seedNodes)
	}
	if cfg.replicaTimeout != 250*time.Millisecond {
		t.Errorf("expected replicaTimeout=250ms, got %v", cfg.replicaTimeout)
	}
	if cfg.selfWeight != 2.5 {
		t.Errorf("expected selfWeight=2.5, got %f", cfg.selfWeight)
	}
}

func TestLoadConfig_InvalidEnvIgnored(t *testing.T) {
	// Invalid values for numeric fields should fall back to defaults.
	t.Setenv("REPLICAS", "notanumber")
	t.Setenv("REPLICATION_FACTOR", "notanumber")
	t.Setenv("REPLICA_TIMEOUT_MS", "notanumber")
	t.Setenv("SELF_WEIGHT", "notanumber")
	// Clear quorum vars so defaults are used.
	t.Setenv("WRITE_QUORUM", "")
	t.Setenv("READ_QUORUM", "")

	cfg := loadConfig()

	if cfg.replicas != 150 {
		t.Errorf("invalid REPLICAS should default to 150, got %d", cfg.replicas)
	}
	if cfg.replicationFactor != 3 {
		t.Errorf("invalid REPLICATION_FACTOR should default to 3, got %d", cfg.replicationFactor)
	}
	if cfg.replicaTimeout != 500*time.Millisecond {
		t.Errorf("invalid REPLICA_TIMEOUT_MS should default to 500ms, got %v", cfg.replicaTimeout)
	}
	if cfg.selfWeight != 1.0 {
		t.Errorf("invalid SELF_WEIGHT should default to 1.0, got %f", cfg.selfWeight)
	}
}

func TestLoadConfig_QuorumInvalidZero(t *testing.T) {
	// WRITE_QUORUM and READ_QUORUM <= 0 should be ignored (must be > 0).
	t.Setenv("REPLICATION_FACTOR", "3")
	t.Setenv("WRITE_QUORUM", "0")
	t.Setenv("READ_QUORUM", "-1")
	t.Setenv("REPLICAS", "")

	cfg := loadConfig()

	// Default quorum for RF=3 is floor(3/2)+1 = 2.
	if cfg.writeQuorum != 2 {
		t.Errorf("WRITE_QUORUM=0 should fall back to default 2, got %d", cfg.writeQuorum)
	}
	if cfg.readQuorum != 2 {
		t.Errorf("READ_QUORUM=-1 should fall back to default 2, got %d", cfg.readQuorum)
	}
}

func TestParseDCReplication_Valid(t *testing.T) {
	// Arrange + Act: surrounding whitespace is tolerated so a wrapped or
	// prettified env value still parses.
	got, err := parseDCReplication(" us-east:3 , eu-west:2 ")

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["us-east"] != 3 || got["eu-west"] != 2 || len(got) != 2 {
		t.Errorf("parsed %v, want us-east:3 eu-west:2", got)
	}
}

func TestParseDCReplication_EmptyIsNil(t *testing.T) {
	// An unset (or blank) value leaves the cluster on the single cluster-wide
	// REPLICATION_FACTOR rather than installing an empty table.
	for _, raw := range []string{"", "   "} {
		got, err := parseDCReplication(raw)
		if err != nil {
			t.Fatalf("parseDCReplication(%q): unexpected error: %v", raw, err)
		}
		if got != nil {
			t.Errorf("parseDCReplication(%q) = %v, want nil", raw, got)
		}
	}
}

func TestParseDCReplication_Malformed(t *testing.T) {
	// Malformed input is an error rather than a silent skip: quietly dropping an
	// entry would under-replicate the DC the operator asked to protect.
	cases := map[string]string{
		"missing colon":    "us-east",
		"empty dc name":    ":3,eu-west:2",
		"non-numeric":      "us-east:three",
		"zero count":       "us-east:0",
		"negative count":   "us-east:-1",
		"duplicate dc":     "us-east:3,us-east:2",
		"trailing garbage": "us-east:3,",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDCReplication(raw); err == nil {
				t.Errorf("parseDCReplication(%q) succeeded, want error", raw)
			}
		})
	}
}

func TestValidateDCReplication(t *testing.T) {
	factors := map[string]int{"us-east": 3, "eu-west": 3}

	// No table: SELF_DC is irrelevant, single-DC placement applies.
	if err := validateDCReplication(nil, ""); err != nil {
		t.Errorf("empty table should validate, got %v", err)
	}
	// Listed DC: fine.
	if err := validateDCReplication(factors, "us-east"); err != nil {
		t.Errorf("listed SELF_DC should validate, got %v", err)
	}
	// Unset SELF_DC with a table: this node would hold nothing.
	if err := validateDCReplication(factors, ""); err == nil {
		t.Error("empty SELF_DC with a table should fail validation")
	}
	// SELF_DC absent from the table: same problem, named explicitly.
	if err := validateDCReplication(factors, "ap-south"); err == nil {
		t.Error("unlisted SELF_DC should fail validation")
	}
}

func TestLoadConfig_DCReplicationDrivesReplicationFactor(t *testing.T) {
	// The per-DC table replaces REPLICATION_FACTOR rather than sitting beside
	// it, so quorum sizing keeps working off a single cluster-wide total.
	t.Setenv("SELF_DC", "us-east")
	t.Setenv("REPLICATION_FACTOR_BY_DC", "us-east:3,eu-west:3")
	t.Setenv("REPLICATION_FACTOR", "2") // overridden by the table
	t.Setenv("WRITE_QUORUM", "")
	t.Setenv("READ_QUORUM", "")

	cfg := loadConfig()

	if cfg.replicationFactor != 6 {
		t.Errorf("replicationFactor = %d, want 6 (sum of the table)", cfg.replicationFactor)
	}
	if len(cfg.dcReplication) != 2 || cfg.dcReplication["us-east"] != 3 {
		t.Errorf("dcReplication = %v, want us-east:3 eu-west:3", cfg.dcReplication)
	}
	// Default quorum tracks the new total: floor(6/2)+1 = 4.
	if cfg.writeQuorum != 4 || cfg.readQuorum != 4 {
		t.Errorf("quorums = w%d/r%d, want 4/4 for RF 6", cfg.writeQuorum, cfg.readQuorum)
	}
}

func TestLoadConfig_NoDCReplicationLeavesFactorAlone(t *testing.T) {
	// Without the table nothing changes: dcReplication stays nil and
	// REPLICATION_FACTOR still drives placement.
	t.Setenv("REPLICATION_FACTOR_BY_DC", "")
	t.Setenv("REPLICATION_FACTOR", "3")
	t.Setenv("WRITE_QUORUM", "")
	t.Setenv("READ_QUORUM", "")

	cfg := loadConfig()

	if cfg.dcReplication != nil {
		t.Errorf("dcReplication = %v, want nil", cfg.dcReplication)
	}
	if cfg.replicationFactor != 3 {
		t.Errorf("replicationFactor = %d, want 3", cfg.replicationFactor)
	}
}
