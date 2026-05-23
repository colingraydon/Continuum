package main

import (
	"testing"
	"time"
)

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
