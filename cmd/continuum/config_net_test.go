package main

import (
	"testing"
	"time"
)

func TestLoadConfig_GossipAdvertiseDefault(t *testing.T) {
	t.Setenv("SELF_ADDRESS", "10.1.2.3:8080")
	t.Setenv("GOSSIP_PORT", "9555")

	cfg := loadConfig()
	if cfg.gossipAdvertise != "10.1.2.3:9555" {
		t.Errorf("expected default advertise host:gossipPort, got %q", cfg.gossipAdvertise)
	}
}

func TestLoadConfig_GossipAdvertiseOverride(t *testing.T) {
	t.Setenv("SELF_ADDRESS", "10.1.2.3:8080")
	t.Setenv("GOSSIP_ADVERTISE_ADDR", "203.0.113.9:9999")

	cfg := loadConfig()
	if cfg.gossipAdvertise != "203.0.113.9:9999" {
		t.Errorf("expected explicit advertise addr, got %q", cfg.gossipAdvertise)
	}
}

func TestLoadConfig_HTTPBindPort(t *testing.T) {
	t.Setenv("HTTP_BIND_PORT", "18080")

	cfg := loadConfig()
	if cfg.httpBindPort != "18080" {
		t.Errorf("expected bind port override, got %q", cfg.httpBindPort)
	}
}

func TestLoadConfig_SyncInterval(t *testing.T) {
	t.Setenv("SYNC_INTERVAL_MS", "2000")

	cfg := loadConfig()
	if cfg.syncInterval != 2*time.Second {
		t.Errorf("expected 2s sync interval, got %v", cfg.syncInterval)
	}
}

func TestAdvertiseHost(t *testing.T) {
	if got := advertiseHost("10.0.0.1:8080"); got != "10.0.0.1" {
		t.Errorf("expected host split, got %q", got)
	}
	if got := advertiseHost("node1"); got != "node1" {
		t.Errorf("expected portless address passthrough, got %q", got)
	}
}
