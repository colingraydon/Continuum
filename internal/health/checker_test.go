// internal/health/checker_test.go
package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestConfig() Config {
	return Config{
		Interval:         100 * time.Millisecond,
		FailureThreshold: 3,
		HTTPTimeout:      1 * time.Second,
	}
}

func TestNewChecker(t *testing.T) {
	// Arrange + Act
	c := NewChecker(DefaultConfig(), nil)

	// Assert
	if c == nil {
		t.Fatal("expected checker to not be nil")
	}
}

func TestAddNode(t *testing.T) {
	// Arrange
	c := NewChecker(DefaultConfig(), nil)

	// Act
	c.AddNode("node1", "10.0.0.1")

	// Assert
	status, exists := c.GetStatus("node1")
	if !exists {
		t.Fatal("expected node1 to exist")
	}
	if status != StatusHealthy {
		t.Errorf("expected healthy, got %s", status)
	}
}

func TestRemoveNode(t *testing.T) {
	// Arrange
	c := NewChecker(DefaultConfig(), nil)
	c.AddNode("node1", "10.0.0.1")

	// Act
	c.RemoveNode("node1")

	// Assert
	_, exists := c.GetStatus("node1")
	if exists {
		t.Fatal("expected node1 to not exist after removal")
	}
}

func TestGetStatusNonExistent(t *testing.T) {
	// Arrange
	c := NewChecker(DefaultConfig(), nil)

	// Act
	status, exists := c.GetStatus("nonexistent")

	// Assert
	if exists {
		t.Fatal("expected nonexistent node to not exist")
	}
	if status != StatusDead {
		t.Errorf("expected dead status for nonexistent node, got %s", status)
	}
}

func TestNodeStatusString(t *testing.T) {
	// Arrange + Act + Assert
	if StatusHealthy.String() != "healthy" {
		t.Errorf("expected healthy, got %s", StatusHealthy.String())
	}
	if StatusSuspect.String() != "suspect" {
		t.Errorf("expected suspect, got %s", StatusSuspect.String())
	}
	if StatusDead.String() != "dead" {
		t.Errorf("expected dead, got %s", StatusDead.String())
	}
}

func TestNodeStatusStringUnknown(t *testing.T) {
	// Arrange + Act
	status := NodeStatus(99)

	// Assert
	if status.String() != "unknown" {
		t.Errorf("expected unknown, got %s", status.String())
	}
}

func TestHealthyNodeStaysHealthy(t *testing.T) {
	// Arrange — spin up a real test server that returns 200
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewChecker(newTestConfig(), nil)
	c.AddNode("node1", srv.Listener.Addr().String())

	// Act
	c.checkNode("node1")

	// Assert
	status, _ := c.GetStatus("node1")
	if status != StatusHealthy {
		t.Errorf("expected healthy, got %s", status)
	}
}

func TestFailingNodeBecomesSuspect(t *testing.T) {
	// Arrange
	c := NewChecker(newTestConfig(), nil)
	c.AddNode("node1", "localhost:19999")

	// Act
	c.checkNode("node1")

	// Assert
	status, _ := c.GetStatus("node1")
	if status != StatusSuspect {
		t.Errorf("expected suspect after 1 failure, got %s", status)
	}
}

func TestFailingNodeBecomesDead(t *testing.T) {
	// Arrange
	c := NewChecker(newTestConfig(), nil)
	c.AddNode("node1", "localhost:19999")

	// Act
	c.checkNode("node1")
	c.checkNode("node1")
	c.checkNode("node1")

	// Assert
	status, _ := c.GetStatus("node1")
	if status != StatusDead {
		t.Errorf("expected dead after 3 failures, got %s", status)
	}
}

func TestRecoveryResetsToHealthy(t *testing.T) {
	// Arrange
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewChecker(newTestConfig(), nil)
	c.AddNode("node1", "localhost:19999")

	c.checkNode("node1")

	c.mu.Lock()
	c.nodes["node1"].Address = srv.Listener.Addr().String()
	c.mu.Unlock()

	// Act
	c.checkNode("node1")

	// Assert
	status, _ := c.GetStatus("node1")
	if status != StatusHealthy {
		t.Errorf("expected healthy after recovery, got %s", status)
	}
}

func TestOnStatusChangeCalledOnFailure(t *testing.T) {
	// Arrange
	var calledWith NodeStatus
	var calledID string
	c := NewChecker(newTestConfig(), func(nodeID string, status NodeStatus) {
		calledID = nodeID
		calledWith = status
	})
	c.AddNode("node1", "localhost:19999")

	// Act
	c.checkNode("node1")

	// Assert
	if calledID != "node1" {
		t.Errorf("expected callback with node1, got %s", calledID)
	}
	if calledWith != StatusSuspect {
		t.Errorf("expected suspect status in callback, got %s", calledWith)
	}
}

func TestOnStatusChangeCalledOnRecovery(t *testing.T) {
	// Arrange
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var lastStatus NodeStatus
	c := NewChecker(newTestConfig(), func(nodeID string, status NodeStatus) {
		lastStatus = status
	})
	c.AddNode("node1", "localhost:19999")
	c.checkNode("node1") // make suspect

	c.mu.Lock()
	c.nodes["node1"].Address = srv.Listener.Addr().String()
	c.mu.Unlock()

	// Act
	c.checkNode("node1")

	// Assert
	if lastStatus != StatusHealthy {
		t.Errorf("expected healthy in recovery callback, got %s", lastStatus)
	}
}

func TestOnStatusChangeNotCalledWhenStatusUnchanged(t *testing.T) {
	// Arrange
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewChecker(newTestConfig(), func(nodeID string, status NodeStatus) {
		callCount++
	})
	c.AddNode("node1", srv.Listener.Addr().String())

	// Act
	c.checkNode("node1")
	c.checkNode("node1")

	// Assert
	if callCount != 0 {
		t.Errorf("expected 0 callbacks, got %d", callCount)
	}
}

func TestStartAndStop(t *testing.T) {
	// Arrange
	c := NewChecker(newTestConfig(), nil)
	ctx := context.Background()

	// Act + Assert
	c.Start(ctx)
	c.Stop()
}

func TestCheckerRunsPeriodicChecks(t *testing.T) {
	// Arrange
	checkCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checkCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		Interval:         50 * time.Millisecond,
		FailureThreshold: 3,
		HTTPTimeout:      1 * time.Second,
	}
	c := NewChecker(cfg, nil)
	c.AddNode("node1", srv.Listener.Addr().String())

	// Act 
	ctx := context.Background()
	c.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	c.Stop()

	// Assert
	if checkCount < 2 {
		t.Errorf("expected at least 2 checks, got %d", checkCount)
	}
}

func TestNonOKResponseCountsAsFailure(t *testing.T) {
	// Arrange
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewChecker(newTestConfig(), nil)
	c.AddNode("node1", srv.Listener.Addr().String())

	// Act
	c.checkNode("node1")

	// Assert
	status, _ := c.GetStatus("node1")
	if status != StatusSuspect {
		t.Errorf("expected suspect after non-200 response, got %s", status)
	}
}

func TestDefaultConfig(t *testing.T) {
	// Arrange + Act
	cfg := DefaultConfig()

	// Assert
	if cfg.Interval != 5*time.Second {
		t.Errorf("expected 5s interval, got %s", cfg.Interval)
	}
	if cfg.FailureThreshold != 3 {
		t.Errorf("expected threshold 3, got %d", cfg.FailureThreshold)
	}
	if cfg.HTTPTimeout != 2*time.Second {
		t.Errorf("expected 2s timeout, got %s", cfg.HTTPTimeout)
	}
}