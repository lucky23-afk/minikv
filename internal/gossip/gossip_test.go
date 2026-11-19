package gossip

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestGossiper_StateTransitions(t *testing.T) {
	var mu sync.Mutex
	isUp := true

	// Mock server representing a storage node
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		up := isUp
		mu.Unlock()

		if up {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	var stateHistory []NodeState
	var historyMu sync.Mutex

	cfg := Config{
		HeartbeatInterval: 20 * time.Millisecond,
		PingTimeout:       15 * time.Millisecond,
		SuspectTimeout:    60 * time.Millisecond,
	}

	g := New(cfg, func(m Member, oldState, newState NodeState) {
		historyMu.Lock()
		defer historyMu.Unlock()
		stateHistory = append(stateHistory, newState)
	}, nil)

	// srv.Listener.Addr().String() gives host:port
	g.AddMember("node-test", "127.0.0.1:9999", srv.Listener.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.Start(ctx)

	// Let it run a bit alive
	time.Sleep(50 * time.Millisecond)

	m, ok := g.GetMember("node-test")
	if !ok || m.State != StateAlive {
		t.Fatalf("expected node to be alive, got ok=%v, state=%v", ok, m.State)
	}

	// Simulate failure
	mu.Lock()
	isUp = false
	mu.Unlock()

	// Wait for Suspect then Dead
	time.Sleep(150 * time.Millisecond)

	m, _ = g.GetMember("node-test")
	if m.State != StateDead {
		t.Fatalf("expected node to transition to dead, got %v", m.State)
	}

	// Verify transitions
	historyMu.Lock()
	defer historyMu.Unlock()
	if len(stateHistory) < 2 {
		t.Fatalf("expected at least 2 state transitions (suspect, dead), got: %v", stateHistory)
	}
	if stateHistory[0] != StateSuspect || stateHistory[1] != StateDead {
		t.Fatalf("unexpected state history: %v", stateHistory)
	}

	// Recover node
	mu.Lock()
	isUp = true
	mu.Unlock()

	time.Sleep(60 * time.Millisecond)
	m, _ = g.GetMember("node-test")
	if m.State != StateAlive {
		t.Fatalf("expected node to recover to alive, got %v", m.State)
	}
}
