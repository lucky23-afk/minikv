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

func TestGossiper_FlappingAndPartition(t *testing.T) {
	var mu sync.Mutex
	status := http.StatusOK

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		st := status
		mu.Unlock()
		w.WriteHeader(st)
	}))
	defer srv.Close()

	var transitions []NodeState
	var transMu sync.Mutex

	cfg := Config{
		HeartbeatInterval: 15 * time.Millisecond,
		PingTimeout:       10 * time.Millisecond,
		SuspectTimeout:    40 * time.Millisecond,
	}

	g := New(cfg, func(m Member, oldState, newState NodeState) {
		transMu.Lock()
		transitions = append(transitions, newState)
		transMu.Unlock()
	}, nil)

	g.AddMember("flap-node", "127.0.0.1:8001", srv.Listener.Addr().String())

	// Test AddMember update
	g.AddMember("flap-node", "127.0.0.1:8002", srv.Listener.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.Start(ctx)

	// Flap 1: Simulate partition (HTTP 503) -> Suspect
	mu.Lock()
	status = http.StatusServiceUnavailable
	mu.Unlock()
	time.Sleep(25 * time.Millisecond)

	// Flap 2: Recover quickly -> Alive before Dead
	mu.Lock()
	status = http.StatusOK
	mu.Unlock()
	time.Sleep(30 * time.Millisecond)

	// Flap 3: Complete partition -> Suspect -> Dead
	mu.Lock()
	status = http.StatusBadGateway
	mu.Unlock()
	time.Sleep(100 * time.Millisecond)

	m, ok := g.GetMember("flap-node")
	if !ok || m.State != StateDead {
		t.Fatalf("expected node to be dead after partition, got: %+v", m)
	}

	// Flap 4: Rejoining after being dead -> Alive
	mu.Lock()
	status = http.StatusOK
	mu.Unlock()
	time.Sleep(40 * time.Millisecond)

	m, _ = g.GetMember("flap-node")
	if m.State != StateAlive {
		t.Fatalf("expected node to rejoin as alive, got: %+v", m)
	}

	// Test Members list
	members := g.Members()
	if len(members) != 1 || members[0].ID != "flap-node" {
		t.Fatalf("unexpected members: %+v", members)
	}

	// Test RemoveMember
	g.RemoveMember("flap-node")
	if len(g.Members()) != 0 {
		t.Fatal("expected 0 members after remove")
	}

	_, found := g.GetMember("nonexistent")
	if found {
		t.Fatal("expected nonexistent member to not be found")
	}
}

func TestGossiper_Handler(t *testing.T) {
	g := New(Config{}, nil, nil)
	g.AddMember("node-1", "127.0.0.1:7000", "127.0.0.1:7001")

	req := httptest.NewRequest(http.MethodGet, "/gossip", nil)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from gossip handler, got %d", rec.Code)
	}
}
