// Package gossip provides membership and node failure detection
// with Alive, Suspect, and Dead states.
package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// NodeState represents the liveness state of a node in the gossip protocol.
type NodeState string

const (
	StateAlive   NodeState = "alive"
	StateSuspect NodeState = "suspect"
	StateDead    NodeState = "dead"
)

// Member holds information and state of a cluster member.
type Member struct {
	ID        string    `json:"id"`
	Addr      string    `json:"addr"`      // TCP address
	HTTPAddr  string    `json:"http_addr"` // HTTP address
	State     NodeState `json:"state"`
	LastSeen  time.Time `json:"last_seen"`
	SuspectAt time.Time `json:"suspect_at,omitempty"`
}

// Config holds failure detector and heartbeat tuning.
type Config struct {
	HeartbeatInterval time.Duration // Interval to send heartbeats (e.g. 500ms)
	PingTimeout       time.Duration // Timeout for HTTP ping (e.g. 300ms)
	SuspectTimeout    time.Duration // Duration in Suspect before declared Dead (e.g. 2s)
}

// OnStateChangeFunc is called when a member's state transitions.
type OnStateChangeFunc func(member Member, oldState, newState NodeState)

// Gossiper manages cluster health and failure detection.
type Gossiper struct {
	cfg           Config
	mu            sync.RWMutex
	members       map[string]*Member
	onStateChange OnStateChangeFunc
	client        *http.Client
	logger        *slog.Logger
}

// New creates a new Gossiper instance.
func New(cfg Config, onStateChange OnStateChangeFunc, logger *slog.Logger) *Gossiper {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 500 * time.Millisecond
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 300 * time.Millisecond
	}
	if cfg.SuspectTimeout <= 0 {
		cfg.SuspectTimeout = 2 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Gossiper{
		cfg:           cfg,
		members:       make(map[string]*Member),
		onStateChange: onStateChange,
		client: &http.Client{
			Timeout: cfg.PingTimeout,
		},
		logger: logger,
	}
}

// AddMember adds or updates a member in the tracker.
func (g *Gossiper) AddMember(id, addr, httpAddr string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if m, exists := g.members[id]; exists {
		m.Addr = addr
		m.HTTPAddr = httpAddr
		return
	}

	g.members[id] = &Member{
		ID:       id,
		Addr:     addr,
		HTTPAddr: httpAddr,
		State:    StateAlive,
		LastSeen: time.Now(),
	}
}

// RemoveMember explicitly drops a member.
func (g *Gossiper) RemoveMember(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.members, id)
}

// Members returns a copy of all current members.
func (g *Gossiper) Members() []Member {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]Member, 0, len(g.members))
	for _, m := range g.members {
		out = append(out, *m)
	}
	return out
}

// GetMember returns a copy of a specific member if tracked.
func (g *Gossiper) GetMember(id string) (Member, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	m, ok := g.members[id]
	if !ok {
		return Member{}, false
	}
	return *m, true
}

// Start begins periodic heartbeats and failure checks until ctx is cancelled.
func (g *Gossiper) Start(ctx context.Context) {
	ticker := time.NewTicker(g.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.checkMembers(ctx)
		}
	}
}

func (g *Gossiper) checkMembers(ctx context.Context) {
	// Snapshot member list to avoid holding lock during HTTP calls
	g.mu.RLock()
	targets := make([]Member, 0, len(g.members))
	for _, m := range g.members {
		targets = append(targets, *m)
	}
	g.mu.RUnlock()

	var wg sync.WaitGroup
	for _, m := range targets {
		if ctx.Err() != nil {
			return
		}
		wg.Add(1)
		go func(target Member) {
			defer wg.Done()
			alive := g.ping(ctx, target.HTTPAddr)
			g.updateMemberLiveness(target.ID, alive)
		}(m)
	}
	wg.Wait()
}

func (g *Gossiper) ping(ctx context.Context, httpAddr string) bool {
	url := fmt.Sprintf("http://%s/health", httpAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (g *Gossiper) updateMemberLiveness(id string, alive bool) {
	g.mu.Lock()
	m, ok := g.members[id]
	if !ok {
		g.mu.Unlock()
		return
	}

	now := time.Now()
	oldState := m.State
	newState := oldState

	if alive {
		m.LastSeen = now
		if oldState != StateAlive {
			newState = StateAlive
			m.State = StateAlive
			m.SuspectAt = time.Time{}
		}
	} else {
		switch oldState {
		case StateAlive:
			newState = StateSuspect
			m.State = StateSuspect
			m.SuspectAt = now
		case StateSuspect:
			if now.Sub(m.SuspectAt) >= g.cfg.SuspectTimeout {
				newState = StateDead
				m.State = StateDead
			}
		}
	}
	memberCopy := *m
	g.mu.Unlock()

	if newState != oldState && g.onStateChange != nil {
		g.logger.Warn("node state changed", "id", id, "from", oldState, "to", newState)
		g.onStateChange(memberCopy, oldState, newState)
	}
}

// Handler returns an HTTP handler for gossip sync or health endpoints.
func (g *Gossiper) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g.Members())
	})
}
