// Package replication handles master -> replica log propagation,
// catch-up, and replica promotion.
package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lucky23-afk/minikv/internal/node"
)

// Role defines whether a node acts as Master or Replica for its shard.
type Role string

const (
	RoleMaster  Role = "master"
	RoleReplica Role = "replica"
)

// PeerStatus tracks replication progress of a replica.
type PeerStatus struct {
	NodeID      string    `json:"node_id"`
	Role        Role      `json:"role"`
	AppliedSeq  uint64    `json:"applied_seq"`
	LastAckTime time.Time `json:"last_ack_time"`
	Lag         uint64    `json:"lag"`
}

// Manager coordinates write replication and failover promotion.
type Manager struct {
	mu           sync.RWMutex
	nodeID       string
	role         Role
	peers        map[string]*node.TCPClient
	peerStatus   map[string]*PeerStatus
	lastSeq      atomic.Uint64
	logger       *slog.Logger
	replAcksCond *sync.Cond
}

// NewManager creates a replication manager for a node.
func NewManager(nodeID string, role Role, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		nodeID:     nodeID,
		role:       role,
		peers:      make(map[string]*node.TCPClient),
		peerStatus: make(map[string]*PeerStatus),
		logger:     logger,
	}
	m.replAcksCond = sync.NewCond(&m.mu)
	return m
}

// SetRole updates the node role (e.g. on promotion).
func (m *Manager) SetRole(role Role) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.role = role
	m.logger.Info("replication role changed", "nodeID", m.nodeID, "role", role)
}

// Role returns the current role.
func (m *Manager) Role() Role {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.role
}

// AddPeer adds or updates a replica connection.
func (m *Manager) AddPeer(nodeID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, ok := m.peers[nodeID]; ok {
		client.Close()
	}
	m.peers[nodeID] = node.NewTCPClient(addr)
	m.peerStatus[nodeID] = &PeerStatus{
		NodeID:      nodeID,
		Role:        RoleReplica,
		AppliedSeq:  0,
		LastAckTime: time.Now(),
		Lag:         0,
	}
}

// RemovePeer removes a peer from replication tracking.
func (m *Manager) RemovePeer(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if client, ok := m.peers[nodeID]; ok {
		client.Close()
		delete(m.peers, nodeID)
	}
	delete(m.peerStatus, nodeID)
	m.replAcksCond.Broadcast()
}

// UpdateLocalSeq updates the highest written sequence on master.
func (m *Manager) UpdateLocalSeq(seq uint64) {
	m.lastSeq.Store(seq)
	m.mu.Lock()
	for _, st := range m.peerStatus {
		if seq >= st.AppliedSeq {
			st.Lag = seq - st.AppliedSeq
		}
	}
	m.mu.Unlock()
}

// ReplicateWrite sends PUT or DEL to all active replicas.
// If requiredAcks > 0, it blocks until at least requiredAcks replicas confirm or timeout occurs.
func (m *Manager) ReplicateWrite(ctx context.Context, cmd string, key string, ttlMs int64, val []byte, seq uint64, requiredAcks int) error {
	m.UpdateLocalSeq(seq)

	m.mu.RLock()
	replicaPeers := make(map[string]*node.TCPClient, len(m.peers))
	for id, c := range m.peers {
		replicaPeers[id] = c
	}
	m.mu.RUnlock()

	var ackCount atomic.Int32

	for id, client := range replicaPeers {
		go func(targetID string, cli *node.TCPClient) {
			var err error
			var line string
			if cmd == "PUT" {
				line, _, err = cli.SendPUT(key, ttlMs, val)
			} else {
				line, _, err = cli.SendLine(fmt.Sprintf("DEL %s", key))
			}

			if err == nil && (line == "+OK" || line == ":1" || line == ":0") {
				m.recordAck(targetID, seq)
				ackCount.Add(1)
				m.replAcksCond.Broadcast()
			} else {
				m.logger.Debug("replication write failed to replica", "targetID", targetID, "err", err)
			}
		}(id, client)
	}

	if requiredAcks <= 0 {
		return nil
	}

	// Wait for synchronous acks with timeout
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Second)
	}

	done := make(chan struct{})
	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for int(ackCount.Load()) < requiredAcks {
			if time.Now().After(deadline) || ctx.Err() != nil {
				return
			}
			m.replAcksCond.Wait()
		}
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(time.Until(deadline)):
		return fmt.Errorf("replication timeout waiting for %d acks (got %d)", requiredAcks, ackCount.Load())
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) recordAck(nodeID string, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.peerStatus[nodeID]; ok {
		if seq > st.AppliedSeq {
			st.AppliedSeq = seq
		}
		st.LastAckTime = time.Now()
		masterSeq := m.lastSeq.Load()
		if masterSeq >= st.AppliedSeq {
			st.Lag = masterSeq - st.AppliedSeq
		}
	}
}

// MaxLagBeforeSnapshot is the maximum number of log records a replica can fall behind
// before a snapshot transfer is required.
const MaxLagBeforeSnapshot uint64 = 1000

// InboundLogApplier tracks applied sequence numbers on a replica and enforces monotonicity.
type InboundLogApplier struct {
	mu         sync.Mutex
	appliedSeq uint64
}

// NewInboundLogApplier creates an applier starting at seq.
func NewInboundLogApplier(startSeq uint64) *InboundLogApplier {
	return &InboundLogApplier{appliedSeq: startSeq}
}

// ApplyResult represents the outcome of validating an inbound replication record.
type ApplyResult string

const (
	ApplyOK        ApplyResult = "applied"
	ApplyDuplicate ApplyResult = "duplicate"
	ApplyGap       ApplyResult = "gap_detected"
)

// ValidateAndApply checks sequence order:
// - seq <= appliedSeq: duplicate, ignored
// - seq == appliedSeq + 1: ok, increment appliedSeq
// - seq > appliedSeq + 1: gap detected, replica must catch up
func (a *InboundLogApplier) ValidateAndApply(seq uint64) ApplyResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	if seq <= a.appliedSeq {
		return ApplyDuplicate
	}
	if seq == a.appliedSeq+1 {
		a.appliedSeq = seq
		return ApplyOK
	}
	return ApplyGap
}

// AppliedSeq returns current sequence.
func (a *InboundLogApplier) AppliedSeq() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.appliedSeq
}

// CheckCatchUpMode evaluates if a lagging replica can catch up from WAL logs
// or if the lag exceeds MaxLagBeforeSnapshot requiring a full snapshot transfer.
func (m *Manager) CheckCatchUpMode(replicaAppliedSeq, masterSeq uint64) (needsSnapshot bool, lag uint64) {
	if masterSeq <= replicaAppliedSeq {
		return false, 0
	}
	lag = masterSeq - replicaAppliedSeq
	if lag > MaxLagBeforeSnapshot {
		return true, lag
	}
	return false, lag
}
func (m *Manager) Statuses() []PeerStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]PeerStatus, 0, len(m.peerStatus))
	for _, s := range m.peerStatus {
		out = append(out, *s)
	}
	return out
}

// Promote selects the replica peer with the highest appliedSeq.
func (m *Manager) SelectPromotionCandidate() (string, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var bestNode string
	var highestSeq uint64
	found := false

	for id, st := range m.peerStatus {
		if !found || st.AppliedSeq > highestSeq {
			bestNode = id
			highestSeq = st.AppliedSeq
			found = true
		}
	}

	if !found {
		return "", 0, fmt.Errorf("no replicas available for promotion")
	}
	return bestNode, highestSeq, nil
}
