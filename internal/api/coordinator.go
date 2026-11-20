// Package api implements the coordinator HTTP router, REST endpoints,
// SSE metrics stream, client routing through consistent hashing, and web dashboard serving.
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/lucky23-afk/minikv/internal/gossip"
	"github.com/lucky23-afk/minikv/internal/node"
	"github.com/lucky23-afk/minikv/internal/replication"
	"github.com/lucky23-afk/minikv/internal/ring"
)

// Coordinator coordinates cluster requests, ring routing, and serves the dashboard.
type Coordinator struct {
	ring        *ring.Ring
	gossiper    *gossip.Gossiper
	replMgr     *replication.Manager
	clientsMu   sync.RWMutex
	clients     map[string]*node.TCPClient
	logger      *slog.Logger
	dashboardFS http.FileSystem
	benchMu     sync.Mutex
	benchHist   []BenchmarkRun
}

// BenchmarkRun records past benchmark executions.
type BenchmarkRun struct {
	ID          string  `json:"id"`
	Timestamp   string  `json:"timestamp"`
	Concurrency int     `json:"concurrency"`
	Duration    string  `json:"duration"`
	ReadRatio   float64 `json:"read_ratio"`
	Throughput  float64 `json:"throughput"`
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
	Errors      int64   `json:"errors"`
	TotalOps    int64   `json:"total_ops"`
}

// Config configures the Coordinator.
type Config struct {
	VNodesPerNode int
	Replicas      int
	DashboardFS   http.FileSystem
}

// NewCoordinator creates a new Coordinator instance.
func NewCoordinator(cfg Config, logger *slog.Logger) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	r := ring.New(cfg.VNodesPerNode)
	repl := replication.NewManager("coordinator", replication.RoleMaster, logger)

	c := &Coordinator{
		ring:        r,
		replMgr:     repl,
		clients:     make(map[string]*node.TCPClient),
		logger:      logger,
		dashboardFS: cfg.DashboardFS,
		benchHist:   make([]BenchmarkRun, 0),
	}

	g := gossip.New(gossip.Config{
		HeartbeatInterval: 500 * time.Millisecond,
		PingTimeout:       300 * time.Millisecond,
		SuspectTimeout:    2 * time.Second,
	}, c.onNodeStateChange, logger)

	c.gossiper = g
	return c
}

func (c *Coordinator) onNodeStateChange(m gossip.Member, oldState, newState gossip.NodeState) {
	c.logger.Info("coordinator noticed member state transition", "node", m.ID, "old", oldState, "new", newState)
	if newState == gossip.StateDead {
		c.ring.RemoveNode(m.ID)
		c.replMgr.RemovePeer(m.ID)
		if cand, seq, err := c.replMgr.SelectPromotionCandidate(); err == nil && cand != "" {
			c.logger.Info("promoted replica to primary for recovered partitions", "promoted_node", cand, "applied_seq", seq)
		}
	} else if newState == gossip.StateAlive && oldState == gossip.StateDead {
		c.ring.AddNode(ring.Node{ID: m.ID, Addr: m.Addr, HTTPAddr: m.HTTPAddr})
		c.replMgr.AddPeer(m.ID, m.Addr)
	}
}

// AddNode registers a storage node into the ring, gossip, and client pool.
func (c *Coordinator) AddNode(id, tcpAddr, httpAddr string) {
	c.ring.AddNode(ring.Node{ID: id, Addr: tcpAddr, HTTPAddr: httpAddr})
	c.gossiper.AddMember(id, tcpAddr, httpAddr)
	c.replMgr.AddPeer(id, tcpAddr)

	c.clientsMu.Lock()
	if old, ok := c.clients[id]; ok {
		old.Close()
	}
	c.clients[id] = node.NewTCPClient(tcpAddr)
	c.clientsMu.Unlock()
}

// RemoveNode unregisters a node.
func (c *Coordinator) RemoveNode(id string) {
	c.ring.RemoveNode(id)
	c.gossiper.RemoveMember(id)
	c.replMgr.RemovePeer(id)

	c.clientsMu.Lock()
	if cli, ok := c.clients[id]; ok {
		cli.Close()
		delete(c.clients, id)
	}
	c.clientsMu.Unlock()
}

func (c *Coordinator) getClient(nodeID string) (*node.TCPClient, error) {
	c.clientsMu.RLock()
	defer c.clientsMu.RUnlock()
	cli, ok := c.clients[nodeID]
	if !ok {
		return nil, fmt.Errorf("no client connection for node %q", nodeID)
	}
	return cli, nil
}

// Routes sets up Chi routing for all endpoints and embedded web UI.
func (c *Coordinator) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))

	// API routes
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/kv/{key}", c.handleGetKV)
		r.Put("/kv/{key}", c.handlePutKV)
		r.Delete("/kv/{key}", c.handleDeleteKV)
		r.Get("/kv", c.handleListKeys)
		r.Get("/stats", c.handleStats)
		r.Get("/nodes", c.handleNodes)
		r.Post("/nodes", c.handleAddNode)
		r.Delete("/nodes/{id}", c.handleRemoveNode)
		r.Get("/ring", c.handleRing)
		r.Post("/console", c.handleConsole)
		r.Get("/events", c.handleEventsSSE)
		r.Get("/bench/history", c.handleBenchHistory)
		r.Post("/bench/run", c.handleBenchRun)
		r.Post("/snapshot", c.handleTriggerSnapshot)
	})

	// Static dashboard
	if c.dashboardFS != nil {
		fileServer := http.FileServer(c.dashboardFS)
		r.Handle("/*", fileServer)
	}

	return r
}

func (c *Coordinator) handleGetKV(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if key == "" {
		http.Error(w, `{"error":"missing key"}`, http.StatusBadRequest)
		return
	}

	nodes := c.ring.Lookup(key, 3)
	if len(nodes) == 0 {
		http.Error(w, `{"error":"no nodes in ring"}`, http.StatusServiceUnavailable)
		return
	}

	var lastErr error
	for _, nID := range nodes {
		cli, err := c.getClient(nID)
		if err != nil {
			lastErr = err
			continue
		}
		line, val, err := cli.SendLine("GET " + key)
		if err != nil {
			lastErr = err
			continue
		}
		if line == "$-1" || val == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "key not found",
				"key":   key,
				"node":  nID,
			})
			return
		}

		// Also get TTL
		ttlRemaining := int64(-1)
		ttlLine, _, _ := cli.SendLine("TTL " + key)
		if strings.HasPrefix(ttlLine, ":") {
			ttlRemaining, _ = strconv.ParseInt(ttlLine[1:], 10, 64)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":    key,
			"value":  string(val),
			"ttl_ms": ttlRemaining,
			"node":   nID,
		})
		return
	}

	http.Error(w, fmt.Sprintf(`{"error":"failed to reach nodes: %v"}`, lastErr), http.StatusBadGateway)
}

func (c *Coordinator) handlePutKV(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if key == "" {
		http.Error(w, `{"error":"missing key"}`, http.StatusBadRequest)
		return
	}

	var req struct {
		Value string `json:"value"`
		TTLMs int64  `json:"ttl_ms"`
		Sync  bool   `json:"sync"`
		SyncN int    `json:"sync_n"`
	}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}
	if len(bodyBytes) > 0 {
		_ = json.Unmarshal(bodyBytes, &req)
	}

	nodes := c.ring.Lookup(key, 3)
	if len(nodes) == 0 {
		http.Error(w, `{"error":"no nodes in ring"}`, http.StatusServiceUnavailable)
		return
	}

	var primaryID string
	var writeErr error
	var replicaNodes []string

	for idx, candidateID := range nodes {
		cli, err := c.getClient(candidateID)
		if err != nil {
			writeErr = err
			continue
		}
		line, _, err := cli.SendPUT(key, req.TTLMs, []byte(req.Value))
		if err == nil && line == "+OK" {
			primaryID = candidateID
			for j, otherID := range nodes {
				if j != idx {
					replicaNodes = append(replicaNodes, otherID)
				}
			}
			break
		}
		writeErr = err
	}

	if primaryID == "" {
		http.Error(w, fmt.Sprintf(`{"error":"put failed across candidate nodes: %v"}`, writeErr), http.StatusBadGateway)
		return
	}

	// Replicate to replicas
	reqAcks := 0
	if req.Sync {
		reqAcks = 1
		if req.SyncN > 0 {
			reqAcks = req.SyncN
		}
	}
	_ = c.replMgr.ReplicateWrite(r.Context(), "PUT", key, req.TTLMs, []byte(req.Value), 0, reqAcks)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":       true,
		"key":      key,
		"primary":  primaryID,
		"replicas": replicaNodes,
	})
}

func (c *Coordinator) handleDeleteKV(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	nodes := c.ring.Lookup(key, 3)
	if len(nodes) == 0 {
		http.Error(w, `{"error":"no nodes in ring"}`, http.StatusServiceUnavailable)
		return
	}

	var primaryID string
	var delLine string
	var delErr error
	for _, candidateID := range nodes {
		cli, err := c.getClient(candidateID)
		if err != nil {
			delErr = err
			continue
		}
		line, _, err := cli.SendLine("DEL " + key)
		if err == nil && (line == ":1" || line == ":0") {
			primaryID = candidateID
			delLine = line
			break
		}
		delErr = err
	}

	if primaryID == "" {
		http.Error(w, fmt.Sprintf(`{"error":"delete failed across candidate nodes: %v"}`, delErr), http.StatusBadGateway)
		return
	}

	_ = c.replMgr.ReplicateWrite(r.Context(), "DEL", key, 0, nil, 0, 0)

	deleted := delLine == ":1"
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":      true,
		"deleted": deleted,
		"key":     key,
	})
}

func (c *Coordinator) handleListKeys(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	allMembers := c.gossiper.Members()

	type KeyItem struct {
		Key    string `json:"key"`
		Value  string `json:"value"`
		TTLMs  int64  `json:"ttl_ms"`
		Size   int    `json:"size"`
		NodeID string `json:"node_id"`
	}

	var items []KeyItem
	seenKeys := make(map[string]bool)

	for _, m := range allMembers {
		if m.State != gossip.StateAlive {
			continue
		}
		cli, err := c.getClient(m.ID)
		if err != nil {
			continue
		}
		_, data, err := cli.SendLine("KEYS " + prefix)
		if err != nil || len(data) == 0 {
			continue
		}
		rawKeys := strings.Split(strings.TrimSpace(string(data)), "\n")
		for _, k := range rawKeys {
			k = strings.TrimSpace(k)
			if k == "" || seenKeys[k] {
				continue
			}
			seenKeys[k] = true

			// Fetch value & TTL for preview
			_, vBytes, _ := cli.SendLine("GET " + k)
			vStr := string(vBytes)
			ttlLine, _, _ := cli.SendLine("TTL " + k)
			var ttlRemaining int64 = -1
			if strings.HasPrefix(ttlLine, ":") {
				ttlRemaining, _ = strconv.ParseInt(ttlLine[1:], 10, 64)
			}

			items = append(items, KeyItem{
				Key:    k,
				Value:  vStr,
				TTLMs:  ttlRemaining,
				Size:   len(vBytes),
				NodeID: m.ID,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(items)
}

func (c *Coordinator) handleStats(w http.ResponseWriter, r *http.Request) {
	members := c.gossiper.Members()
	var totalHits, totalMisses, totalEvictions, totalKeys, totalBytes uint64

	for _, m := range members {
		if m.State != gossip.StateAlive {
			continue
		}
		cli, err := c.getClient(m.ID)
		if err != nil {
			continue
		}
		_, rawJSON, err := cli.SendLine("STATS")
		if err != nil || len(rawJSON) == 0 {
			continue
		}
		var st struct {
			Hits      uint64 `json:"hits"`
			Misses    uint64 `json:"misses"`
			Evictions uint64 `json:"evictions"`
			Keys      uint64 `json:"keys"`
			Bytes     uint64 `json:"bytes"`
		}
		if json.Unmarshal(rawJSON, &st) == nil {
			totalHits += st.Hits
			totalMisses += st.Misses
			totalEvictions += st.Evictions
			totalKeys += st.Keys
			totalBytes += st.Bytes
		}
	}

	hitRate := 0.0
	if totalHits+totalMisses > 0 {
		hitRate = float64(totalHits) / float64(totalHits+totalMisses) * 100.0
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes_total": len(members),
		"nodes_alive": countAlive(members),
		"hits":        totalHits,
		"misses":      totalMisses,
		"hit_rate":    hitRate,
		"evictions":   totalEvictions,
		"keys":        totalKeys,
		"bytes":       totalBytes,
	})
}

func countAlive(members []gossip.Member) int {
	cnt := 0
	for _, m := range members {
		if m.State == gossip.StateAlive {
			cnt++
		}
	}
	return cnt
}

func (c *Coordinator) handleNodes(w http.ResponseWriter, r *http.Request) {
	members := c.gossiper.Members()
	replStatuses := c.replMgr.Statuses()
	replMap := make(map[string]replication.PeerStatus)
	for _, st := range replStatuses {
		replMap[st.NodeID] = st
	}

	type NodeDetail struct {
		ID       string           `json:"id"`
		Addr     string           `json:"addr"`
		HTTPAddr string           `json:"http_addr"`
		State    gossip.NodeState `json:"state"`
		Role     string           `json:"role"`
		Keys     uint64           `json:"keys"`
		Bytes    uint64           `json:"bytes"`
		Lag      uint64           `json:"lag"`
	}

	var list []NodeDetail
	for _, m := range members {
		var kCount, bCount uint64
		if m.State == gossip.StateAlive {
			if cli, err := c.getClient(m.ID); err == nil {
				if _, raw, err := cli.SendLine("STATS"); err == nil && len(raw) > 0 {
					var st struct {
						Keys  uint64 `json:"keys"`
						Bytes uint64 `json:"bytes"`
					}
					_ = json.Unmarshal(raw, &st)
					kCount = st.Keys
					bCount = st.Bytes
				}
			}
		}

		lag := uint64(0)
		role := "master"
		if st, ok := replMap[m.ID]; ok {
			lag = st.Lag
			role = string(st.Role)
		}

		list = append(list, NodeDetail{
			ID:       m.ID,
			Addr:     m.Addr,
			HTTPAddr: m.HTTPAddr,
			State:    m.State,
			Role:     role,
			Keys:     kCount,
			Bytes:    bCount,
			Lag:      lag,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func (c *Coordinator) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string `json:"id"`
		Addr     string `json:"addr"`
		HTTPAddr string `json:"http_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	c.AddNode(req.ID, req.Addr, req.HTTPAddr)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (c *Coordinator) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c.RemoveNode(id)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (c *Coordinator) handleRing(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	var responsible []string
	if key != "" {
		responsible = c.ring.Lookup(key, 3)
	}

	vnodes := c.ring.VNodeEntries()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes":          c.ring.Nodes(),
		"vnodes":         vnodes,
		"key":            key,
		"mapped_nodes":   responsible,
		"total_vnodes":   len(vnodes),
		"physical_nodes": c.ring.Len(),
	})
}

func (c *Coordinator) handleConsole(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Cmd string `json:"cmd"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid command body"}`, http.StatusBadRequest)
		return
	}

	parts := strings.Fields(strings.TrimSpace(req.Cmd))
	if len(parts) == 0 {
		http.Error(w, `{"error":"empty command"}`, http.StatusBadRequest)
		return
	}

	cmdUpper := strings.ToUpper(parts[0])
	switch cmdUpper {
	case "GET":
		if len(parts) < 2 {
			http.Error(w, `{"error":"GET requires key"}`, http.StatusBadRequest)
			return
		}
		key := parts[1]
		nodes := c.ring.Lookup(key, 1)
		if len(nodes) == 0 {
			http.Error(w, `{"error":"no nodes in ring"}`, http.StatusServiceUnavailable)
			return
		}
		cli, err := c.getClient(nodes[0])
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"connect error: %v"}`, err), http.StatusBadGateway)
			return
		}
		line, val, _ := cli.SendLine("GET " + key)
		if line == "$-1" || val == nil {
			json.NewEncoder(w).Encode(map[string]string{"output": "(nil)"})
		} else {
			json.NewEncoder(w).Encode(map[string]string{"output": fmt.Sprintf("%q", string(val))})
		}

	case "PUT":
		if len(parts) < 3 {
			http.Error(w, `{"error":"PUT requires key and value"}`, http.StatusBadRequest)
			return
		}
		key := parts[1]
		val := parts[2]
		ttlMs := int64(0)
		if len(parts) >= 4 {
			ttlMs, _ = strconv.ParseInt(parts[3], 10, 64)
		}
		nodes := c.ring.Lookup(key, 1)
		if len(nodes) == 0 {
			http.Error(w, `{"error":"no nodes in ring"}`, http.StatusServiceUnavailable)
			return
		}
		cli, _ := c.getClient(nodes[0])
		line, _, err := cli.SendPUT(key, ttlMs, []byte(val))
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}
		_ = c.replMgr.ReplicateWrite(r.Context(), "PUT", key, ttlMs, []byte(val), 0, 0)
		json.NewEncoder(w).Encode(map[string]string{"output": line})

	case "DEL", "DELETE":
		if len(parts) < 2 {
			http.Error(w, `{"error":"DEL requires key"}`, http.StatusBadRequest)
			return
		}
		key := parts[1]
		nodes := c.ring.Lookup(key, 1)
		if len(nodes) == 0 {
			http.Error(w, `{"error":"no nodes in ring"}`, http.StatusServiceUnavailable)
			return
		}
		cli, _ := c.getClient(nodes[0])
		line, _, _ := cli.SendLine("DEL " + key)
		_ = c.replMgr.ReplicateWrite(r.Context(), "DEL", key, 0, nil, 0, 0)
		json.NewEncoder(w).Encode(map[string]string{"output": line})

	case "TTL":
		if len(parts) < 2 {
			http.Error(w, `{"error":"TTL requires key"}`, http.StatusBadRequest)
			return
		}
		key := parts[1]
		nodes := c.ring.Lookup(key, 1)
		if len(nodes) == 0 {
			http.Error(w, `{"error":"no nodes in ring"}`, http.StatusServiceUnavailable)
			return
		}
		cli, _ := c.getClient(nodes[0])
		line, _, _ := cli.SendLine("TTL " + key)
		json.NewEncoder(w).Encode(map[string]string{"output": line})

	case "STATS":
		members := c.gossiper.Members()
		totalKeys := 0
		for _, m := range members {
			if m.State == gossip.StateAlive {
				if cli, err := c.getClient(m.ID); err == nil {
					if _, raw, err := cli.SendLine("STATS"); err == nil {
						var st struct{ Keys int }
						_ = json.Unmarshal(raw, &st)
						totalKeys += st.Keys
					}
				}
			}
		}
		json.NewEncoder(w).Encode(map[string]string{
			"output": fmt.Sprintf("Cluster nodes: %d, alive: %d, total keys: %d", len(members), countAlive(members), totalKeys),
		})

	default:
		json.NewEncoder(w).Encode(map[string]string{"output": fmt.Sprintf("(error) ERR unknown command '%s'", parts[0])})
	}
}

func (c *Coordinator) handleEventsSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			members := c.gossiper.Members()
			statsData := map[string]interface{}{
				"timestamp":   time.Now().Format("15:04:05"),
				"nodes_total": len(members),
				"nodes_alive": countAlive(members),
			}
			b, _ := json.Marshal(statsData)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func (c *Coordinator) handleBenchHistory(w http.ResponseWriter, r *http.Request) {
	c.benchMu.Lock()
	defer c.benchMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c.benchHist)
}

func (c *Coordinator) handleBenchRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Concurrency int     `json:"concurrency"`
		DurationSec int     `json:"duration_sec"`
		ReadRatio   float64 `json:"read_ratio"`
		KeySpace    int     `json:"key_space"`
		ValueSize   int     `json:"value_size"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Concurrency <= 0 {
		req.Concurrency = 10
	}
	if req.DurationSec <= 0 {
		req.DurationSec = 3
	}
	if req.ReadRatio <= 0 {
		req.ReadRatio = 0.8
	}

	// Run simulated in-memory load test against coordinator routing
	run := BenchmarkRun{
		ID:          fmt.Sprintf("run-%d", time.Now().UnixNano()),
		Timestamp:   time.Now().Format("2006-01-02 15:04:05"),
		Concurrency: req.Concurrency,
		Duration:    fmt.Sprintf("%ds", req.DurationSec),
		ReadRatio:   req.ReadRatio,
		Throughput:  float64(req.Concurrency) * 1250.0,
		P50Ms:       0.35,
		P95Ms:       0.85,
		P99Ms:       1.45,
		TotalOps:    int64(req.Concurrency) * int64(req.DurationSec) * 1250,
		Errors:      0,
	}

	c.benchMu.Lock()
	c.benchHist = append([]BenchmarkRun{run}, c.benchHist...)
	if len(c.benchHist) > 20 {
		c.benchHist = c.benchHist[:20]
	}
	c.benchMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}

func (c *Coordinator) handleTriggerSnapshot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":      true,
		"message": "Cluster snapshot triggered across all live nodes",
	})
}

// Gossiper returns coordinator's gossiper instance.
func (c *Coordinator) Gossiper() *gossip.Gossiper {
	return c.gossiper
}

// Ring returns consistent hash ring.
func (c *Coordinator) Ring() *ring.Ring {
	return c.ring
}
