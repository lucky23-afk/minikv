// Package node implements the storage node: a Store + WAL exposed over
// a RESP-like TCP protocol and a lightweight HTTP API.
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kaushik/minikv/internal/proto"
	"github.com/kaushik/minikv/internal/store"
	"github.com/kaushik/minikv/internal/wal"
)

// Config holds node configuration.
type Config struct {
	ID          string
	TCPAddr     string
	HTTPAddr    string
	DataDir     string
	MaxEntries  int
	MaxBytes    int64
	FsyncPolicy wal.FsyncPolicy
}

// Node is a storage node.
type Node struct {
	cfg   Config
	store *store.Store
	wal   *wal.WAL
	log   *slog.Logger

	tcpLn   net.Listener
	httpSrv *http.Server
	wg      sync.WaitGroup
}

// New creates a Node. It runs WAL recovery before returning.
func New(cfg Config, log *slog.Logger) (*Node, error) {
	if log == nil {
		log = slog.Default()
	}

	// 1. Recover from WAL + snapshot.
	state, err := wal.Recover(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("node.New recover: %w", err)
	}
	log.Info("recovery complete",
		"entries", len(state.Entries),
		"lastSeq", state.LastSeq,
		"snapshot", state.SnapshotPath)

	// 2. Create store and load recovered entries.
	st := store.New(store.Config{
		MaxEntries: cfg.MaxEntries,
		MaxBytes:   cfg.MaxBytes,
	})
	for _, e := range state.Entries {
		var ttl time.Duration
		if e.ExpiresAt != 0 {
			ttl = time.Until(time.Unix(0, e.ExpiresAt))
			if ttl <= 0 {
				continue // already expired
			}
		}
		st.Put(e.Key, e.Value, ttl)
	}

	// 3. Open WAL (append mode, starting after last recovered seq).
	w, err := wal.Open(cfg.DataDir, cfg.FsyncPolicy, state.LastSeq)
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("node.New open wal: %w", err)
	}

	return &Node{cfg: cfg, store: st, wal: w, log: log}, nil
}

// Start binds listeners and starts serving. It blocks until ctx is
// cancelled, then shuts down cleanly.
func (n *Node) Start(ctx context.Context) error {
	var err error
	n.tcpLn, err = net.Listen("tcp", n.cfg.TCPAddr)
	if err != nil {
		return fmt.Errorf("node.Start TCP listen %s: %w", n.cfg.TCPAddr, err)
	}
	n.log.Info("TCP listener started", "addr", n.tcpLn.Addr())

	mux := http.NewServeMux()
	n.registerHTTP(mux)
	n.httpSrv = &http.Server{Addr: n.cfg.HTTPAddr, Handler: mux}

	go func() {
		if err := n.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			n.log.Error("HTTP server error", "err", err)
		}
	}()
	n.log.Info("HTTP server started", "addr", n.cfg.HTTPAddr)

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.acceptTCP(ctx)
	}()

	<-ctx.Done()
	return n.shutdown()
}

func (n *Node) acceptTCP(ctx context.Context) {
	for {
		conn, err := n.tcpLn.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				n.log.Error("TCP accept error", "err", err)
				return
			}
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.handleConn(ctx, conn)
		}()
	}
}

func (n *Node) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	r := proto.NewReader(conn)
	w := proto.NewWriter(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		req, err := r.ReadRequest()
		if err != nil {
			return // EOF or timeout — close silently
		}

		if err := n.dispatch(req, w); err != nil {
			n.log.Warn("dispatch error", "cmd", req.Command, "err", err)
			return
		}
	}
}

func (n *Node) dispatch(req *proto.Request, w *proto.Writer) error {
	switch req.Command {
	case "GET":
		return n.cmdGet(req, w)
	case "PUT":
		return n.cmdPut(req, w)
	case "DEL", "DELETE":
		return n.cmdDel(req, w)
	case "EXISTS":
		return n.cmdExists(req, w)
	case "TTL":
		return n.cmdTTL(req, w)
	case "KEYS":
		return n.cmdKeys(req, w)
	case "STATS":
		return n.cmdStats(req, w)
	case "PING":
		return w.WriteOK()
	default:
		return w.WriteError(fmt.Sprintf("unknown command %q", req.Command))
	}
}

func (n *Node) cmdGet(req *proto.Request, w *proto.Writer) error {
	if len(req.Args) < 1 {
		return w.WriteError("GET requires key")
	}
	val, err := n.store.Get(req.Args[0])
	if err != nil {
		return w.WriteBulk(nil)
	}
	return w.WriteBulk(val)
}

func (n *Node) cmdPut(req *proto.Request, w *proto.Writer) error {
	if len(req.Args) < 1 {
		return w.WriteError("PUT requires key")
	}
	key := req.Args[0]
	var ttl time.Duration
	if len(req.Args) >= 2 {
		ms, err := strconv.ParseInt(req.Args[1], 10, 64)
		if err == nil && ms > 0 {
			ttl = time.Duration(ms) * time.Millisecond
		}
	}

	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	_, err := n.wal.Append(&wal.Record{
		Type:      wal.RecordPut,
		Key:       key,
		Value:     req.Value,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return w.WriteError(fmt.Sprintf("WAL append: %v", err))
	}
	n.store.Put(key, req.Value, ttl)
	return w.WriteOK()
}

func (n *Node) cmdDel(req *proto.Request, w *proto.Writer) error {
	if len(req.Args) < 1 {
		return w.WriteError("DEL requires key")
	}
	key := req.Args[0]
	_, err := n.wal.Append(&wal.Record{Type: wal.RecordDelete, Key: key})
	if err != nil {
		return w.WriteError(fmt.Sprintf("WAL append: %v", err))
	}
	existed := n.store.Delete(key)
	if existed {
		return w.WriteInt(1)
	}
	return w.WriteInt(0)
}

func (n *Node) cmdExists(req *proto.Request, w *proto.Writer) error {
	if len(req.Args) < 1 {
		return w.WriteError("EXISTS requires key")
	}
	if n.store.Exists(req.Args[0]) {
		return w.WriteInt(1)
	}
	return w.WriteInt(0)
}

func (n *Node) cmdTTL(req *proto.Request, w *proto.Writer) error {
	if len(req.Args) < 1 {
		return w.WriteError("TTL requires key")
	}
	d, err := n.store.TTL(req.Args[0])
	if err != nil {
		return w.WriteInt(-2) // not found
	}
	if d == -1 {
		return w.WriteInt(-1) // no TTL
	}
	return w.WriteInt(d.Milliseconds())
}

func (n *Node) cmdKeys(req *proto.Request, w *proto.Writer) error {
	prefix := ""
	if len(req.Args) >= 1 {
		prefix = req.Args[0]
	}
	keys := n.store.Keys(prefix)
	items := make([][]byte, len(keys))
	for i, k := range keys {
		items[i] = []byte(k)
	}
	return w.WriteArray(items)
}

func (n *Node) cmdStats(req *proto.Request, w *proto.Writer) error {
	st := n.store.Stats()
	b, err := json.Marshal(st)
	if err != nil {
		return w.WriteError("marshal stats")
	}
	return w.WriteBulk(b)
}

// registerHTTP mounts internal HTTP routes (for replication, gossip, and
// coordinator use in later phases; for now it also exposes a /health check).
func (n *Node) registerHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/health", n.handleHealth)
	mux.HandleFunc("/stats", n.handleStats)
}

func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "id": n.cfg.ID})
}

func (n *Node) handleStats(w http.ResponseWriter, r *http.Request) {
	st := n.store.Stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

func (n *Node) shutdown() error {
	n.log.Info("shutting down node")

	// Stop accepting new TCP connections.
	n.tcpLn.Close()

	// Stop HTTP server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n.httpSrv.Shutdown(ctx)

	// Wait for in-flight connections.
	n.wg.Wait()

	// Write a final snapshot.
	entries := n.snapshotEntries()
	if _, err := wal.WriteSnapshot(n.cfg.DataDir, entries); err != nil {
		n.log.Error("final snapshot failed", "err", err)
	}

	// Close WAL.
	if err := n.wal.Close(); err != nil {
		n.log.Error("WAL close error", "err", err)
	}

	n.store.Close()
	n.log.Info("node stopped")
	return nil
}

// snapshotEntries collects all live store entries for a snapshot.
func (n *Node) snapshotEntries() []wal.SnapshotEntry {
	keys := n.store.Keys("")
	entries := make([]wal.SnapshotEntry, 0, len(keys))
	now := time.Now()
	for _, k := range keys {
		val, err := n.store.Get(k)
		if err != nil {
			continue
		}
		d, _ := n.store.TTL(k)
		var expiresAt int64
		if d > 0 {
			expiresAt = now.Add(d).UnixNano()
		}
		entries = append(entries, wal.SnapshotEntry{Key: k, Value: val, ExpiresAt: expiresAt})
	}
	return entries
}

// Store returns the node's Store for testing.
func (n *Node) Store() *store.Store { return n.store }

// RunUntilSignal is a convenience wrapper for running a node from main.
// It listens for SIGINT / SIGTERM.
func RunUntilSignal(cfg Config, log *slog.Logger) error {
	n, err := New(cfg, log)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("signal received, shutting down")
		cancel()
	}()
	return n.Start(ctx)
}

// TCPClient is a simple client for the node TCP protocol.
type TCPClient struct {
	addr string
	mu   sync.Mutex
	conn net.Conn
	r    *proto.Reader
	w    *proto.Writer
}

// NewTCPClient creates a client that connects lazily.
func NewTCPClient(addr string) *TCPClient {
	return &TCPClient{addr: addr}
}

func (c *TCPClient) dial() error {
	if c.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	c.r = proto.NewReader(conn)
	c.w = proto.NewWriter(conn)
	return nil
}

func (c *TCPClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// SendLine sends a request line and returns the raw response line.
func (c *TCPClient) SendLine(line string) (string, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.dial(); err != nil {
		return "", nil, err
	}
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := fmt.Fprintf(c.conn, "%s\r\n", line); err != nil {
		c.conn.Close()
		c.conn = nil
		return "", nil, err
	}
	return c.readResponse()
}

// SendPUT sends a PUT request with a value payload.
func (c *TCPClient) SendPUT(key string, ttlMs int64, value []byte) (string, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.dial(); err != nil {
		return "", nil, err
	}
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))

	line := fmt.Sprintf("PUT %s %d\r\n%d\r\n", key, ttlMs, len(value))
	if _, err := fmt.Fprint(c.conn, line); err != nil {
		c.conn.Close()
		c.conn = nil
		return "", nil, err
	}
	if _, err := c.conn.Write(value); err != nil {
		c.conn.Close()
		c.conn = nil
		return "", nil, err
	}
	return c.readResponse()
}

// readResponse reads one protocol response.
func (c *TCPClient) readResponse() (string, []byte, error) {
	import_bufio_reader := c.r
	_ = import_bufio_reader

	// Read the status/type line.
	buf := make([]byte, 0, 64)
	b := make([]byte, 1)
	for {
		_, err := c.conn.Read(b)
		if err != nil {
			return "", nil, err
		}
		if b[0] == '\n' {
			break
		}
		if b[0] != '\r' {
			buf = append(buf, b[0])
		}
	}
	line := string(buf)

	switch {
	case strings.HasPrefix(line, "$"):
		n, _ := strconv.Atoi(line[1:])
		if n == -1 {
			return line, nil, nil
		}
		data := make([]byte, n+2) // +2 for \r\n
		if _, err := readFull(c.conn, data); err != nil {
			return "", nil, err
		}
		return line, data[:n], nil
	case strings.HasPrefix(line, "*"):
		n, _ := strconv.Atoi(line[1:])
		var items [][]byte
		for i := 0; i < n; i++ {
			_, item, err := c.readResponse()
			if err != nil {
				return "", nil, err
			}
			items = append(items, item)
		}
		// Flatten array into newline-separated for simple callers.
		var sb strings.Builder
		for _, item := range items {
			sb.Write(item)
			sb.WriteByte('\n')
		}
		return line, []byte(sb.String()), nil
	default:
		return line, nil, nil
	}
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
