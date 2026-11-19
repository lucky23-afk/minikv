package node

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/kaushik/minikv/internal/wal"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func startTestNode(t *testing.T) (*Node, *TCPClient, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "node-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}

	tcpPort := getFreePort(t)
	httpPort := getFreePort(t)

	cfg := Config{
		ID:          "test-node-1",
		TCPAddr:     fmt.Sprintf("127.0.0.1:%d", tcpPort),
		HTTPAddr:    fmt.Sprintf("127.0.0.1:%d", httpPort),
		DataDir:     dir,
		FsyncPolicy: wal.FsyncNone,
	}

	n, err := New(cfg, nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("New node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- n.Start(ctx)
	}()

	// Wait for TCP server to become available
	client := NewTCPClient(cfg.TCPAddr)
	var pingErr error
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		line, _, err := client.SendLine("PING")
		if err == nil && line == "+OK" {
			pingErr = nil
			break
		}
		pingErr = err
	}
	if pingErr != nil {
		cancel()
		os.RemoveAll(dir)
		t.Fatalf("timed out connecting to test node: %v", pingErr)
	}

	cleanup := func() {
		client.Close()
		cancel()
		<-errCh
		os.RemoveAll(dir)
	}

	return n, client, cleanup
}

func TestNode_BasicCommands(t *testing.T) {
	_, client, cleanup := startTestNode(t)
	defer cleanup()

	// 1. PUT
	line, _, err := client.SendPUT("greeting", 0, []byte("hello world"))
	if err != nil {
		t.Fatalf("SendPUT: %v", err)
	}
	if line != "+OK" {
		t.Fatalf("PUT expected +OK, got %q", line)
	}

	// 2. GET
	line, data, err := client.SendLine("GET greeting")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("GET expected 'hello world', got %q (line: %s)", string(data), line)
	}

	// 3. EXISTS
	line, _, err = client.SendLine("EXISTS greeting")
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if line != ":1" {
		t.Fatalf("EXISTS expected :1, got %q", line)
	}

	// 4. TTL
	line, _, err = client.SendLine("TTL greeting")
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if line != ":-1" {
		t.Fatalf("TTL expected :-1 (no expiry), got %q", line)
	}

	// 5. KEYS
	line, data, err = client.SendLine("KEYS greet")
	if err != nil {
		t.Fatalf("KEYS: %v", err)
	}
	if line != "*1" {
		t.Fatalf("KEYS expected *1, got %q", line)
	}

	// 6. DEL
	line, _, err = client.SendLine("DEL greeting")
	if err != nil {
		t.Fatalf("DEL: %v", err)
	}
	if line != ":1" {
		t.Fatalf("DEL expected :1, got %q", line)
	}

	// 7. GET after DEL
	line, data, err = client.SendLine("GET greeting")
	if err != nil {
		t.Fatalf("GET after DEL: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data after DEL, got %q", string(data))
	}
}

func TestNode_TTL(t *testing.T) {
	_, client, cleanup := startTestNode(t)
	defer cleanup()

	// PUT with 100ms TTL
	line, _, err := client.SendPUT("tempKey", 100, []byte("tempVal"))
	if err != nil || line != "+OK" {
		t.Fatalf("PUT with TTL: %v, line: %s", err, line)
	}

	// GET immediate
	_, data, err := client.SendLine("GET tempKey")
	if err != nil || string(data) != "tempVal" {
		t.Fatalf("GET immediate: %v, val: %s", err, string(data))
	}

	time.Sleep(150 * time.Millisecond)

	// GET after expiry
	_, data, err = client.SendLine("GET tempKey")
	if err != nil {
		t.Fatalf("GET after expiry: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data after expiry, got %q", string(data))
	}
}
