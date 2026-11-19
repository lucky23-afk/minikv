package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kaushik/minikv/internal/proto"
)

func mockNodeServer(t *testing.T) (net.Listener, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock node: %v", err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := proto.NewReader(c)
				w := proto.NewWriter(c)
				for {
					req, err := r.ReadRequest()
					if err != nil {
						return
					}
					if req.Command == "PUT" {
						w.WriteOK()
					} else if req.Command == "DEL" {
						w.WriteInt(1)
					}
				}
			}(conn)
		}
	}()

	return ln, func() { ln.Close() }
}

func TestReplication_SyncAndAsync(t *testing.T) {
	ln1, close1 := mockNodeServer(t)
	defer close1()
	ln2, close2 := mockNodeServer(t)
	defer close2()

	mgr := NewManager("master-node", RoleMaster, nil)
	mgr.AddPeer("replica-1", ln1.Addr().String())
	mgr.AddPeer("replica-2", ln2.Addr().String())

	// 1. Test Async write (0 acks)
	ctx := context.Background()
	err := mgr.ReplicateWrite(ctx, "PUT", "key1", 0, []byte("val1"), 1, 0)
	if err != nil {
		t.Fatalf("async ReplicateWrite: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	statuses := mgr.Statuses()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 peer statuses, got %d", len(statuses))
	}

	// 2. Test Sync write (2 acks)
	ctxSync, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = mgr.ReplicateWrite(ctxSync, "PUT", "key2", 0, []byte("val2"), 2, 2)
	if err != nil {
		t.Fatalf("sync ReplicateWrite: %v", err)
	}

	// 3. Test promotion selection
	cand, seq, err := mgr.SelectPromotionCandidate()
	if err != nil {
		t.Fatalf("SelectPromotionCandidate: %v", err)
	}
	if cand == "" || seq != 2 {
		t.Fatalf("expected promotion candidate with seq 2, got cand=%q seq=%d", cand, seq)
	}
}
