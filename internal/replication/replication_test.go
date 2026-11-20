package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lucky23-afk/minikv/internal/proto"
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

func TestReplication_CatchUpEvaluation(t *testing.T) {
	mgr := NewManager("master", RoleMaster, nil)

	// Small lag: WAL catchup
	needsSnap, lag := mgr.CheckCatchUpMode(100, 150)
	if needsSnap || lag != 50 {
		t.Fatalf("expected WAL catchup (needsSnap=false, lag=50), got snap=%v lag=%d", needsSnap, lag)
	}

	// Large lag > 1000: Snapshot catchup
	needsSnap, lag = mgr.CheckCatchUpMode(100, 2000)
	if !needsSnap || lag != 1900 {
		t.Fatalf("expected snapshot catchup (needsSnap=true, lag=1900), got snap=%v lag=%d", needsSnap, lag)
	}

	// Up to date
	needsSnap, lag = mgr.CheckCatchUpMode(500, 500)
	if needsSnap || lag != 0 {
		t.Fatalf("expected up to date (lag=0), got lag=%d", lag)
	}
}

func TestReplication_SequenceEnforcement(t *testing.T) {
	applier := NewInboundLogApplier(10)

	// In-order write
	res := applier.ValidateAndApply(11)
	if res != ApplyOK || applier.AppliedSeq() != 11 {
		t.Fatalf("expected ApplyOK, got %v", res)
	}

	// Duplicate write
	res = applier.ValidateAndApply(11)
	if res != ApplyDuplicate {
		t.Fatalf("expected ApplyDuplicate for seq 11, got %v", res)
	}

	// Older write
	res = applier.ValidateAndApply(8)
	if res != ApplyDuplicate {
		t.Fatalf("expected ApplyDuplicate for seq 8, got %v", res)
	}

	// Gap write (skipped 12, received 15)
	res = applier.ValidateAndApply(15)
	if res != ApplyGap {
		t.Fatalf("expected ApplyGap for seq 15, got %v", res)
	}
}

func TestReplication_PromotionAndPeerManagement(t *testing.T) {
	mgr := NewManager("master-1", RoleMaster, nil)
	mgr.SetRole(RoleMaster)
	if mgr.Role() != RoleMaster {
		t.Fatalf("expected RoleMaster, got %v", mgr.Role())
	}

	mgr.AddPeer("rep-A", "127.0.0.1:9001")
	mgr.AddPeer("rep-B", "127.0.0.1:9002")

	// Update local seq
	mgr.UpdateLocalSeq(50)

	// Simulate acks
	mgr.recordAck("rep-A", 48)
	mgr.recordAck("rep-B", 50)

	cand, seq, err := mgr.SelectPromotionCandidate()
	if err != nil {
		t.Fatalf("promotion candidate selection: %v", err)
	}
	if cand != "rep-B" || seq != 50 {
		t.Fatalf("expected rep-B with seq 50, got cand=%q seq=%d", cand, seq)
	}

	// Remove peers
	mgr.RemovePeer("rep-B")
	cand, seq, _ = mgr.SelectPromotionCandidate()
	if cand != "rep-A" || seq != 48 {
		t.Fatalf("expected rep-A with seq 48 after removing rep-B, got cand=%q seq=%d", cand, seq)
	}

	mgr.RemovePeer("rep-A")
	_, _, err = mgr.SelectPromotionCandidate()
	if err == nil {
		t.Fatal("expected error when no replicas are available")
	}
}

func TestReplication_SyncTimeout(t *testing.T) {
	// Mock server that does not respond to simulate unreachable replica
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mgr := NewManager("master-node", RoleMaster, nil)
	mgr.AddPeer("unresponsive-replica", ln.Addr().String())

	// Short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = mgr.ReplicateWrite(ctx, "PUT", "k", 0, []byte("v"), 1, 1)
	if err == nil {
		t.Fatal("expected error waiting for unresponsive replica ack in sync mode")
	}
}
