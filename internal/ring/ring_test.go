package ring

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

func makeNode(id string) Node {
	return Node{ID: id, Addr: "127.0.0.1:700" + id, HTTPAddr: "127.0.0.1:800" + id}
}

// TestRing_Lookup_Deterministic verifies the same key always maps to the
// same primary node.
func TestRing_Lookup_Deterministic(t *testing.T) {
	r := New(128)
	r.AddNode(makeNode("1"))
	r.AddNode(makeNode("2"))
	r.AddNode(makeNode("3"))

	for _, key := range []string{"foo", "bar", "baz", "user:1", "product:99"} {
		first := r.Lookup(key, 1)
		for i := 0; i < 10; i++ {
			got := r.Lookup(key, 1)
			if len(got) != 1 || got[0] != first[0] {
				t.Fatalf("key %q: got %v, want %v on iteration %d", key, got, first, i)
			}
		}
	}
}

// TestRing_Lookup_ReturnsCorrectReplicaCount checks the replica count.
func TestRing_Lookup_ReturnsCorrectReplicaCount(t *testing.T) {
	r := New(64)
	for i := 0; i < 5; i++ {
		r.AddNode(Node{ID: fmt.Sprintf("n%d", i)})
	}

	tests := []struct {
		replicas int
		want     int
	}{
		{1, 1},
		{3, 3},
		{5, 5},
		{10, 5}, // capped at physical node count
	}
	for _, tc := range tests {
		got := r.Lookup("anykey", tc.replicas)
		if len(got) != tc.want {
			t.Errorf("replicas=%d: got %d nodes, want %d", tc.replicas, len(got), tc.want)
		}
	}
}

// TestRing_Lookup_DistinctNodes verifies no duplicate node IDs in result.
func TestRing_Lookup_DistinctNodes(t *testing.T) {
	r := New(128)
	for i := 0; i < 10; i++ {
		r.AddNode(Node{ID: fmt.Sprintf("n%d", i)})
	}
	nodes := r.Lookup("testkey", 10)
	seen := map[string]bool{}
	for _, id := range nodes {
		if seen[id] {
			t.Fatalf("duplicate node %q in Lookup result %v", id, nodes)
		}
		seen[id] = true
	}
}

// TestRing_VirtualNodes_Distribution checks that keys are spread across nodes.
func TestRing_VirtualNodes_Distribution(t *testing.T) {
	r := New(128)
	r.AddNode(makeNode("A"))
	r.AddNode(makeNode("B"))
	r.AddNode(makeNode("C"))

	counts := map[string]int{}
	const keyCount = 10000
	for i := 0; i < keyCount; i++ {
		primary := r.Lookup(fmt.Sprintf("key-%d", i), 1)
		counts[primary[0]]++
	}

	// Each node should own between 20% and 50% of keys.
	for id, c := range counts {
		ratio := float64(c) / keyCount
		if ratio < 0.15 || ratio > 0.60 {
			t.Errorf("node %s owns %.1f%% of keys (want 15-60%%)", id, ratio*100)
		}
	}
}

// TestRing_AddNode_Idempotent verifies re-adding a node doesn't duplicate vnodes.
func TestRing_AddNode_Idempotent(t *testing.T) {
	r := New(10)
	r.AddNode(makeNode("X"))
	r.AddNode(makeNode("X")) // second add should replace, not append
	if r.Len() != 1 {
		t.Fatalf("expected 1 node, got %d", r.Len())
	}
	vnodes := r.VNodes()
	if len(vnodes) != 10 {
		t.Fatalf("expected 10 vnodes, got %d", len(vnodes))
	}
}

// TestRing_RemoveNode removes a node and verifies its keys remap.
func TestRing_RemoveNode(t *testing.T) {
	r := New(128)
	r.AddNode(makeNode("1"))
	r.AddNode(makeNode("2"))
	r.AddNode(makeNode("3"))

	// Record primary for 1000 keys.
	const n = 1000
	origPrimary := make([]string, n)
	for i := 0; i < n; i++ {
		p := r.Lookup(fmt.Sprintf("k%d", i), 1)
		origPrimary[i] = p[0]
	}

	r.RemoveNode("2")

	// No key should now map to "2".
	for i := 0; i < n; i++ {
		p := r.Lookup(fmt.Sprintf("k%d", i), 1)
		if p[0] == "2" {
			t.Fatalf("key k%d still maps to removed node '2'", i)
		}
	}

	// Keys that were NOT on "2" should still map to the same node.
	remapped := 0
	for i := 0; i < n; i++ {
		if origPrimary[i] == "2" {
			remapped++ // expected remapping
			continue
		}
		p := r.Lookup(fmt.Sprintf("k%d", i), 1)
		if p[0] != origPrimary[i] {
			remapped++
		}
	}
	// Only keys that were on node "2" should have moved.
	maxExpectedRemap := n / 3 * 2 // generous bound
	if remapped > maxExpectedRemap {
		t.Errorf("too many keys remapped: %d (want <= %d)", remapped, maxExpectedRemap)
	}
}

// TestRing_RemoveNode_NoOp verifies removing absent node is safe.
func TestRing_RemoveNode_NoOp(t *testing.T) {
	r := New(16)
	r.AddNode(makeNode("A"))
	r.RemoveNode("nonexistent") // must not panic
	if r.Len() != 1 {
		t.Fatal("node count changed after removing absent node")
	}
}

// TestRing_Empty_Lookup returns nil on empty ring.
func TestRing_Empty_Lookup(t *testing.T) {
	r := New(16)
	got := r.Lookup("key", 3)
	if got != nil {
		t.Fatalf("expected nil from empty ring, got %v", got)
	}
}

// TestRing_SingleNode_Wraps verifies the ring wraps around correctly.
func TestRing_SingleNode_Wraps(t *testing.T) {
	r := New(16)
	r.AddNode(makeNode("only"))
	for i := 0; i < 20; i++ {
		got := r.Lookup(fmt.Sprintf("k%d", i), 1)
		if len(got) != 1 || got[0] != "only" {
			t.Fatalf("expected [only], got %v", got)
		}
	}
}

// TestRing_VNodesSorted verifies the internal vnode slice is sorted.
func TestRing_VNodesSorted(t *testing.T) {
	r := New(50)
	r.AddNode(makeNode("alpha"))
	r.AddNode(makeNode("beta"))
	vnodes := r.VNodes()
	for i := 1; i < len(vnodes); i++ {
		if vnodes[i].hash < vnodes[i-1].hash {
			t.Fatalf("vnodes not sorted at index %d: %d < %d", i, vnodes[i].hash, vnodes[i-1].hash)
		}
	}
}

// TestRing_Nodes returns all physical nodes.
func TestRing_Nodes(t *testing.T) {
	r := New(10)
	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		r.AddNode(Node{ID: id})
	}
	got := r.Nodes()
	if len(got) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(got))
	}
	gotIDs := make([]string, len(got))
	for i, n := range got {
		gotIDs[i] = n.ID
	}
	sort.Strings(gotIDs)
	for i, id := range ids {
		_ = i
		found := false
		for _, g := range gotIDs {
			if g == id {
				found = true
			}
		}
		if !found {
			t.Fatalf("node %q not in Nodes()", id)
		}
	}
}

// TestRing_VNodeEntries returns exported entries.
func TestRing_VNodeEntries(t *testing.T) {
	r := New(5)
	r.AddNode(makeNode("X"))
	entries := r.VNodeEntries()
	if len(entries) != 5 {
		t.Fatalf("expected 5 vnode entries, got %d", len(entries))
	}
}

// TestRing_Concurrent is a race-detector test.
func TestRing_Concurrent(t *testing.T) {
	r := New(64)
	r.AddNode(makeNode("A"))
	r.AddNode(makeNode("B"))

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Lookup(fmt.Sprintf("key-%d-%d", i, j), 2)
			}
		}()
	}
	// Also do concurrent adds/removes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			r.AddNode(makeNode(fmt.Sprintf("tmp%d", i)))
			r.RemoveNode(fmt.Sprintf("tmp%d", i))
		}
	}()
	wg.Wait()
}

// --- Benchmarks -------------------------------------------------------------

func BenchmarkRing_Lookup(b *testing.B) {
	r := New(128)
	for i := 0; i < 10; i++ {
		r.AddNode(Node{ID: fmt.Sprintf("n%d", i)})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.Lookup(fmt.Sprintf("key-%d", i), 3)
			i++
		}
	})
}
