// Package ring provides a consistent hash ring with virtual nodes,
// binary search lookup, and O(log n) add/remove operations.
package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

const defaultVNodes = 128

// Node is one physical member of the ring.
type Node struct {
	ID       string
	Addr     string // TCP address (host:port)
	HTTPAddr string // HTTP address (host:port)
}

// vnode is one virtual-node slot in the sorted ring.
type vnode struct {
	hash   uint64
	nodeID string
}

// Ring is a consistent-hash ring. It is safe for concurrent use.
type Ring struct {
	mu     sync.RWMutex
	vnodes []vnode          // sorted by hash ascending
	nodes  map[string]*Node // nodeID -> Node
	vn     int              // virtual nodes per physical node
}

// New creates a Ring with vnodes virtual nodes per physical node.
// vnodes <= 0 defaults to 128.
func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = defaultVNodes
	}
	return &Ring{
		nodes: make(map[string]*Node),
		vn:    vnodes,
	}
}

// AddNode inserts n into the ring, adding vn virtual nodes for it.
// If a node with the same ID already exists, it is replaced.
func (r *Ring) AddNode(n Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove any existing vnodes for this node ID.
	r.removeNodeLocked(n.ID)

	r.nodes[n.ID] = &Node{ID: n.ID, Addr: n.Addr, HTTPAddr: n.HTTPAddr}

	for i := 0; i < r.vn; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", n.ID, i))
		r.vnodes = append(r.vnodes, vnode{hash: h, nodeID: n.ID})
	}

	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
}

// RemoveNode removes the node with id from the ring. No-op if absent.
func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeNodeLocked(id)
}

func (r *Ring) removeNodeLocked(id string) {
	delete(r.nodes, id)
	filtered := r.vnodes[:0]
	for _, vn := range r.vnodes {
		if vn.nodeID != id {
			filtered = append(filtered, vn)
		}
	}
	r.vnodes = filtered
}

// Lookup returns up to replicas distinct node IDs responsible for key,
// starting with the primary. Returns fewer nodes if the ring has fewer
// physical nodes.
func (r *Ring) Lookup(key string, replicas int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx == len(r.vnodes) {
		idx = 0 // wrap around
	}

	maxNodes := len(r.nodes)
	if replicas > maxNodes {
		replicas = maxNodes
	}

	seen := make(map[string]bool, replicas)
	var result []string
	for len(result) < replicas {
		nodeID := r.vnodes[idx%len(r.vnodes)].nodeID
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
		idx++
		if idx >= len(r.vnodes)*2 { // safety: prevent infinite loop
			break
		}
	}
	return result
}

// Nodes returns a snapshot of all physical nodes currently in the ring.
func (r *Ring) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	return out
}

// Len returns the number of physical nodes in the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// VNodes returns a copy of all virtual node slots, sorted by hash.
// Useful for visualization.
func (r *Ring) VNodes() []vnode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make([]vnode, len(r.vnodes))
	copy(cp, r.vnodes)
	return cp
}

// VNodeEntry is an exported view of a virtual node slot for use by
// callers (e.g., the dashboard).
type VNodeEntry struct {
	Hash   uint64
	NodeID string
}

// VNodeEntries returns all virtual node slots as exported structs.
func (r *Ring) VNodeEntries() []VNodeEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]VNodeEntry, len(r.vnodes))
	for i, vn := range r.vnodes {
		out[i] = VNodeEntry{Hash: vn.hash, NodeID: vn.nodeID}
	}
	return out
}

// hashKey returns a 64-bit FNV-1a hash of key.
func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
