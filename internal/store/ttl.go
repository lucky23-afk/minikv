package store

import (
	"container/heap"
	"sync"
	"time"
)

// ttlItem is one element in the min-heap.
type ttlItem struct {
	key       string
	expiresAt int64 // Unix nanoseconds
	index     int   // position in the heap slice (for heap.Fix / heap.Remove)
}

// ttlHeap implements heap.Interface ordered by expiresAt ascending.
type ttlHeap []*ttlItem

func (h ttlHeap) Len() int           { return len(h) }
func (h ttlHeap) Less(i, j int) bool { return h[i].expiresAt < h[j].expiresAt }
func (h ttlHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *ttlHeap) Push(x interface{}) {
	item := x.(*ttlItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *ttlHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// ttlIndex wraps a min-heap and a lookup map so the shard can update
// or remove items in O(log n).
type ttlIndex struct {
	h    ttlHeap
	keys map[string]*ttlItem
}

func newTTLIndex() *ttlIndex {
	return &ttlIndex{keys: make(map[string]*ttlItem)}
}

// Set inserts or updates the expiry for key.
func (idx *ttlIndex) Set(key string, expiresAt int64) {
	if item, ok := idx.keys[key]; ok {
		item.expiresAt = expiresAt
		heap.Fix(&idx.h, item.index)
		return
	}
	item := &ttlItem{key: key, expiresAt: expiresAt}
	heap.Push(&idx.h, item)
	idx.keys[key] = item
}

// Remove removes key from the index (no-op if absent).
func (idx *ttlIndex) Remove(key string) {
	item, ok := idx.keys[key]
	if !ok {
		return
	}
	heap.Remove(&idx.h, item.index)
	delete(idx.keys, key)
}

// PopExpired removes and returns all keys whose expiresAt <= now.
func (idx *ttlIndex) PopExpired(now int64) []string {
	var expired []string
	for idx.h.Len() > 0 && idx.h[0].expiresAt <= now {
		item := heap.Pop(&idx.h).(*ttlItem)
		delete(idx.keys, item.key)
		expired = append(expired, item.key)
	}
	return expired
}

// Peek returns the soonest expiry time, or 0 if the index is empty.
func (idx *ttlIndex) Peek() int64 {
	if idx.h.Len() == 0 {
		return 0
	}
	return idx.h[0].expiresAt
}

// sweeper runs the active TTL sweeper for a shard. It stops when ctx
// is cancelled.  The shard is locked inside deleteExpired.
func sweeper(s *shard, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case t := <-ticker.C:
			s.deleteExpired(t.UnixNano())
		}
	}
}

// shardSweeper manages a sweeper goroutine for one shard.
type shardSweeper struct {
	stop chan struct{}
	wg   sync.WaitGroup
}

func startSweeper(s *shard, interval time.Duration) *shardSweeper {
	sw := &shardSweeper{stop: make(chan struct{})}
	sw.wg.Add(1)
	go func() {
		defer sw.wg.Done()
		sweeper(s, interval, sw.stop)
	}()
	return sw
}

func (sw *shardSweeper) Shutdown() {
	close(sw.stop)
	sw.wg.Wait()
}
