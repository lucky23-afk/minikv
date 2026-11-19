// Package lru provides an O(1) least-recently-used cache backed by
// container/list and a hash map. It supports eviction by entry count
// and by total byte size, and calls an optional OnEvict hook on each
// evicted entry.
package lru

import "container/list"

// entry is stored as the Value of each list.Element.
type entry struct {
	key   string
	value []byte
	bytes int64
}

// LRU is a least-recently-used cache. The zero value is not usable;
// create one with New.
//
// LRU is not safe for concurrent use. The caller (typically a Store
// shard) must hold its own mutex.
type LRU struct {
	maxEntries int
	maxBytes   int64

	list      *list.List               // front = most recently used
	items     map[string]*list.Element // key -> element
	usedBytes int64

	// OnEvict is called once for every evicted entry, with the key and
	// the value that was evicted. It is called while the LRU is locked
	// by the owner — do not re-enter the LRU inside this callback.
	OnEvict func(key string, val []byte)
}

// New creates an LRU with the given capacity limits.
//   - maxEntries <= 0 means no entry-count limit.
//   - maxBytes   <= 0 means no byte limit.
//
// At least one limit should be positive, but neither is enforced here.
func New(maxEntries int, maxBytes int64) *LRU {
	return &LRU{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		list:       list.New(),
		items:      make(map[string]*list.Element),
	}
}

// Len returns the number of entries currently in the cache.
func (l *LRU) Len() int { return len(l.items) }

// Bytes returns the total byte count of values currently in the cache.
func (l *LRU) Bytes() int64 { return l.usedBytes }

// Get retrieves the value for key and marks it as recently used.
// Returns (value, true) on hit or (nil, false) on miss.
func (l *LRU) Get(key string) ([]byte, bool) {
	el, ok := l.items[key]
	if !ok {
		return nil, false
	}
	l.list.MoveToFront(el)
	return el.Value.(*entry).value, true
}

// Put inserts or updates key with val. After the update it evicts
// least-recently-used entries until both capacity limits are satisfied.
func (l *LRU) Put(key string, val []byte) {
	size := int64(len(val))

	if el, ok := l.items[key]; ok {
		// Update existing entry.
		e := el.Value.(*entry)
		l.usedBytes += size - e.bytes
		e.value = val
		e.bytes = size
		l.list.MoveToFront(el)
	} else {
		// Insert new entry at the front.
		e := &entry{key: key, value: val, bytes: size}
		el := l.list.PushFront(e)
		l.items[key] = el
		l.usedBytes += size
	}

	l.evict()
}

// Remove removes key from the cache if it exists. Returns true if the
// key was present.
func (l *LRU) Remove(key string) bool {
	el, ok := l.items[key]
	if !ok {
		return false
	}
	l.removeElement(el)
	return true
}

// Keys returns all keys in LRU order (most-recently-used first).
func (l *LRU) Keys() []string {
	keys := make([]string, 0, len(l.items))
	for el := l.list.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Value.(*entry).key)
	}
	return keys
}

// evict removes tail entries until both limits are satisfied.
func (l *LRU) evict() {
	for {
		if !l.overLimit() {
			break
		}
		tail := l.list.Back()
		if tail == nil {
			break
		}
		l.removeElement(tail)
	}
}

func (l *LRU) overLimit() bool {
	if l.maxEntries > 0 && len(l.items) > l.maxEntries {
		return true
	}
	if l.maxBytes > 0 && l.usedBytes > l.maxBytes {
		return true
	}
	return false
}

func (l *LRU) removeElement(el *list.Element) {
	e := el.Value.(*entry)
	l.list.Remove(el)
	delete(l.items, e.key)
	l.usedBytes -= e.bytes
	if l.OnEvict != nil {
		l.OnEvict(e.key, e.value)
	}
}
