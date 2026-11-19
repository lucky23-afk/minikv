package store

import (
	"sync"
	"time"

	"github.com/kaushik/minikv/internal/lru"
)

// shard is one stripe of the Store. It owns a hash table, an LRU,
// and a TTL index. All fields are protected by mu.
type shard struct {
	mu       sync.RWMutex
	table    map[string]*Entry
	lruCache *lru.LRU
	ttlIdx   *ttlIndex
	st       *stats // shared pointer to Store stats
}

func newShard(maxEntries int, maxBytes int64, st *stats) *shard {
	s := &shard{
		table:  make(map[string]*Entry),
		ttlIdx: newTTLIndex(),
		st:     st,
	}
	s.lruCache = lru.New(maxEntries, maxBytes)
	s.lruCache.OnEvict = func(key string, _ []byte) {
		// Called with shard lock already held by Put/evict path.
		delete(s.table, key)
		s.ttlIdx.Remove(key)
		st.eviction()
	}
	return s
}

// get returns the entry for key, checking TTL lazily.
func (s *shard) get(key string, now int64) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.table[key]
	if !ok {
		s.st.miss()
		return nil, false
	}
	if e.IsExpired(now) {
		s.deleteKey(key)
		s.st.expired()
		s.st.miss()
		return nil, false
	}
	s.lruCache.Get(key) // promote
	s.st.hit()
	return e.Value, true
}

// put inserts or updates a key. ttl == 0 means no expiry.
func (s *shard) put(key string, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	e := &Entry{Value: value, ExpiresAt: expiresAt, Size: int64(len(value))}
	s.table[key] = e
	s.lruCache.Put(key, value)

	if expiresAt > 0 {
		s.ttlIdx.Set(key, expiresAt)
	} else {
		s.ttlIdx.Remove(key)
	}
}

// delete removes key. Returns true if it existed.
func (s *shard) delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.table[key]
	if !ok {
		return false
	}
	s.deleteKey(key)
	return true
}

// exists checks if key exists and is not expired.
func (s *shard) exists(key string, now int64) bool {
	_, ok := s.get(key, now)
	return ok
}

// ttlRemaining returns the duration until expiry.
// -1: no TTL, -2: key not found, 0+: remaining duration.
func (s *shard) ttlRemaining(key string, now time.Time) (time.Duration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.table[key]
	if !ok {
		return -2, false
	}
	if e.IsExpired(now.UnixNano()) {
		return -2, false
	}
	return e.TTLRemaining(now), true
}

// keys returns all non-expired keys that have the given prefix.
func (s *shard) keys(prefix string, now int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for k, e := range s.table {
		if e.IsExpired(now) {
			continue
		}
		if prefix == "" || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out
}

// len returns the number of live (non-expired) entries.
func (s *shard) len(now int64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.table {
		if !e.IsExpired(now) {
			n++
		}
	}
	return n
}

// bytes returns the total bytes of live values.
func (s *shard) bytes(now int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total int64
	for _, e := range s.table {
		if !e.IsExpired(now) {
			total += e.Size
		}
	}
	return total
}

// deleteExpired is called by the sweeper with the current time in nanos.
func (s *shard) deleteExpired(now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expired := s.ttlIdx.PopExpired(now)
	for _, key := range expired {
		if e, ok := s.table[key]; ok && e.IsExpired(now) {
			s.lruCache.Remove(key)
			delete(s.table, key)
			s.st.expired()
		}
	}
}

// deleteKey removes key unconditionally. Must be called with mu held.
func (s *shard) deleteKey(key string) {
	delete(s.table, key)
	s.lruCache.Remove(key)
	s.ttlIdx.Remove(key)
}
