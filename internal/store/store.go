package store

import (
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

// Sentinel errors returned by Store methods.
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyExpired  = errors.New("key expired")
)

const (
	defaultShards        = 256
	defaultSweepInterval = 100 * time.Millisecond
)

// Config controls Store behaviour.
type Config struct {
	// NumShards must be a power of two; defaults to 256.
	NumShards int
	// MaxEntries is the LRU entry limit per shard (0 = unlimited).
	MaxEntries int
	// MaxBytes is the LRU byte limit per shard (0 = unlimited).
	MaxBytes int64
	// SweepInterval is how often the active TTL sweeper runs.
	// Defaults to 100 ms.
	SweepInterval time.Duration
}

func (c *Config) shards() int {
	if c.NumShards <= 0 {
		return defaultShards
	}
	return c.NumShards
}

func (c *Config) sweepInterval() time.Duration {
	if c.SweepInterval <= 0 {
		return defaultSweepInterval
	}
	return c.SweepInterval
}

// Store is a sharded, in-memory key-value store with LRU eviction and
// TTL expiry. It is safe for concurrent use.
type Store struct {
	shards    []*shard
	nShards   uint32
	sweepers  []*shardSweeper
	st        *stats
	closeOnce sync.Once
}

// New creates a Store with the given config.
func New(cfg Config) *Store {
	n := cfg.shards()
	st := &stats{}
	shards := make([]*shard, n)
	sweepers := make([]*shardSweeper, n)
	interval := cfg.sweepInterval()

	for i := 0; i < n; i++ {
		sh := newShard(cfg.MaxEntries, cfg.MaxBytes, st)
		shards[i] = sh
		sweepers[i] = startSweeper(sh, interval)
	}
	return &Store{
		shards:   shards,
		nShards:  uint32(n),
		sweepers: sweepers,
		st:       st,
	}
}

// Close stops all background goroutines. The Store must not be used
// after Close returns.
func (s *Store) Close() {
	s.closeOnce.Do(func() {
		for _, sw := range s.sweepers {
			sw.Shutdown()
		}
	})
}

// shard returns the shard responsible for key.
func (s *Store) shard(key string) *shard {
	return s.shards[fnv32a(key)%s.nShards]
}

func fnv32a(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// Get retrieves the value for key.
// Returns ErrKeyNotFound if the key does not exist or is expired.
func (s *Store) Get(key string) ([]byte, error) {
	val, ok := s.shard(key).get(key, time.Now().UnixNano())
	if !ok {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

// Put stores value under key with an optional TTL (0 = no expiry).
func (s *Store) Put(key string, value []byte, ttl time.Duration) {
	s.shard(key).put(key, value, ttl)
}

// Delete removes key. Returns true if the key existed.
func (s *Store) Delete(key string) bool {
	return s.shard(key).delete(key)
}

// Exists reports whether key exists and is not expired.
func (s *Store) Exists(key string) bool {
	return s.shard(key).exists(key, time.Now().UnixNano())
}

// TTL returns the remaining time-to-live for key.
//   - Returns -1 and nil if key has no TTL.
//   - Returns -2 and ErrKeyNotFound if key does not exist or is expired.
func (s *Store) TTL(key string) (time.Duration, error) {
	d, ok := s.shard(key).ttlRemaining(key, time.Now())
	if !ok {
		return -2, ErrKeyNotFound
	}
	return d, nil
}

// Keys returns all non-expired keys with the given prefix.
// An empty prefix returns all keys.
func (s *Store) Keys(prefix string) []string {
	now := time.Now().UnixNano()
	// Fan out across shards concurrently.
	type result struct {
		keys []string
	}
	results := make([]result, s.nShards)
	var wg sync.WaitGroup
	wg.Add(int(s.nShards))
	for i := uint32(0); i < s.nShards; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i].keys = s.shards[i].keys(prefix, now)
		}()
	}
	wg.Wait()

	var out []string
	for _, r := range results {
		out = append(out, r.keys...)
	}
	return out
}

// Stats returns a snapshot of store counters.
func (s *Store) Stats() StoreStats {
	now := time.Now().UnixNano()
	var keys uint64
	var bytes uint64
	for _, sh := range s.shards {
		keys += uint64(sh.len(now))
		bytes += uint64(sh.bytes(now))
	}
	return StoreStats{
		Hits:      s.st.hits.Load(),
		Misses:    s.st.misses.Load(),
		Evictions: s.st.evictions.Load(),
		Expireds:  s.st.expireds.Load(),
		Keys:      keys,
		Bytes:     bytes,
	}
}
