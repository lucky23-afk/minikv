package store

import "sync/atomic"

// StoreStats is a point-in-time snapshot of store counters.
type StoreStats struct {
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Evictions uint64 `json:"evictions"`
	Expireds  uint64 `json:"expireds"`
	Keys      uint64 `json:"keys"`
	Bytes     uint64 `json:"bytes"`
}

// stats holds atomic counters for the whole store.
type stats struct {
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	expireds  atomic.Uint64
}

func (s *stats) hit()      { s.hits.Add(1) }
func (s *stats) miss()     { s.misses.Add(1) }
func (s *stats) eviction() { s.evictions.Add(1) }
func (s *stats) expired()  { s.expireds.Add(1) }
