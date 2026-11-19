// Package store provides a sharded, in-memory key-value store with
// LRU eviction, TTL expiry, and atomic statistics.
package store

import "time"

// Entry is one value slot in the store.
type Entry struct {
	Value     []byte
	ExpiresAt int64 // Unix nanoseconds; 0 = no expiry
	Size      int64 // len(Value)
}

// IsExpired reports whether the entry has expired at time t.
func (e *Entry) IsExpired(now int64) bool {
	return e.ExpiresAt != 0 && now >= e.ExpiresAt
}

// TTLRemaining returns the duration until expiry given the current
// time. Returns -1 if the entry has no TTL, 0 if it has already expired.
func (e *Entry) TTLRemaining(now time.Time) time.Duration {
	if e.ExpiresAt == 0 {
		return -1
	}
	d := time.Duration(e.ExpiresAt - now.UnixNano())
	if d < 0 {
		return 0
	}
	return d
}
