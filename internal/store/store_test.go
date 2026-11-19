package store

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// newTestStore returns a Store with tight limits for testing.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s := New(Config{
		NumShards:     16,
		SweepInterval: 10 * time.Millisecond,
	})
	t.Cleanup(s.Close)
	return s
}

// --- basic operations -------------------------------------------------------

func TestStore_PutGet(t *testing.T) {
	s := newTestStore(t)
	s.Put("hello", []byte("world"), 0)
	v, err := s.Get("hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(v) != "world" {
		t.Fatalf("got %q, want %q", v, "world")
	}
}

func TestStore_GetMissing(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get("nope")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestStore_PutOverwrite(t *testing.T) {
	s := newTestStore(t)
	s.Put("k", []byte("v1"), 0)
	s.Put("k", []byte("v2"), 0)
	v, _ := s.Get("k")
	if string(v) != "v2" {
		t.Fatalf("expected v2, got %q", v)
	}
}

func TestStore_Delete(t *testing.T) {
	s := newTestStore(t)
	s.Put("k", []byte("v"), 0)
	if !s.Delete("k") {
		t.Fatal("Delete should return true for existing key")
	}
	if s.Delete("k") {
		t.Fatal("Delete should return false for missing key")
	}
	_, err := s.Get("k")
	if err != ErrKeyNotFound {
		t.Fatal("key should be gone after Delete")
	}
}

func TestStore_Exists(t *testing.T) {
	s := newTestStore(t)
	s.Put("k", []byte("v"), 0)
	if !s.Exists("k") {
		t.Fatal("Exists should return true")
	}
	s.Delete("k")
	if s.Exists("k") {
		t.Fatal("Exists should return false after Delete")
	}
}

// --- TTL tests --------------------------------------------------------------

func TestStore_TTL_LazyExpiry(t *testing.T) {
	s := newTestStore(t)
	s.Put("x", []byte("val"), 50*time.Millisecond)

	// Should be present immediately.
	if !s.Exists("x") {
		t.Fatal("key should exist before TTL")
	}

	time.Sleep(100 * time.Millisecond)

	// Lazy expiry: Get should report not-found.
	_, err := s.Get("x")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after TTL, got %v", err)
	}
}

func TestStore_TTL_ActiveSweeper(t *testing.T) {
	s := newTestStore(t)
	s.Put("sweep-me", []byte("val"), 20*time.Millisecond)

	// Wait longer than TTL and sweep interval.
	time.Sleep(100 * time.Millisecond)

	// Even without a Get, the sweeper should have removed it.
	stats := s.Stats()
	if stats.Expireds == 0 {
		t.Fatal("sweeper should have incremented expireds counter")
	}
}

func TestStore_TTL_Remaining(t *testing.T) {
	s := newTestStore(t)
	s.Put("ttlkey", []byte("v"), 2*time.Second)
	d, err := s.TTL("ttlkey")
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if d <= 0 || d > 2*time.Second {
		t.Fatalf("unexpected TTL remaining: %v", d)
	}
}

func TestStore_TTL_NoTTL(t *testing.T) {
	s := newTestStore(t)
	s.Put("persistent", []byte("v"), 0)
	d, err := s.TTL("persistent")
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if d != -1 {
		t.Fatalf("expected -1 for no TTL, got %v", d)
	}
}

func TestStore_TTL_Missing(t *testing.T) {
	s := newTestStore(t)
	d, err := s.TTL("missing")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
	if d != -2 {
		t.Fatalf("expected -2 for missing key, got %v", d)
	}
}

func TestStore_TTL_ExpiredKey(t *testing.T) {
	s := newTestStore(t)
	s.Put("expired", []byte("v"), 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	_, err := s.TTL("expired")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for expired key, got %v", err)
	}
}

// --- KEYS prefix scan -------------------------------------------------------

func TestStore_Keys_All(t *testing.T) {
	s := newTestStore(t)
	want := []string{"apple", "banana", "cherry"}
	for _, k := range want {
		s.Put(k, []byte("v"), 0)
	}
	got := s.Keys("")
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, k := range want {
		if got[i] != k {
			t.Fatalf("got[%d]=%q, want %q", i, got[i], k)
		}
	}
}

func TestStore_Keys_Prefix(t *testing.T) {
	s := newTestStore(t)
	keys := []string{"user:1", "user:2", "product:1", "user:3"}
	for _, k := range keys {
		s.Put(k, []byte("v"), 0)
	}
	got := s.Keys("user:")
	for _, k := range got {
		if !strings.HasPrefix(k, "user:") {
			t.Fatalf("key %q does not have prefix user:", k)
		}
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 user: keys, got %d", len(got))
	}
}

func TestStore_Keys_ExcludesExpired(t *testing.T) {
	s := newTestStore(t)
	s.Put("live", []byte("v"), 0)
	s.Put("dead", []byte("v"), 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	got := s.Keys("")
	for _, k := range got {
		if k == "dead" {
			t.Fatal("KEYS should not return expired keys")
		}
	}
}

// --- STATS ------------------------------------------------------------------

func TestStore_Stats_HitMiss(t *testing.T) {
	s := newTestStore(t)
	s.Put("k", []byte("v"), 0)
	s.Get("k")        // hit
	s.Get("missing")  // miss
	s.Get("missing2") // miss

	st := s.Stats()
	if st.Hits < 1 {
		t.Fatalf("expected hits >= 1, got %d", st.Hits)
	}
	if st.Misses < 2 {
		t.Fatalf("expected misses >= 2, got %d", st.Misses)
	}
}

func TestStore_Stats_Keys(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 10; i++ {
		s.Put(fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	st := s.Stats()
	if st.Keys != 10 {
		t.Fatalf("expected 10 keys, got %d", st.Keys)
	}
}

func TestStore_Stats_Bytes(t *testing.T) {
	s := newTestStore(t)
	s.Put("k", []byte("hello"), 0) // 5 bytes
	st := s.Stats()
	if st.Bytes < 5 {
		t.Fatalf("expected bytes >= 5, got %d", st.Bytes)
	}
}

func TestStore_Stats_Evictions(t *testing.T) {
	s := New(Config{
		NumShards:  1,
		MaxEntries: 2,
	})
	defer s.Close()

	s.Put("a", []byte("1"), 0)
	s.Put("b", []byte("2"), 0)
	s.Put("c", []byte("3"), 0) // should evict one entry

	st := s.Stats()
	if st.Evictions == 0 {
		t.Fatal("expected at least one eviction")
	}
}

// --- LRU integration --------------------------------------------------------

func TestStore_LRU_Eviction(t *testing.T) {
	// 1 shard, max 3 entries — easy to predict which key is evicted.
	s := New(Config{NumShards: 1, MaxEntries: 3})
	defer s.Close()

	s.Put("a", []byte("1"), 0)
	s.Put("b", []byte("2"), 0)
	s.Put("c", []byte("3"), 0)
	// Access "a" so it is MRU; LRU tail is "b".
	s.Get("a")
	// Insert "d" — should evict "b".
	s.Put("d", []byte("4"), 0)

	_, err := s.Get("b")
	if err != ErrKeyNotFound {
		t.Fatal("expected 'b' to have been evicted")
	}
	// a, c, d should still be present.
	for _, k := range []string{"a", "c", "d"} {
		if _, err := s.Get(k); err != nil {
			t.Fatalf("expected %q to exist, got %v", k, err)
		}
	}
}

// --- concurrency / race detector --------------------------------------------

func TestStore_ConcurrentReadWrite(t *testing.T) {
	s := newTestStore(t)
	const goroutines = 50
	const ops = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("key-%d-%d", i, j%10)
				s.Put(key, []byte("value"), 50*time.Millisecond)
				s.Get(key)
				s.Exists(key)
				if j%5 == 0 {
					s.Delete(key)
				}
			}
		}()
	}
	wg.Wait()
}

func TestStore_ConcurrentStats(t *testing.T) {
	s := newTestStore(t)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Stats()
			}
		}()
	}
	wg.Wait()
}

// --- benchmarks -------------------------------------------------------------

func BenchmarkStore_Get(b *testing.B) {
	s := New(Config{NumShards: 256})
	defer s.Close()
	s.Put("bench", []byte("value"), 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("bench")
		}
	})
}

func BenchmarkStore_Put(b *testing.B) {
	s := New(Config{NumShards: 256})
	defer s.Close()
	val := []byte("benchvalue")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Put(fmt.Sprintf("k%d", i), val, 0)
			i++
		}
	})
}

func BenchmarkStore_Keys(b *testing.B) {
	s := New(Config{NumShards: 256})
	defer s.Close()
	for i := 0; i < 10000; i++ {
		s.Put(fmt.Sprintf("user:%d", i), []byte("v"), 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Keys("user:")
	}
}
