package lru

import (
	"testing"
)

// --- helpers ----------------------------------------------------------------

func newLRU(maxEntries int, maxBytes int64) *LRU {
	return New(maxEntries, maxBytes)
}

func mustGet(t *testing.T, l *LRU, key string) []byte {
	t.Helper()
	v, ok := l.Get(key)
	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}
	return v
}

func mustMiss(t *testing.T, l *LRU, key string) {
	t.Helper()
	_, ok := l.Get(key)
	if ok {
		t.Fatalf("expected key %q to be absent", key)
	}
}

// --- tests ------------------------------------------------------------------

func TestLRU_BasicPutGet(t *testing.T) {
	l := newLRU(10, 0)
	l.Put("a", []byte("hello"))
	v := mustGet(t, l, "a")
	if string(v) != "hello" {
		t.Fatalf("got %q, want %q", v, "hello")
	}
}

func TestLRU_Update(t *testing.T) {
	l := newLRU(10, 0)
	l.Put("a", []byte("v1"))
	l.Put("a", []byte("v2"))
	if l.Len() != 1 {
		t.Fatalf("expected len 1, got %d", l.Len())
	}
	if string(mustGet(t, l, "a")) != "v2" {
		t.Fatal("expected v2")
	}
}

func TestLRU_Remove(t *testing.T) {
	l := newLRU(10, 0)
	l.Put("a", []byte("v"))
	if !l.Remove("a") {
		t.Fatal("expected Remove to return true")
	}
	mustMiss(t, l, "a")
	if l.Remove("a") {
		t.Fatal("expected second Remove to return false")
	}
}

func TestLRU_EvictionByCount(t *testing.T) {
	evicted := map[string]bool{}
	l := newLRU(3, 0)
	l.OnEvict = func(key string, _ []byte) { evicted[key] = true }

	l.Put("a", []byte("1"))
	l.Put("b", []byte("2"))
	l.Put("c", []byte("3"))
	// Access "a" so it becomes most-recently-used; "b" is now LRU tail.
	l.Get("a")
	// Insert "d" — should evict "b" (LRU tail).
	l.Put("d", []byte("4"))

	if !evicted["b"] {
		t.Fatal("expected 'b' to be evicted")
	}
	mustMiss(t, l, "b")
	mustGet(t, l, "a")
	mustGet(t, l, "c")
	mustGet(t, l, "d")
}

func TestLRU_EvictionByBytes(t *testing.T) {
	evicted := []string{}
	// Max 10 bytes.
	l := newLRU(0, 10)
	l.OnEvict = func(key string, _ []byte) { evicted = append(evicted, key) }

	l.Put("a", []byte("12345")) // 5 bytes
	l.Put("b", []byte("67890")) // 5 bytes — total 10, OK
	l.Put("c", []byte("x"))     // 1 byte  — total 11, must evict "a"

	if len(evicted) == 0 {
		t.Fatal("expected at least one eviction")
	}
	if evicted[0] != "a" {
		t.Fatalf("expected 'a' evicted first, got %q", evicted[0])
	}
	if l.Bytes() > 10 {
		t.Fatalf("bytes %d exceeds max 10 after eviction", l.Bytes())
	}
}

func TestLRU_EvictionByBytes_UpdateReduces(t *testing.T) {
	l := newLRU(0, 20)
	l.Put("a", []byte("1234567890")) // 10 bytes
	l.Put("b", []byte("1234567890")) // 10 bytes — total 20
	// Shrink "a" — total should drop.
	l.Put("a", []byte("hi")) // 2 bytes — total 12
	if l.Bytes() != 12 {
		t.Fatalf("expected 12 bytes, got %d", l.Bytes())
	}
}

func TestLRU_OnEvictCallback(t *testing.T) {
	called := 0
	l := newLRU(2, 0)
	l.OnEvict = func(_ string, _ []byte) { called++ }

	l.Put("a", []byte("1"))
	l.Put("b", []byte("2"))
	l.Put("c", []byte("3")) // evicts one
	l.Put("d", []byte("4")) // evicts another

	if called != 2 {
		t.Fatalf("expected 2 evictions, got %d", called)
	}
}

func TestLRU_Keys_Order(t *testing.T) {
	l := newLRU(5, 0)
	l.Put("a", nil)
	l.Put("b", nil)
	l.Put("c", nil)
	l.Get("a") // promote "a" to front

	keys := l.Keys()
	if keys[0] != "a" {
		t.Fatalf("expected 'a' at front, got %q", keys[0])
	}
}

func TestLRU_Len(t *testing.T) {
	l := newLRU(100, 0)
	for i := 0; i < 50; i++ {
		l.Put(string(rune('a'+i%26))+string(rune('0'+i/26)), []byte("v"))
	}
	// All 50 distinct keys inserted (they're all different).
	if l.Len() == 0 {
		t.Fatal("Len should be non-zero")
	}
}

func TestLRU_Empty(t *testing.T) {
	l := newLRU(10, 0)
	mustMiss(t, l, "missing")
	if l.Remove("missing") {
		t.Fatal("Remove on empty should return false")
	}
}

// --- benchmarks -------------------------------------------------------------

func BenchmarkLRU_Put(b *testing.B) {
	l := New(b.N+1, 0)
	val := []byte("benchvalue")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Put(string(rune('a'+i%26)), val)
	}
}

func BenchmarkLRU_Get(b *testing.B) {
	l := New(1000, 0)
	val := []byte("benchvalue")
	for i := 0; i < 1000; i++ {
		l.Put(string(rune('a'+i%26)), val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Get(string(rune('a' + i%26)))
	}
}
