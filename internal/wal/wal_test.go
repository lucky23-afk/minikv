package wal

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// helpers --------------------------------------------------------------------

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func openWAL(t *testing.T, dir string, policy FsyncPolicy) *WAL {
	t.Helper()
	w, err := Open(dir, policy, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func putRec(key string, value []byte, ttl time.Duration) *Record {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}
	return &Record{Type: RecordPut, Key: key, Value: value, ExpiresAt: expiresAt}
}

func delRec(key string) *Record {
	return &Record{Type: RecordDelete, Key: key}
}

// --- tests ------------------------------------------------------------------

func TestWAL_AppendAndReadAll(t *testing.T) {
	dir := tempDir(t)
	w := openWAL(t, dir, FsyncNone)

	recs := []*Record{
		putRec("a", []byte("v1"), 0),
		putRec("b", []byte("v2"), 0),
		delRec("a"),
	}
	for _, r := range recs {
		if _, err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := ReadAll(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 records, got %d", len(got))
	}
	if got[0].Key != "a" || string(got[0].Value) != "v1" {
		t.Fatalf("unexpected record[0]: %+v", got[0])
	}
	if got[2].Type != RecordDelete || got[2].Key != "a" {
		t.Fatalf("unexpected record[2]: %+v", got[2])
	}
}

func TestWAL_Seq_Monotonic(t *testing.T) {
	dir := tempDir(t)
	w := openWAL(t, dir, FsyncNone)

	var prev uint64
	for i := 0; i < 10; i++ {
		seq, err := w.Append(putRec("k", []byte("v"), 0))
		if err != nil {
			t.Fatal(err)
		}
		if seq <= prev {
			t.Fatalf("seq %d not monotonically increasing after %d", seq, prev)
		}
		prev = seq
	}
}

func TestWAL_CRC32_Detection(t *testing.T) {
	dir := tempDir(t)
	w := openWAL(t, dir, FsyncNone)
	_, _ = w.Append(putRec("key", []byte("value"), 0))
	w.Close()

	// Corrupt one payload byte (offset 9 is inside the payload).
	path := filepath.Join(dir, "wal.log")
	data, _ := os.ReadFile(path)
	data[9] ^= 0xFF
	os.WriteFile(path, data, 0o644)

	_, err := ReadAll(path)
	if err == nil {
		t.Fatal("expected CRC error, got nil")
	}
}

func TestWAL_TornRecord(t *testing.T) {
	dir := tempDir(t)
	w := openWAL(t, dir, FsyncNone)

	// Write 10 complete records.
	for i := 0; i < 10; i++ {
		_, _ = w.Append(putRec("k", []byte("value"), 0))
	}
	w.Close()

	path := filepath.Join(dir, "wal.log")
	data, _ := os.ReadFile(path)
	fullLen := len(data)

	// Truncate at a random point in the last record (but not at a clean boundary).
	// Each record is 1+2+1+4+5+8+8=29 bytes payload → frame = 8+29+4 = 41 bytes.
	// Truncate somewhere in the last 41 bytes.
	truncAt := fullLen - rand.Intn(40) - 1
	if err := os.Truncate(path, int64(truncAt)); err != nil {
		t.Fatal(err)
	}

	recs, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll with torn record: %v", err)
	}
	// Should have 9 complete records (the torn 10th is dropped).
	if len(recs) != 9 {
		t.Fatalf("expected 9 records after torn tail, got %d", len(recs))
	}
	// File should have been truncated at the clean boundary.
	fi, _ := os.Stat(path)
	if int(fi.Size()) >= truncAt {
		t.Fatalf("expected file truncated to clean boundary, size=%d truncAt=%d", fi.Size(), truncAt)
	}
}

func TestWAL_FsyncNone(t *testing.T) {
	dir := tempDir(t)
	w := openWAL(t, dir, FsyncNone)
	for i := 0; i < 100; i++ {
		if _, err := w.Append(putRec("k", []byte("v"), 0)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWAL_FsyncAlways(t *testing.T) {
	dir := tempDir(t)
	w := openWAL(t, dir, FsyncAlways)
	for i := 0; i < 5; i++ {
		if _, err := w.Append(putRec("k", []byte("v"), 0)); err != nil {
			t.Fatalf("FsyncAlways Append: %v", err)
		}
	}
}

func TestWAL_FsyncEverySecond(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, FsyncEverySecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := w.Append(putRec("k", []byte("v"), 0)); err != nil {
			t.Fatal(err)
		}
	}
	w.Close() // must not hang
}

// --- Snapshot tests ---------------------------------------------------------

func TestSnapshot_RoundTrip(t *testing.T) {
	dir := tempDir(t)
	entries := []SnapshotEntry{
		{Key: "apple", Value: []byte("red"), ExpiresAt: 0},
		{Key: "banana", Value: []byte("yellow"), ExpiresAt: 999},
	}
	path, err := WriteSnapshot(dir, entries)
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	got, _, err := ReadSnapshot(dir)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	_ = path
}

func TestSnapshot_CRC_Mismatch(t *testing.T) {
	dir := tempDir(t)
	_, err := WriteSnapshot(dir, []SnapshotEntry{{Key: "k", Value: []byte("v")}})
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.snap"))
	data, _ := os.ReadFile(files[0])
	data[10] ^= 0xFF
	os.WriteFile(files[0], data, 0o644)

	_, _, err = ReadSnapshot(dir)
	if err == nil {
		t.Fatal("expected CRC error")
	}
}

func TestSnapshot_TTL_Survives_Restart(t *testing.T) {
	dir := tempDir(t)

	// Write a snapshot with an entry that has a future TTL.
	future := time.Now().Add(10 * time.Second).UnixNano()
	entries := []SnapshotEntry{
		{Key: "ttlkey", Value: []byte("val"), ExpiresAt: future},
	}
	_, err := WriteSnapshot(dir, entries)
	if err != nil {
		t.Fatal(err)
	}

	// Recover: TTL should still be intact.
	state, err := Recover(dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	found := false
	for _, e := range state.Entries {
		if e.Key == "ttlkey" {
			found = true
			if e.ExpiresAt != future {
				t.Fatalf("ExpiresAt got %d, want %d", e.ExpiresAt, future)
			}
		}
	}
	if !found {
		t.Fatal("ttlkey not found after recovery")
	}
}

func TestSnapshot_ExpiredDroppedOnRecovery(t *testing.T) {
	dir := tempDir(t)

	// Write a snapshot with an already-expired entry.
	past := time.Now().Add(-1 * time.Second).UnixNano()
	_, err := WriteSnapshot(dir, []SnapshotEntry{
		{Key: "expired", Value: []byte("v"), ExpiresAt: past},
		{Key: "live", Value: []byte("v"), ExpiresAt: 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	state, err := Recover(dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for _, e := range state.Entries {
		if e.Key == "expired" {
			t.Fatal("expired key should not appear after recovery")
		}
	}
}

// --- Recovery tests ---------------------------------------------------------

func TestWAL_Recovery_ReplayAfterSnapshot(t *testing.T) {
	dir := tempDir(t)

	// Write a snapshot with key "a".
	_, err := WriteSnapshot(dir, []SnapshotEntry{{Key: "a", Value: []byte("snap")}})
	if err != nil {
		t.Fatal(err)
	}

	// Write WAL: update "a", add "b", delete "a".
	w, err := Open(dir, FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(putRec("a", []byte("wal"), 0))
	w.Append(putRec("b", []byte("new"), 0))
	w.Append(delRec("a"))
	w.Close()

	state, err := Recover(dir)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	table := make(map[string]string)
	for _, e := range state.Entries {
		table[e.Key] = string(e.Value)
	}

	if _, ok := table["a"]; ok {
		t.Fatal("key 'a' should have been deleted")
	}
	if table["b"] != "new" {
		t.Fatalf("key 'b' got %q, want 'new'", table["b"])
	}
	if state.LastSeq != 3 {
		t.Fatalf("LastSeq got %d, want 3", state.LastSeq)
	}
}

func TestWAL_Recovery_CrashSimulation(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write 1000 records.
	const total = 1000
	for i := 0; i < total; i++ {
		_, _ = w.Append(putRec("k", []byte("v"), 0))
	}
	w.Close()

	// Simulate crash: truncate the WAL at a random point in the last record.
	path := filepath.Join(dir, "wal.log")
	data, _ := os.ReadFile(path)
	fullLen := len(data)
	truncAt := int64(fullLen) - int64(rand.Intn(40)+1)
	if truncAt < 0 {
		truncAt = 0
	}
	os.Truncate(path, truncAt)

	state, err := Recover(dir)
	if err != nil {
		t.Fatalf("Recover after crash: %v", err)
	}
	// Should have recovered at least 998 complete records (the torn
	// record may be the last or second-to-last depending on truncation).
	if state.LastSeq < total-2 || state.LastSeq >= total {
		t.Fatalf("expected LastSeq in [%d, %d), got %d", total-2, total, state.LastSeq)
	}
}

func TestSnapshot_DeleteOldSnapshots(t *testing.T) {
	dir := tempDir(t)
	// Write three snapshots.
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Millisecond) // ensure distinct timestamps
		_, err := WriteSnapshot(dir, []SnapshotEntry{{Key: "k", Value: []byte("v")}})
		if err != nil {
			t.Fatal(err)
		}
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.snap"))
	if len(files) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(files))
	}
	if err := DeleteOldSnapshots(dir); err != nil {
		t.Fatal(err)
	}
	files, _ = filepath.Glob(filepath.Join(dir, "*.snap"))
	if len(files) != 1 {
		t.Fatalf("expected 1 snapshot after cleanup, got %d", len(files))
	}
}

func TestSnapshot_NoSnapshotDir(t *testing.T) {
	dir := tempDir(t)
	entries, path, err := ReadSnapshot(filepath.Join(dir, "nonexistent"))
	if err != nil {
		t.Fatal(err)
	}
	if entries != nil || path != "" {
		t.Fatal("expected nil entries and empty path when no snapshot exists")
	}
}

func TestWAL_ReadAll_EmptyFile(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "wal.log")
	os.WriteFile(path, nil, 0o644)
	recs, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll empty: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records, got %d", len(recs))
	}
}

func TestWAL_ReadAll_NonExistent(t *testing.T) {
	dir := tempDir(t)
	recs, err := ReadAll(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("ReadAll nonexistent: %v", err)
	}
	if recs != nil {
		t.Fatal("expected nil for nonexistent file")
	}
}

func TestWAL_Recovery_EmptyDir(t *testing.T) {
	dir := tempDir(t)
	state, err := Recover(dir)
	if err != nil {
		t.Fatalf("Recover empty dir: %v", err)
	}
	if len(state.Entries) != 0 || state.LastSeq != 0 {
		t.Fatalf("unexpected state from empty dir: %+v", state)
	}
}

func TestWAL_StartSeq(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, FsyncNone, 100)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := w.Append(putRec("k", []byte("v"), 0))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	if seq != 101 {
		t.Fatalf("expected seq 101 (startSeq 100 + 1), got %d", seq)
	}
}

func TestWAL_ReadAll_BadMagic(t *testing.T) {
	dir := tempDir(t)
	w := openWAL(t, dir, FsyncNone)
	w.Append(putRec("k", []byte("v"), 0))
	w.Close()

	path := filepath.Join(dir, "wal.log")
	data, _ := os.ReadFile(path)
	// Zero out the magic bytes.
	data[0] = 0x00
	data[1] = 0x00
	data[2] = 0x00
	data[3] = 0x00
	os.WriteFile(path, data, 0o644)

	_, err := ReadAll(path)
	if err == nil {
		t.Fatal("expected bad magic error")
	}
}

func TestWAL_Recovery_DeleteRecord(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(putRec("x", []byte("val"), 0))
	w.Append(delRec("x"))
	w.Close()

	state, err := Recover(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range state.Entries {
		if e.Key == "x" {
			t.Fatal("deleted key 'x' should not appear in recovered state")
		}
	}
}

func TestWAL_Recovery_ExpiredWALEntry(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, FsyncNone, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Write a WAL entry that is already expired.
	past := time.Now().Add(-time.Second).UnixNano()
	w.Append(&Record{
		Type:      RecordPut,
		Key:       "gone",
		Value:     []byte("v"),
		ExpiresAt: past,
	})
	w.Append(putRec("live", []byte("v"), 0))
	w.Close()

	state, err := Recover(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range state.Entries {
		if e.Key == "gone" {
			t.Fatal("expired WAL entry should not appear in recovered state")
		}
	}
	found := false
	for _, e := range state.Entries {
		if e.Key == "live" {
			found = true
		}
	}
	if !found {
		t.Fatal("live key not found after recovery")
	}
}

func TestSnapshot_EmptyEntries(t *testing.T) {
	dir := tempDir(t)
	_, err := WriteSnapshot(dir, nil)
	if err != nil {
		t.Fatalf("WriteSnapshot with nil entries: %v", err)
	}
	got, _, err := ReadSnapshot(dir)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(got))
	}
}

func TestSnapshot_DeleteNoSnapshots(t *testing.T) {
	dir := tempDir(t)
	// Should be a no-op when no snapshots exist.
	if err := DeleteOldSnapshots(dir); err != nil {
		t.Fatal(err)
	}
}

// --- Benchmarks -------------------------------------------------------------

func BenchmarkWAL_Append(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-bench-*")
	defer os.RemoveAll(dir)
	w, _ := Open(dir, FsyncNone, 0)
	defer w.Close()
	rec := putRec("benchkey", []byte("benchvalue"), 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Seq = 0 // reset so Append assigns a new one
		w.Append(rec)
	}
}
