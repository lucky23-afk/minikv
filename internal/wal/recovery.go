package wal

import (
	"fmt"
	"path/filepath"
	"time"
)

// RecoveredState is the result of replaying a snapshot plus WAL.
type RecoveredState struct {
	// Entries are the live key-value pairs after recovery.
	Entries []SnapshotEntry
	// LastSeq is the highest sequence number seen in the WAL.
	LastSeq uint64
	// SnapshotPath is the path of the snapshot file that was loaded,
	// or empty if no snapshot existed.
	SnapshotPath string
}

// Recover loads the latest snapshot from dir and then replays any WAL
// records with Seq > snapshot's last seq to produce a merged state.
//
// TTL-expired entries (ExpiresAt != 0 && ExpiresAt <= now) are dropped
// during recovery.
func Recover(dir string) (*RecoveredState, error) {
	now := time.Now().UnixNano()

	// 1. Load snapshot.
	snapEntries, snapPath, err := ReadSnapshot(dir)
	if err != nil {
		return nil, fmt.Errorf("wal.Recover snapshot: %w", err)
	}

	// Build a map from snapshot entries.
	table := make(map[string]*SnapshotEntry, len(snapEntries))
	for i := range snapEntries {
		e := &snapEntries[i]
		if e.ExpiresAt != 0 && e.ExpiresAt <= now {
			continue // drop expired entries on recovery
		}
		table[e.Key] = e
	}

	// 2. Replay WAL.
	walPath := filepath.Join(dir, "wal.log")
	records, err := ReadAll(walPath)
	if err != nil {
		return nil, fmt.Errorf("wal.Recover wal: %w", err)
	}

	var lastSeq uint64
	for _, r := range records {
		if r.Seq > lastSeq {
			lastSeq = r.Seq
		}
		switch r.Type {
		case RecordPut:
			if r.ExpiresAt != 0 && r.ExpiresAt <= now {
				delete(table, r.Key) // already expired
				continue
			}
			e := &SnapshotEntry{Key: r.Key, Value: r.Value, ExpiresAt: r.ExpiresAt}
			table[r.Key] = e
		case RecordDelete:
			delete(table, r.Key)
		default:
			return nil, fmt.Errorf("wal.Recover unknown record type %d", r.Type)
		}
	}

	// 3. Flatten to slice.
	entries := make([]SnapshotEntry, 0, len(table))
	for _, e := range table {
		entries = append(entries, *e)
	}

	return &RecoveredState{
		Entries:      entries,
		LastSeq:      lastSeq,
		SnapshotPath: snapPath,
	}, nil
}
