// Package wal provides an append-only Write-Ahead Log with per-record
// CRC32 integrity checks, configurable fsync policies, periodic snapshots,
// and crash recovery.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RecordType identifies the kind of WAL entry.
type RecordType uint8

const (
	RecordPut    RecordType = 1
	RecordDelete RecordType = 2
)

// magic is the 4-byte header written before every record.
const magic uint32 = 0xABCD1234

// Record is one WAL entry.
type Record struct {
	Type      RecordType
	Key       string
	Value     []byte
	ExpiresAt int64  // Unix nanoseconds; 0 = no expiry
	Seq       uint64 // monotonic sequence number
}

// FsyncPolicy controls when the WAL calls fsync.
type FsyncPolicy int

const (
	// FsyncAlways calls fsync after every Append.
	FsyncAlways FsyncPolicy = iota
	// FsyncEverySecond calls fsync on a 1-second ticker.
	FsyncEverySecond
	// FsyncNone never calls fsync explicitly.
	FsyncNone
)

// ErrTornRecord is returned when the final record in a WAL file is
// truncated (torn write). The caller should truncate at that point and
// continue.
var ErrTornRecord = errors.New("wal: torn record at end of file")

// WAL is an append-only log file. It is safe for concurrent use.
type WAL struct {
	dir    string
	path   string
	f      *os.File
	mu     sync.Mutex
	seq    uint64
	policy FsyncPolicy

	// for FsyncEverySecond
	stopSync chan struct{}
	syncDone sync.WaitGroup
}

// Open opens or creates a WAL in dir. Seq is the first sequence number
// to use (typically 1 + lastAppliedSeq from recovery).
func Open(dir string, policy FsyncPolicy, startSeq uint64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal.Open mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal.Open %s: %w", path, err)
	}

	w := &WAL{
		dir:    dir,
		path:   path,
		f:      f,
		seq:    startSeq,
		policy: policy,
	}

	if policy == FsyncEverySecond {
		w.stopSync = make(chan struct{})
		w.syncDone.Add(1)
		go func() {
			defer w.syncDone.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-w.stopSync:
					return
				case <-ticker.C:
					w.mu.Lock()
					_ = w.f.Sync()
					w.mu.Unlock()
				}
			}
		}()
	}

	return w, nil
}

// Append writes rec to the WAL and returns its assigned sequence number.
func (w *WAL) Append(rec *Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	rec.Seq = w.seq

	payload := encodeRecord(rec)

	// Frame: [magic:4][len:4][payload:N][crc32:4]
	var frame [8]byte
	binary.LittleEndian.PutUint32(frame[0:4], magic)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(payload)))

	crc := crc32.NewIEEE()
	crc.Write(frame[4:8])
	crc.Write(payload)
	checksum := crc.Sum32()

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], checksum)

	if _, err := w.f.Write(frame[:]); err != nil {
		return 0, fmt.Errorf("wal.Append write frame: %w", err)
	}
	if _, err := w.f.Write(payload); err != nil {
		return 0, fmt.Errorf("wal.Append write payload: %w", err)
	}
	if _, err := w.f.Write(crcBuf[:]); err != nil {
		return 0, fmt.Errorf("wal.Append write crc: %w", err)
	}

	if w.policy == FsyncAlways {
		if err := w.f.Sync(); err != nil {
			return 0, fmt.Errorf("wal.Append fsync: %w", err)
		}
	}

	return w.seq, nil
}

// Seq returns the last sequence number assigned.
func (w *WAL) Seq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// Truncate clears the WAL file and resets seq to newStartSeq.
func (w *WAL) Truncate(newStartSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.f.Close()
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	w.seq = newStartSeq
	return nil
}

// Close flushes (if needed) and closes the WAL file.
func (w *WAL) Close() error {
	if w.policy == FsyncEverySecond && w.stopSync != nil {
		close(w.stopSync)
		w.syncDone.Wait()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.policy != FsyncNone {
		_ = w.f.Sync()
	}
	return w.f.Close()
}

// ReadAll reads all complete records from path, skipping a torn final
// record gracefully. It truncates the file at the boundary of the last
// complete record so future appends do not corrupt the log.
func ReadAll(path string) ([]*Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal.ReadAll open %s: %w", path, err)
	}

	var records []*Record
	pos := 0
	goodEnd := 0

	for pos < len(data) {
		// Need at least 8 bytes for frame header.
		if pos+8 > len(data) {
			// Torn header — truncate here.
			break
		}

		mg := binary.LittleEndian.Uint32(data[pos : pos+4])
		if mg != magic {
			return nil, fmt.Errorf("wal.ReadAll at offset %d: bad magic 0x%08X", pos, mg)
		}
		payLen := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))

		end := pos + 8 + payLen + 4
		if end > len(data) {
			// Torn record — stop reading, truncate.
			break
		}

		payload := data[pos+8 : pos+8+payLen]
		storedCRC := binary.LittleEndian.Uint32(data[pos+8+payLen : end])

		crc := crc32.NewIEEE()
		crc.Write(data[pos+4 : pos+8]) // length bytes
		crc.Write(payload)
		if crc.Sum32() != storedCRC {
			return nil, fmt.Errorf("wal.ReadAll at offset %d: CRC mismatch", pos)
		}

		rec, err := decodeRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("wal.ReadAll decode at offset %d: %w", pos, err)
		}
		records = append(records, rec)
		pos = end
		goodEnd = end
	}

	// Truncate file to goodEnd if there was a torn tail.
	if goodEnd < len(data) {
		if err := os.Truncate(path, int64(goodEnd)); err != nil {
			return nil, fmt.Errorf("wal.ReadAll truncate: %w", err)
		}
	}

	return records, nil
}
