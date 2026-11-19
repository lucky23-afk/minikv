package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// SnapshotEntry is one key-value pair in a snapshot file.
type SnapshotEntry struct {
	Key       string
	Value     []byte
	ExpiresAt int64 // Unix nanoseconds; 0 = no expiry
}

const (
	snapshotMagic   uint32 = 0x534E4150 // "SNAP"
	snapshotVersion uint32 = 1
)

// WriteSnapshot writes all entries to a new snapshot file in dir and
// returns the file path. The file is written atomically via a temp file
// rename.
//
// Snapshot layout:
//
//	[magic:4][version:4][createdAt:8][count:8]
//	for each entry: [keyLen:2][key:N][valueLen:4][value:M][expiresAt:8]
//	[crc32:4] over everything above
func WriteSnapshot(dir string, entries []SnapshotEntry) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("wal.WriteSnapshot mkdir: %w", err)
	}

	// Sort entries for reproducibility.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	tmp := filepath.Join(dir, fmt.Sprintf("snap-%d.tmp", time.Now().UnixNano()))
	f, err := os.Create(tmp)
	if err != nil {
		return "", fmt.Errorf("wal.WriteSnapshot create tmp: %w", err)
	}

	crc := crc32.NewIEEE()

	write := func(b []byte) error {
		_, err := f.Write(b)
		if err == nil {
			crc.Write(b)
		}
		return err
	}

	// Header
	var hdr [24]byte
	binary.LittleEndian.PutUint32(hdr[0:], snapshotMagic)
	binary.LittleEndian.PutUint32(hdr[4:], snapshotVersion)
	binary.LittleEndian.PutUint64(hdr[8:], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint64(hdr[16:], uint64(len(entries)))
	if err := write(hdr[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}

	// Entries
	for _, e := range entries {
		kLen := uint16(len(e.Key))
		vLen := uint32(len(e.Value))

		var klenBuf [2]byte
		binary.LittleEndian.PutUint16(klenBuf[:], kLen)
		if err := write(klenBuf[:]); err != nil {
			f.Close()
			os.Remove(tmp)
			return "", err
		}
		if err := write([]byte(e.Key)); err != nil {
			f.Close()
			os.Remove(tmp)
			return "", err
		}

		var vlenBuf [4]byte
		binary.LittleEndian.PutUint32(vlenBuf[:], vLen)
		if err := write(vlenBuf[:]); err != nil {
			f.Close()
			os.Remove(tmp)
			return "", err
		}
		if err := write(e.Value); err != nil {
			f.Close()
			os.Remove(tmp)
			return "", err
		}

		var expBuf [8]byte
		binary.LittleEndian.PutUint64(expBuf[:], uint64(e.ExpiresAt))
		if err := write(expBuf[:]); err != nil {
			f.Close()
			os.Remove(tmp)
			return "", err
		}
	}

	// Checksum
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc.Sum32())
	if _, err := f.Write(crcBuf[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}

	final := filepath.Join(dir, fmt.Sprintf("snap-%d.snap", time.Now().UnixNano()))
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("wal.WriteSnapshot rename: %w", err)
	}
	return final, nil
}

// ReadSnapshot reads the most recent snapshot from dir and returns its
// entries plus the file path. Returns nil entries (and no error) when
// no snapshot exists.
func ReadSnapshot(dir string) ([]SnapshotEntry, string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil || len(files) == 0 {
		return nil, "", nil
	}
	sort.Strings(files)
	latest := files[len(files)-1]

	entries, err := readSnapshotFile(latest)
	if err != nil {
		return nil, latest, err
	}
	return entries, latest, nil
}

func readSnapshotFile(path string) ([]SnapshotEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("wal.ReadSnapshot read %s: %w", path, err)
	}
	if len(data) < 28 { // 24 header + 4 crc
		return nil, fmt.Errorf("wal.ReadSnapshot %s: file too short", path)
	}

	// Verify CRC (over everything except the last 4 bytes).
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	crc := crc32.NewIEEE()
	crc.Write(data[:len(data)-4])
	if crc.Sum32() != storedCRC {
		return nil, fmt.Errorf("wal.ReadSnapshot %s: CRC mismatch", path)
	}

	mg := binary.LittleEndian.Uint32(data[0:])
	if mg != snapshotMagic {
		return nil, fmt.Errorf("wal.ReadSnapshot %s: bad magic", path)
	}

	count := int(binary.LittleEndian.Uint64(data[16:]))
	entries := make([]SnapshotEntry, 0, count)
	pos := 24

	for i := 0; i < count; i++ {
		if pos+2 > len(data)-4 {
			return nil, fmt.Errorf("wal.ReadSnapshot truncated at entry %d", i)
		}
		kLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+kLen > len(data)-4 {
			return nil, fmt.Errorf("wal.ReadSnapshot truncated key at entry %d", i)
		}
		key := string(data[pos : pos+kLen])
		pos += kLen

		if pos+4 > len(data)-4 {
			return nil, fmt.Errorf("wal.ReadSnapshot truncated value len at entry %d", i)
		}
		vLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+vLen > len(data)-4 {
			return nil, fmt.Errorf("wal.ReadSnapshot truncated value at entry %d", i)
		}
		value := make([]byte, vLen)
		copy(value, data[pos:pos+vLen])
		pos += vLen

		if pos+8 > len(data)-4 {
			return nil, fmt.Errorf("wal.ReadSnapshot truncated expiresAt at entry %d", i)
		}
		expiresAt := int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8

		entries = append(entries, SnapshotEntry{Key: key, Value: value, ExpiresAt: expiresAt})
	}

	return entries, nil
}

// DeleteOldSnapshots removes all snapshot files except the latest one.
func DeleteOldSnapshots(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil || len(files) <= 1 {
		return nil
	}
	sort.Strings(files)
	for _, f := range files[:len(files)-1] {
		if err := os.Remove(f); err != nil {
			return err
		}
	}
	return nil
}
