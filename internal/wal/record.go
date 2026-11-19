package wal

import (
	"encoding/binary"
	"fmt"
)

// encodeRecord serialises a Record into a compact binary format:
//
//	[type:1][keyLen:2][key:N][valueLen:4][value:M][expiresAt:8][seq:8]
func encodeRecord(r *Record) []byte {
	kLen := len(r.Key)
	vLen := len(r.Value)
	// 1 + 2 + kLen + 4 + vLen + 8 + 8
	buf := make([]byte, 1+2+kLen+4+vLen+8+8)
	pos := 0

	buf[pos] = byte(r.Type)
	pos++

	binary.LittleEndian.PutUint16(buf[pos:], uint16(kLen))
	pos += 2
	copy(buf[pos:], r.Key)
	pos += kLen

	binary.LittleEndian.PutUint32(buf[pos:], uint32(vLen))
	pos += 4
	copy(buf[pos:], r.Value)
	pos += vLen

	binary.LittleEndian.PutUint64(buf[pos:], uint64(r.ExpiresAt))
	pos += 8

	binary.LittleEndian.PutUint64(buf[pos:], r.Seq)

	return buf
}

// decodeRecord deserialises a Record from the payload bytes produced by
// encodeRecord.
func decodeRecord(data []byte) (*Record, error) {
	if len(data) < 1+2+4+8+8 {
		return nil, fmt.Errorf("record too short (%d bytes)", len(data))
	}
	pos := 0

	rType := RecordType(data[pos])
	pos++

	kLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	if pos+kLen > len(data) {
		return nil, fmt.Errorf("key length %d exceeds data", kLen)
	}
	key := string(data[pos : pos+kLen])
	pos += kLen

	if pos+4 > len(data) {
		return nil, fmt.Errorf("truncated value length field")
	}
	vLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if pos+vLen > len(data) {
		return nil, fmt.Errorf("value length %d exceeds data", vLen)
	}
	value := make([]byte, vLen)
	copy(value, data[pos:pos+vLen])
	pos += vLen

	if pos+16 > len(data) {
		return nil, fmt.Errorf("truncated tail fields")
	}
	expiresAt := int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8
	seq := binary.LittleEndian.Uint64(data[pos:])

	return &Record{
		Type:      rType,
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
		Seq:       seq,
	}, nil
}
