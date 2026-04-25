package raft

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	hraft "github.com/hashicorp/raft"
)

// Wire format for a single persisted Raft log entry. All integers big-endian.
//
//	 uint64  Index
//	 uint64  Term
//	 byte    Type            (hraft.LogType is uint8)
//	 uint32  dataLen
//	 ...     Data            (dataLen bytes)
//	 uint32  extLen
//	 ...     Extensions      (extLen bytes)
//	 int64   AppendedAtUnixNano
//
// The fileLogStore wraps each encoded body with an 8-byte length prefix so
// load() can skip past corrupt records without parsing them. The encoder
// here only produces the body — appendLocked owns the prefix.
//
// Replaces encoding/json, which pprof of the Raft Apply path showed
// allocating ~27% of the inuse heap during sustained writes (the per-entry
// json.Marshal builds an intermediate bytes.Buffer plus a final
// append-copy). The binary codec writes directly into a caller-supplied
// destination slice — combined with the pool-buffer pattern in
// appendLocked, every append is zero-alloc once the pool has warmed.

// logRecordFixedSize is the minimum on-disk size of an encoded log entry
// (everything except the variable-length Data and Extensions slices).
const logRecordFixedSize = 8 + 8 + 1 + 4 + 4 + 8

// encodeLog appends the binary encoding of log to dst and returns the
// resulting slice. dst may be nil or a reused buffer.
func encodeLog(dst []byte, log *hraft.Log) ([]byte, error) {
	if uint64(len(log.Data)) > math.MaxUint32 {
		return nil, fmt.Errorf("log store: Data too large (%d bytes)", len(log.Data))
	}
	if uint64(len(log.Extensions)) > math.MaxUint32 {
		return nil, fmt.Errorf("log store: Extensions too large (%d bytes)", len(log.Extensions))
	}

	// Grow once to the exact size — append on a single grown slice avoids
	// the geometric-growth realloc chain inside the standard library's
	// encoding/json.Marshal -> bytes.Buffer.Grow path.
	need := logRecordFixedSize + len(log.Data) + len(log.Extensions)
	if cap(dst)-len(dst) < need {
		newBuf := make([]byte, len(dst), len(dst)+need)
		copy(newBuf, dst)
		dst = newBuf
	}

	dst = binary.BigEndian.AppendUint64(dst, log.Index)
	dst = binary.BigEndian.AppendUint64(dst, log.Term)
	dst = append(dst, byte(log.Type))
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(log.Data)))
	dst = append(dst, log.Data...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(log.Extensions)))
	dst = append(dst, log.Extensions...)
	dst = binary.BigEndian.AppendUint64(dst, uint64(log.AppendedAt.UnixNano()))
	return dst, nil
}

// decodeLog parses a single encoded log entry from src into out. It
// returns an error for truncated input or oversized variable-length
// fields (the latter guards against a corrupt prefix triggering a giant
// allocation).
func decodeLog(src []byte, out *hraft.Log) error {
	if len(src) < logRecordFixedSize {
		return fmt.Errorf("log store: record too short (%d bytes)", len(src))
	}
	pos := 0

	out.Index = binary.BigEndian.Uint64(src[pos:])
	pos += 8
	out.Term = binary.BigEndian.Uint64(src[pos:])
	pos += 8
	out.Type = hraft.LogType(src[pos])
	pos++

	dataLen := binary.BigEndian.Uint32(src[pos:])
	pos += 4
	if uint64(pos)+uint64(dataLen) > uint64(len(src)) {
		return fmt.Errorf("log store: truncated Data field (need %d, have %d)", dataLen, len(src)-pos)
	}
	if dataLen == 0 {
		out.Data = nil
	} else {
		// Defensive copy: the source slice will be reused / freed by the
		// caller and Raft retains the Log struct.
		out.Data = make([]byte, dataLen)
		copy(out.Data, src[pos:pos+int(dataLen)])
	}
	pos += int(dataLen)

	if pos+4 > len(src) {
		return fmt.Errorf("log store: truncated Extensions length")
	}
	extLen := binary.BigEndian.Uint32(src[pos:])
	pos += 4
	if uint64(pos)+uint64(extLen) > uint64(len(src)) {
		return fmt.Errorf("log store: truncated Extensions field (need %d, have %d)", extLen, len(src)-pos)
	}
	if extLen == 0 {
		out.Extensions = nil
	} else {
		out.Extensions = make([]byte, extLen)
		copy(out.Extensions, src[pos:pos+int(extLen)])
	}
	pos += int(extLen)

	if pos+8 > len(src) {
		return fmt.Errorf("log store: truncated AppendedAt")
	}
	out.AppendedAt = time.Unix(0, int64(binary.BigEndian.Uint64(src[pos:]))).UTC()
	pos += 8

	if pos != len(src) {
		return fmt.Errorf("log store: %d trailing bytes after entry", len(src)-pos)
	}
	return nil
}
