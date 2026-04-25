package replication

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// ErrUnsupportedArgType is returned by EncodeEntry when WALEntry.Args contains
// a value of a Go type the binary encoder does not recognise. Callers should
// surface this back to the application so the offending write is rejected
// cleanly rather than written in a format that cannot be replayed.
//
// Supported arg types: nil, all integer widths, float32/64, bool, string,
// []byte, and time.Time.
var ErrUnsupportedArgType = errors.New("replication: unsupported arg type")

// BinaryMagic is the 4-byte prefix that identifies a binary-format record.
// Chosen so it is extremely unlikely to appear as the first four bytes of a
// gob stream — gob records start with a small type descriptor count (usually
// a single byte in the 0x01..0x7F range), never the ASCII "MDBW" sequence.
//
// The magic is exported so dispatchers (for example, raft FSM apply or WAL
// replay) can probe an incoming record's first four bytes to choose between
// the binary decoder and a legacy gob decoder for backward compatibility.
var BinaryMagic = [4]byte{'M', 'D', 'B', 'W'}

// BinaryVersion is the current binary wire format version. Bumped on any
// incompatible change. Decoders MUST reject unknown versions rather than
// silently misinterpret the bytes.
const BinaryVersion byte = 1

// Argument type tags used by the binary encoder. Values are stable wire
// constants — do not renumber. If a new Go type must be supported, append
// a new tag; bump BinaryVersion if decoding older payloads becomes
// impossible (which is not the case when simply adding tags).
const (
	argNil     byte = 0
	argInt64   byte = 1
	argUint64  byte = 2
	argFloat64 byte = 3
	argBool    byte = 4
	argString  byte = 5
	argBytes   byte = 6
	argTime    byte = 7 // time.Time as UnixNano int64
)

// EncodeEntry appends the binary v1 representation of entry to dst and
// returns the resulting slice. dst may be nil or a reused buffer — the
// encoder grows it as needed using standard append semantics.
//
// Wire format (all integers big-endian):
//
//	[4]byte  magic      "MDBW"
//	 byte    version    BinaryVersion
//	 uint64  seq
//	 int64   timestamp
//	 uint32  sqlLen
//	 ...     sql bytes
//	 uint32  nArgs
//	 for each arg:
//	   byte tag
//	   ... tag-specific payload:
//	     nil     — nothing
//	     int64   — 8 bytes
//	     uint64  — 8 bytes
//	     float64 — 8 bytes (IEEE 754 bits)
//	     bool    — 1 byte (0 or 1)
//	     string  — uint32 len + bytes
//	     []byte  — uint32 len + bytes
//	     time    — 8 bytes (UnixNano int64)
//
// Rationale for the explicit types: the argument types accepted by
// database/sql via the mattn/go-sqlite3 driver are a small finite set.
// encoding/gob's reflection-based encoder was measured at ~25% of total CPU
// under DurabilityWAL and ~31% in the Raft FSM Apply path — replacing it
// with this purpose-built encoder eliminates every reflect.Value allocation
// and every type-descriptor hash table lookup on the hot path.
func EncodeEntry(dst []byte, entry WALEntry) ([]byte, error) {
	dst = append(dst, BinaryMagic[0], BinaryMagic[1], BinaryMagic[2], BinaryMagic[3])
	dst = append(dst, BinaryVersion)

	dst = appendU64(dst, entry.Seq)
	dst = appendU64(dst, uint64(entry.Timestamp))

	if uint64(len(entry.SQL)) > math.MaxUint32 {
		return nil, fmt.Errorf("replication: sql too long (%d bytes)", len(entry.SQL))
	}
	dst = appendU32(dst, uint32(len(entry.SQL)))
	dst = append(dst, entry.SQL...)

	if uint64(len(entry.Args)) > math.MaxUint32 {
		return nil, fmt.Errorf("replication: too many args (%d)", len(entry.Args))
	}
	dst = appendU32(dst, uint32(len(entry.Args)))
	for i, a := range entry.Args {
		var err error
		dst, err = appendArg(dst, a)
		if err != nil {
			return nil, fmt.Errorf("arg %d: %w", i, err)
		}
	}
	return dst, nil
}

// HasBinaryMagic reports whether body begins with BinaryMagic. Callers that
// must support both binary and a legacy format use this as the dispatch
// predicate (binary first, legacy fallback).
func HasBinaryMagic(body []byte) bool {
	return len(body) >= 4 &&
		body[0] == BinaryMagic[0] &&
		body[1] == BinaryMagic[1] &&
		body[2] == BinaryMagic[2] &&
		body[3] == BinaryMagic[3]
}

// DecodeEntry decodes a body that begins with BinaryMagic. It validates the
// version tag and returns a descriptive error for any truncation or type-tag
// inconsistency so the caller can stop at exactly the first unrecoverable
// record.
func DecodeEntry(body []byte) (WALEntry, error) {
	r := reader{buf: body}

	magic, ok := r.readBytes(4)
	if !ok || magic[0] != BinaryMagic[0] || magic[1] != BinaryMagic[1] ||
		magic[2] != BinaryMagic[2] || magic[3] != BinaryMagic[3] {
		return WALEntry{}, errors.New("replication: missing binary magic")
	}
	version, ok := r.readByte()
	if !ok {
		return WALEntry{}, errors.New("replication: truncated version")
	}
	if version != BinaryVersion {
		return WALEntry{}, fmt.Errorf("replication: unknown binary version %d", version)
	}

	var entry WALEntry

	seq, ok := r.readU64()
	if !ok {
		return WALEntry{}, errors.New("replication: truncated seq")
	}
	entry.Seq = seq

	ts, ok := r.readU64()
	if !ok {
		return WALEntry{}, errors.New("replication: truncated timestamp")
	}
	entry.Timestamp = int64(ts)

	sqlLen, ok := r.readU32()
	if !ok {
		return WALEntry{}, errors.New("replication: truncated sql length")
	}
	sqlBytes, ok := r.readBytes(int(sqlLen))
	if !ok {
		return WALEntry{}, errors.New("replication: truncated sql")
	}
	entry.SQL = string(sqlBytes)

	nArgs, ok := r.readU32()
	if !ok {
		return WALEntry{}, errors.New("replication: truncated args count")
	}
	if nArgs > 1<<20 {
		return WALEntry{}, fmt.Errorf("replication: implausible args count %d", nArgs)
	}
	if nArgs > 0 {
		entry.Args = make([]any, nArgs)
		for i := uint32(0); i < nArgs; i++ {
			v, err := r.readArg()
			if err != nil {
				return WALEntry{}, fmt.Errorf("arg %d: %w", i, err)
			}
			entry.Args[i] = v
		}
	}

	if r.pos != len(r.buf) {
		return WALEntry{}, fmt.Errorf("replication: %d trailing bytes after entry", len(r.buf)-r.pos)
	}
	return entry, nil
}

// appendArg serialises a single SQL argument. Types are dispatched in
// descending order of frequency (strings and int64 dominate real-world SQL
// parameters) so the common path hits the first case branches.
func appendArg(dst []byte, a any) ([]byte, error) {
	switch v := a.(type) {
	case nil:
		return append(dst, argNil), nil
	case string:
		if uint64(len(v)) > math.MaxUint32 {
			return nil, fmt.Errorf("string arg too long (%d bytes)", len(v))
		}
		dst = append(dst, argString)
		dst = appendU32(dst, uint32(len(v)))
		return append(dst, v...), nil
	case int64:
		dst = append(dst, argInt64)
		return appendU64(dst, uint64(v)), nil
	case int:
		dst = append(dst, argInt64)
		return appendU64(dst, uint64(int64(v))), nil
	case int32:
		dst = append(dst, argInt64)
		return appendU64(dst, uint64(int64(v))), nil
	case int16:
		dst = append(dst, argInt64)
		return appendU64(dst, uint64(int64(v))), nil
	case int8:
		dst = append(dst, argInt64)
		return appendU64(dst, uint64(int64(v))), nil
	case uint64:
		dst = append(dst, argUint64)
		return appendU64(dst, v), nil
	case uint:
		dst = append(dst, argUint64)
		return appendU64(dst, uint64(v)), nil
	case uint32:
		dst = append(dst, argUint64)
		return appendU64(dst, uint64(v)), nil
	case uint16:
		dst = append(dst, argUint64)
		return appendU64(dst, uint64(v)), nil
	case uint8:
		dst = append(dst, argUint64)
		return appendU64(dst, uint64(v)), nil
	case float64:
		dst = append(dst, argFloat64)
		return appendU64(dst, math.Float64bits(v)), nil
	case float32:
		dst = append(dst, argFloat64)
		return appendU64(dst, math.Float64bits(float64(v))), nil
	case bool:
		dst = append(dst, argBool)
		if v {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case []byte:
		if uint64(len(v)) > math.MaxUint32 {
			return nil, fmt.Errorf("bytes arg too long (%d bytes)", len(v))
		}
		dst = append(dst, argBytes)
		dst = appendU32(dst, uint32(len(v)))
		return append(dst, v...), nil
	case time.Time:
		dst = append(dst, argTime)
		return appendU64(dst, uint64(v.UnixNano())), nil
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedArgType, a)
	}
}

func appendU32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendU64(dst []byte, v uint64) []byte {
	return append(dst,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// reader is a tiny forward-only byte reader. Inlined rather than using
// bytes.Reader so the decoder has no interface dispatch on the hot path.
type reader struct {
	buf []byte
	pos int
}

func (r *reader) readByte() (byte, bool) {
	if r.pos >= len(r.buf) {
		return 0, false
	}
	b := r.buf[r.pos]
	r.pos++
	return b, true
}

func (r *reader) readBytes(n int) ([]byte, bool) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, false
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, true
}

func (r *reader) readU32() (uint32, bool) {
	b, ok := r.readBytes(4)
	if !ok {
		return 0, false
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), true
}

func (r *reader) readU64() (uint64, bool) {
	b, ok := r.readBytes(8)
	if !ok {
		return 0, false
	}
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]), true
}

// readArg decodes one tagged argument. The decoder returns int64 for every
// signed-integer wire tag and uint64 for every unsigned tag — callers that
// care about the original Go type (int8, int16, ...) cannot recover it,
// but database/sql and the SQLite driver accept the widened types
// transparently so this is intentional.
func (r *reader) readArg() (any, error) {
	tag, ok := r.readByte()
	if !ok {
		return nil, errors.New("truncated tag")
	}
	switch tag {
	case argNil:
		return nil, nil
	case argInt64:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated int64")
		}
		return int64(v), nil
	case argUint64:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated uint64")
		}
		return v, nil
	case argFloat64:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated float64")
		}
		return math.Float64frombits(v), nil
	case argBool:
		b, ok := r.readByte()
		if !ok {
			return nil, errors.New("truncated bool")
		}
		return b != 0, nil
	case argString:
		n, ok := r.readU32()
		if !ok {
			return nil, errors.New("truncated string length")
		}
		b, ok := r.readBytes(int(n))
		if !ok {
			return nil, errors.New("truncated string")
		}
		return string(b), nil
	case argBytes:
		n, ok := r.readU32()
		if !ok {
			return nil, errors.New("truncated bytes length")
		}
		b, ok := r.readBytes(int(n))
		if !ok {
			return nil, errors.New("truncated bytes")
		}
		// Defensive copy: the caller may retain the returned slice long
		// after the source buffer has been freed.
		cp := make([]byte, n)
		copy(cp, b)
		return cp, nil
	case argTime:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated time")
		}
		return time.Unix(0, int64(v)).UTC(), nil
	default:
		return nil, fmt.Errorf("unknown arg tag %d", tag)
	}
}
