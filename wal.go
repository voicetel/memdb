package memdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// walEncBuf is a pooled scratch buffer used by WAL.Append to avoid allocating
// a fresh byte slice on every write. The buffer is reset (not reallocated)
// between uses so its underlying storage grows to the largest entry ever
// encoded and then stays stable for the lifetime of the process.
var walEncBuf = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256) // typical Raft/WAL entry fits in a few hundred bytes
		return &b
	},
}

// WALEntry records a single write operation for replay on recovery.
type WALEntry struct {
	Seq       uint64
	Timestamp int64
	SQL       string
	Args      []any
}

// WAL is a simple append-only write-ahead log.
//
// # On-disk format
//
// Each record on disk is:
//
//	[4-byte big-endian length][record body]
//
// The record body is one of:
//
//  1. Binary v1 format (this package, preferred): begins with the magic
//     byte sequence walBinaryMagic = {0x4D, 0x44, 0x42, 0x57} ("MDBW") and
//     a 1-byte version tag. This path avoids encoding/gob reflection and
//     was measured at ~25% of total CPU in WAL-mode pprof traces before
//     the change. See walEncodeBinary / walDecodeBinary for the wire
//     layout.
//
//  2. Legacy gob-encoded WALEntry: the format used before v1.5.
//     Records that do not start with the binary magic are decoded as gob
//     for backwards compatibility so an in-place upgrade does not lose
//     any WAL entry written by an older binary.
//
// Each record is self-contained so that a reader can decode one record
// without state from any previous record. This is required for
// crash-recovery Replay to skip past a truncated or corrupt tail cleanly.
//
// # Correct usage sequence
//
//  1. OpenWAL — opens or creates the file
//  2. Replay  — replays all existing entries (call once at startup)
//  3. Append  — appends entries during normal operation
//  4. Truncate — clears the log after a successful snapshot
//  5. Close   — closes the file
type WAL struct {
	mu  sync.Mutex
	f   *os.File
	seq atomic.Uint64
}

// walBinaryMagic is the 4-byte prefix that identifies a binary-format
// record. Chosen so it is extremely unlikely to appear as the first four
// bytes of a gob stream — gob records start with a small type descriptor
// count (usually a single byte in the 0x01..0x7F range), never the ASCII
// "MDBW" sequence.
var walBinaryMagic = [4]byte{'M', 'D', 'B', 'W'}

// walBinaryVersion is the current binary wire format version. Bumped on any
// incompatible change. Readers MUST reject unknown versions rather than
// silently misinterpret the bytes.
const walBinaryVersion byte = 1

// Argument type tags used by the binary encoder. Values are stable wire
// constants — do not renumber. If a new Go type must be supported, append
// a new tag and bump walBinaryVersion if decoding older files becomes
// impossible (which is not the case when simply adding tags).
const (
	walArgNil     byte = 0
	walArgInt64   byte = 1
	walArgUint64  byte = 2
	walArgFloat64 byte = 3
	walArgBool    byte = 4
	walArgString  byte = 5
	walArgBytes   byte = 6
	walArgTime    byte = 7 // time.Time as UnixNano int64
)

// walMaxRecord bounds a single WAL record body (length-prefixed payload,
// not counting the 4-byte header). 64 MB is generous for any reasonable
// SQL statement + bound parameters and prevents a corrupt length prefix
// from causing a huge allocation on Replay.
const walMaxRecord = 64 * 1024 * 1024

// OpenWAL opens or creates the WAL file at path.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("memdb: open wal: %w", err)
	}
	return &WAL{f: f}, nil
}

// NextSeq returns the next monotonically increasing sequence number.
func (w *WAL) NextSeq() uint64 {
	return w.seq.Add(1)
}

// Append encodes entry in the binary v1 format, writes the length-prefixed
// record, and fsyncs.
//
// Hot path: the encoder scratch buffer is drawn from a sync.Pool and the
// 4-byte length prefix is reserved as the first four bytes of that same
// buffer. walEncodeBinary appends the body starting at offset 4, so the
// final slice is [length-prefix][body] with zero extra allocations. The
// 4-byte length prefix and body are written in one write(2) call, which
// guarantees no reader can observe a header without its payload.
//
// If the binary encoder encounters an Arg of an unregistered type it
// returns a typed error — the caller (Exec / execDirect) surfaces this
// back to the application so the offending write is rejected rather than
// silently written in a format we cannot decode.
func (w *WAL) Append(entry WALEntry) error {
	bufPtr := walEncBuf.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	defer func() {
		*bufPtr = buf
		walEncBuf.Put(bufPtr)
	}()

	// Reserve 4 bytes for the length prefix. walEncodeBinary will append the
	// body immediately after, giving us [prefix][body] in one contiguous
	// slice — no second allocation or copy needed.
	buf = append(buf, 0, 0, 0, 0)

	var err error
	buf, err = walEncodeBinary(buf, entry)
	if err != nil {
		return fmt.Errorf("memdb: wal encode: %w", err)
	}
	bodyLen := len(buf) - 4
	if bodyLen > walMaxRecord {
		return fmt.Errorf("memdb: wal entry too large (%d bytes)", bodyLen)
	}
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("memdb: wal write: %w", err)
	}
	return w.f.Sync()
}

// Replay reads every entry from the log and calls fn for each.
// fn is called without holding the WAL mutex so it can safely call
// Append — but note the replay callback is typically db.mem.Exec which
// does NOT re-enter the WAL, so this is not actually exercised.
func (w *WAL) Replay(fn func(WALEntry) error) error {
	entries, err := w.readAll()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := fn(entry); err != nil {
			return err
		}
		if entry.Seq > w.seq.Load() {
			w.seq.Store(entry.Seq)
		}
	}
	return nil
}

// readAll reads all WAL entries under the lock and returns them.
//
// Records prefixed with walBinaryMagic are decoded with walDecodeBinary;
// everything else is decoded as gob for backwards compatibility with WAL
// files written by memdb versions prior to 1.5.
func (w *WAL) readAll() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []WALEntry
	var lenBuf [4]byte
	for {
		// Read the 4-byte length prefix.
		_, err := io.ReadFull(w.f, lenBuf[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			// Partial length prefix at end of file — truncated; stop here.
			break
		}
		if err != nil {
			return nil, fmt.Errorf("memdb: wal read length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf[:])
		if length == 0 || length > walMaxRecord {
			// Corrupt record — stop replay; everything prior is valid.
			break
		}

		body := make([]byte, length)
		if _, err := io.ReadFull(w.f, body); err != nil {
			// Truncated body — stop here; everything prior is valid.
			break
		}

		entry, err := walDecodeRecord(body)
		if err != nil {
			// Corrupt body — stop here; everything prior is valid.
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// walDecodeRecord dispatches to the binary or legacy-gob decoder based on
// the record prefix. Kept separate from readAll so unit tests can exercise
// dispatch without touching the filesystem.
func walDecodeRecord(body []byte) (WALEntry, error) {
	if len(body) >= 5 &&
		body[0] == walBinaryMagic[0] &&
		body[1] == walBinaryMagic[1] &&
		body[2] == walBinaryMagic[2] &&
		body[3] == walBinaryMagic[3] {
		return walDecodeBinary(body)
	}
	var entry WALEntry
	if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&entry); err != nil {
		return WALEntry{}, err
	}
	return entry, nil
}

// Truncate clears the WAL after a successful snapshot.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, io.SeekStart)
	return err
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// ─── binary encoder ──────────────────────────────────────────────────────────

// ErrWALUnsupportedArgType is declared in errors.go alongside the rest of
// the package's sentinel errors. It is returned by Append when a WALEntry's
// Args slice contains a value of a type the binary encoder cannot
// serialise; the caller (Exec / execDirect / Raft FSM) should surface this
// back to the application so the offending write is rejected cleanly
// instead of being written in a format that cannot be replayed.

// walEncodeBinary appends the binary v1 representation of entry to dst and
// returns the resulting slice. dst may be nil or a reused buffer from the
// pool — walEncodeBinary grows it as needed using standard append semantics.
//
// Wire format (all integers big-endian):
//
//	[4]byte  magic      "MDBW"
//	 byte    version    walBinaryVersion
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
// encoding/gob's reflection-based encoder was measured at ~25% of total
// CPU under DurabilityWAL — replacing it with this purpose-built encoder
// eliminates every reflect.Value allocation and every type-descriptor
// hash table lookup on the hot path.
func walEncodeBinary(dst []byte, entry WALEntry) ([]byte, error) {
	// Header.
	dst = append(dst, walBinaryMagic[0], walBinaryMagic[1], walBinaryMagic[2], walBinaryMagic[3])
	dst = append(dst, walBinaryVersion)

	// Fixed fields.
	dst = walAppendU64(dst, entry.Seq)
	dst = walAppendU64(dst, uint64(entry.Timestamp)) // round-trips via int64

	// SQL.
	if uint64(len(entry.SQL)) > math.MaxUint32 {
		return nil, fmt.Errorf("memdb: wal: sql too long (%d bytes)", len(entry.SQL))
	}
	dst = walAppendU32(dst, uint32(len(entry.SQL)))
	dst = append(dst, entry.SQL...)

	// Args.
	if uint64(len(entry.Args)) > math.MaxUint32 {
		return nil, fmt.Errorf("memdb: wal: too many args (%d)", len(entry.Args))
	}
	dst = walAppendU32(dst, uint32(len(entry.Args)))
	for i, a := range entry.Args {
		var err error
		dst, err = walAppendArg(dst, a)
		if err != nil {
			return nil, fmt.Errorf("arg %d: %w", i, err)
		}
	}
	return dst, nil
}

// walAppendArg serialises a single SQL argument. Types are dispatched in
// descending order of frequency (strings and int64 dominate real-world
// SQL parameters) so the common path hits the first case branches.
func walAppendArg(dst []byte, a any) ([]byte, error) {
	switch v := a.(type) {
	case nil:
		return append(dst, walArgNil), nil
	case string:
		if uint64(len(v)) > math.MaxUint32 {
			return nil, fmt.Errorf("string arg too long (%d bytes)", len(v))
		}
		dst = append(dst, walArgString)
		dst = walAppendU32(dst, uint32(len(v)))
		return append(dst, v...), nil
	case int64:
		dst = append(dst, walArgInt64)
		return walAppendU64(dst, uint64(v)), nil
	case int:
		dst = append(dst, walArgInt64)
		return walAppendU64(dst, uint64(int64(v))), nil
	case int32:
		dst = append(dst, walArgInt64)
		return walAppendU64(dst, uint64(int64(v))), nil
	case int16:
		dst = append(dst, walArgInt64)
		return walAppendU64(dst, uint64(int64(v))), nil
	case int8:
		dst = append(dst, walArgInt64)
		return walAppendU64(dst, uint64(int64(v))), nil
	case uint64:
		dst = append(dst, walArgUint64)
		return walAppendU64(dst, v), nil
	case uint:
		dst = append(dst, walArgUint64)
		return walAppendU64(dst, uint64(v)), nil
	case uint32:
		dst = append(dst, walArgUint64)
		return walAppendU64(dst, uint64(v)), nil
	case uint16:
		dst = append(dst, walArgUint64)
		return walAppendU64(dst, uint64(v)), nil
	case uint8:
		dst = append(dst, walArgUint64)
		return walAppendU64(dst, uint64(v)), nil
	case float64:
		dst = append(dst, walArgFloat64)
		return walAppendU64(dst, math.Float64bits(v)), nil
	case float32:
		dst = append(dst, walArgFloat64)
		return walAppendU64(dst, math.Float64bits(float64(v))), nil
	case bool:
		dst = append(dst, walArgBool)
		if v {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case []byte:
		if uint64(len(v)) > math.MaxUint32 {
			return nil, fmt.Errorf("bytes arg too long (%d bytes)", len(v))
		}
		dst = append(dst, walArgBytes)
		dst = walAppendU32(dst, uint32(len(v)))
		return append(dst, v...), nil
	case time.Time:
		dst = append(dst, walArgTime)
		return walAppendU64(dst, uint64(v.UnixNano())), nil
	default:
		return nil, fmt.Errorf("%w: %T", ErrWALUnsupportedArgType, a)
	}
}

func walAppendU32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func walAppendU64(dst []byte, v uint64) []byte {
	return append(dst,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// ─── binary decoder ──────────────────────────────────────────────────────────

// walDecodeBinary decodes a body that begins with walBinaryMagic. It
// validates the version tag and returns a descriptive error for any
// truncation or type-tag inconsistency so Replay can stop at exactly the
// first unrecoverable record.
func walDecodeBinary(body []byte) (WALEntry, error) {
	r := walReader{buf: body}

	// Header.
	magic, ok := r.readBytes(4)
	if !ok || magic[0] != walBinaryMagic[0] || magic[1] != walBinaryMagic[1] ||
		magic[2] != walBinaryMagic[2] || magic[3] != walBinaryMagic[3] {
		return WALEntry{}, errors.New("wal: missing binary magic")
	}
	version, ok := r.readByte()
	if !ok {
		return WALEntry{}, errors.New("wal: truncated version")
	}
	if version != walBinaryVersion {
		return WALEntry{}, fmt.Errorf("wal: unknown binary version %d", version)
	}

	var entry WALEntry

	// Fixed fields.
	seq, ok := r.readU64()
	if !ok {
		return WALEntry{}, errors.New("wal: truncated seq")
	}
	entry.Seq = seq

	ts, ok := r.readU64()
	if !ok {
		return WALEntry{}, errors.New("wal: truncated timestamp")
	}
	entry.Timestamp = int64(ts)

	// SQL.
	sqlLen, ok := r.readU32()
	if !ok {
		return WALEntry{}, errors.New("wal: truncated sql length")
	}
	sqlBytes, ok := r.readBytes(int(sqlLen))
	if !ok {
		return WALEntry{}, errors.New("wal: truncated sql")
	}
	// Copy — the reader backs onto the record body which is discarded
	// after decoding, and we do not want the returned string to alias
	// the caller's buffer.
	entry.SQL = string(sqlBytes)

	// Args.
	nArgs, ok := r.readU32()
	if !ok {
		return WALEntry{}, errors.New("wal: truncated args count")
	}
	// Cap the arg count at something sane so a corrupt prefix cannot
	// trigger a giant allocation on make([]any, nArgs).
	if nArgs > 1<<20 {
		return WALEntry{}, fmt.Errorf("wal: implausible args count %d", nArgs)
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
		return WALEntry{}, fmt.Errorf("wal: %d trailing bytes after entry", len(r.buf)-r.pos)
	}
	return entry, nil
}

// walReader is a tiny forward-only byte reader. Inlined rather than using
// bytes.Reader so the decoder has no interface dispatch on the hot path.
type walReader struct {
	buf []byte
	pos int
}

func (r *walReader) readByte() (byte, bool) {
	if r.pos >= len(r.buf) {
		return 0, false
	}
	b := r.buf[r.pos]
	r.pos++
	return b, true
}

func (r *walReader) readBytes(n int) ([]byte, bool) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, false
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, true
}

func (r *walReader) readU32() (uint32, bool) {
	b, ok := r.readBytes(4)
	if !ok {
		return 0, false
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), true
}

func (r *walReader) readU64() (uint64, bool) {
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
func (r *walReader) readArg() (any, error) {
	tag, ok := r.readByte()
	if !ok {
		return nil, errors.New("truncated tag")
	}
	switch tag {
	case walArgNil:
		return nil, nil
	case walArgInt64:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated int64")
		}
		return int64(v), nil
	case walArgUint64:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated uint64")
		}
		return v, nil
	case walArgFloat64:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated float64")
		}
		return math.Float64frombits(v), nil
	case walArgBool:
		b, ok := r.readByte()
		if !ok {
			return nil, errors.New("truncated bool")
		}
		return b != 0, nil
	case walArgString:
		n, ok := r.readU32()
		if !ok {
			return nil, errors.New("truncated string length")
		}
		b, ok := r.readBytes(int(n))
		if !ok {
			return nil, errors.New("truncated string")
		}
		// Copy via string() — the underlying buffer is discarded.
		return string(b), nil
	case walArgBytes:
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
	case walArgTime:
		v, ok := r.readU64()
		if !ok {
			return nil, errors.New("truncated time")
		}
		return time.Unix(0, int64(v)).UTC(), nil
	default:
		return nil, fmt.Errorf("unknown arg tag %d", tag)
	}
}
