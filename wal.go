package memdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/voicetel/memdb/replication"
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
//
// Aliased onto replication.WALEntry so the type is shared across the WAL
// hot path and the Raft FSM apply path; both encode/decode through the
// same binary codec in package replication. Existing callers that
// construct memdb.WALEntry literals or refer to the type by name continue
// to work transparently.
type WALEntry = replication.WALEntry

// WAL is a simple append-only write-ahead log.
//
// # On-disk format
//
// Each record on disk is:
//
//	[4-byte big-endian length][record body]
//
// The record body is the binary v1 format defined in package replication:
// it begins with replication.BinaryMagic ({0x4D, 0x44, 0x42, 0x57} =
// "MDBW") and a 1-byte version tag. The encoder avoids reflection
// entirely; pprof of the WAL hot path before the binary codec showed
// encoding/gob at ~25% of total CPU.
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

// Seq returns the highest sequence number handed out so far.
func (w *WAL) Seq() uint64 {
	return w.seq.Load()
}

// Append encodes entry in the binary v1 format, writes the length-prefixed
// record, and fsyncs.
//
// Hot path: the encoder scratch buffer is drawn from a sync.Pool and the
// 4-byte length prefix is reserved as the first four bytes of that same
// buffer. replication.EncodeEntry appends the body starting at offset 4,
// so the final slice is [length-prefix][body] with zero extra allocations.
// The 4-byte length prefix and body are written in one write(2) call,
// which guarantees no reader can observe a header without its payload.
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

	// Reserve 4 bytes for the length prefix. EncodeEntry will append the
	// body immediately after, giving us [prefix][body] in one contiguous
	// slice — no second allocation or copy needed.
	buf = append(buf, 0, 0, 0, 0)

	var err error
	buf, err = replication.EncodeEntry(buf, entry)
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

// Replay reads every entry from the log and calls fn for each. It is
// ReplayFrom(0, fn): every entry is replayed.
func (w *WAL) Replay(fn func(WALEntry) error) error {
	return w.ReplayFrom(0, fn)
}

// ReplayFrom reads every entry whose Seq is greater than from and calls fn for
// each. from is the snapshot's checkpoint: entries at or below it were already
// in the snapshot the caller restored, so re-applying them would fail (or
// double-apply), and skipping them is what makes recovery idempotent across a
// crash between writing the snapshot and truncating the WAL.
//
// The sequence counter is seeded to at least from so a later Append never
// reuses a sequence number an already-snapshotted entry holds. fn is called
// without holding the WAL mutex.
func (w *WAL) ReplayFrom(from uint64, fn func(WALEntry) error) error {
	entries, err := w.readAll()
	if err != nil {
		return err
	}
	if from > w.seq.Load() {
		w.seq.Store(from)
	}
	for _, entry := range entries {
		if entry.Seq <= from {
			continue
		}
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
// Records are decoded with replication.DecodeEntry; bodies that do not
// start with replication.BinaryMagic are treated as corrupt and stop
// the replay (everything prior remains valid).
func (w *WAL) readAll() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []WALEntry
	var lenBuf [4]byte
	offset := int64(0) // offset of the next record's length prefix
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

		entry, err := replication.DecodeEntry(body)
		if err != nil {
			// Corrupt body — stop here; everything prior is valid.
			break
		}
		entries = append(entries, entry)
		offset += 4 + int64(length)
	}
	// A torn or corrupt tail is cut off now, so later Appends land right after
	// the last valid entry instead of behind the torn bytes. Without this, a
	// second crash left acknowledged writes after a torn record unreachable,
	// because replay stopped at the torn record and never saw them.
	if err := w.cutTail(offset); err != nil {
		return nil, err
	}
	return entries, nil
}

// cutTail truncates the file to offset and fsyncs the truncation, so replay's
// view of the tail is durable. offset is the end of the last valid record, or
// the current end when every record was valid.
func (w *WAL) cutTail(offset int64) error {
	end, err := w.f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("memdb: wal seek end: %w", err)
	}
	if offset == end {
		return nil
	}
	if err := w.f.Truncate(offset); err != nil {
		return fmt.Errorf("memdb: wal truncate torn tail: %w", err)
	}
	return w.f.Sync()
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
