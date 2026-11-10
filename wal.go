package memdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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
// # Recovery
//
// Replay distinguishes a torn tail from corruption. A torn tail — a partial
// length prefix, a body that runs past the end of the file, or a zero-filled
// tail — is what a crash mid-append leaves behind; it is cut off silently.
// Anything else that fails to parse (a zero or oversized length prefix with
// data after it, or a complete record that does not decode) is corruption:
// the bytes from the bad record to the end of the file are copied to a
// sidecar file "<path>.corrupt-<UTC timestamp>" before the WAL is cut back to
// its valid prefix, and Quarantine reports what was set aside.
//
// # Correct usage sequence
//
//  1. OpenWAL — opens or creates the file
//  2. Replay  — replays all existing entries (call once at startup)
//  3. Append  — appends entries during normal operation
//  4. Truncate — clears the log after a successful snapshot
//  5. Close   — closes the file
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
	seq  atomic.Uint64
	// quarantine describes what the last replay set aside; nil when it found
	// no corruption. Guarded by mu.
	quarantine *WALQuarantine
}

// WALQuarantine describes the corrupt region a replay moved out of the WAL.
// The bytes from Offset to the end of the file — the corrupt record and
// everything after it, valid or not — were copied to Path before the WAL was
// truncated to Offset. None of those entries were replayed.
type WALQuarantine struct {
	// Path is the sidecar file holding the discarded bytes.
	Path string
	// Offset is the WAL file offset of the first discarded byte, i.e. the
	// length prefix of the record that failed to parse.
	Offset int64
	// Bytes is the number of bytes moved to the sidecar.
	Bytes int64
	// Reason says why the record at Offset was rejected.
	Reason string
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
	return &WAL{f: f, path: path}, nil
}

// Quarantine reports the corruption the most recent Replay / ReplayFrom set
// aside, or nil when that replay found none (a torn tail is not corruption
// and is not reported). The caller should surface a non-nil result loudly:
// entries after Offset were not replayed.
func (w *WAL) Quarantine() *WALQuarantine {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.quarantine == nil {
		return nil
	}
	q := *w.quarantine
	return &q
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

// readAll reads all WAL entries under the lock and returns them, then cuts
// the file back to the end of the last valid record so later Appends land
// right after it instead of behind unreadable bytes. Without the cut, a
// second crash left acknowledged writes after a torn record unreachable,
// because replay stopped at the torn record and never saw them.
//
// A torn tail is cut silently. Corruption is first copied to a sidecar file
// (see quarantineTail); if that copy fails the WAL is left untouched and the
// error is returned. A read error is returned as-is and nothing is cut: an
// I/O failure says nothing about what the file holds.
func (w *WAL) readAll() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.quarantine = nil
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	scan, err := scanWAL(w.f)
	if err != nil {
		return nil, err
	}
	if scan.corrupt != "" {
		q, err := w.quarantineTail(scan.validEnd, scan.corrupt)
		if err != nil {
			return nil, err
		}
		if q != nil {
			w.quarantine = q
		}
	}
	if err := w.cutTail(scan.validEnd); err != nil {
		return nil, err
	}
	return scan.entries, nil
}

// walScan is the result of reading a WAL stream front to back.
type walScan struct {
	entries []WALEntry
	// validEnd is the offset just past the last record that decoded.
	validEnd int64
	// corrupt is empty when everything after validEnd is a torn tail (or
	// there is nothing after it). Otherwise it says why the record at
	// validEnd was rejected, and the bytes from validEnd on are corruption.
	corrupt string
}

// scanWAL decodes records from r until the first one that does not parse and
// classifies what stopped it:
//
//   - a partial length prefix, a body that runs past EOF, or a zero length
//     prefix followed only by zero bytes is a torn tail (corrupt == "");
//   - a zero length prefix with data after it, a length above walMaxRecord,
//     or a complete record DecodeEntry rejects is corruption;
//   - any read error other than EOF is returned as an error.
//
// It takes an io.Reader rather than the file so the classification can be
// tested against readers that fail.
func scanWAL(r io.Reader) (walScan, error) {
	var scan walScan
	br := bufio.NewReaderSize(r, 64*1024)
	var lenBuf [4]byte
	for {
		// Read the 4-byte length prefix.
		_, err := io.ReadFull(br, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Clean end, or a partial length prefix: a torn tail.
			return scan, nil
		}
		if err != nil {
			return scan, fmt.Errorf("memdb: wal read length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf[:])
		if length == 0 {
			// Append never writes an empty record. Zeros to the end of the
			// file are what a crash leaves when the file was extended but
			// the data never reached the disk; anything else is corruption.
			zero, err := restIsZero(br)
			if err != nil {
				return scan, fmt.Errorf("memdb: wal read: %w", err)
			}
			if !zero {
				scan.corrupt = "zero length prefix followed by data"
			}
			return scan, nil
		}
		if length > walMaxRecord {
			scan.corrupt = fmt.Sprintf("length prefix %d exceeds the %d-byte record limit", length, walMaxRecord)
			return scan, nil
		}

		body := make([]byte, length)
		if _, err := io.ReadFull(br, body); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// The body runs past the end of the file: a torn tail.
				return scan, nil
			}
			return scan, fmt.Errorf("memdb: wal read record: %w", err)
		}

		entry, err := replication.DecodeEntry(body)
		if err != nil {
			scan.corrupt = fmt.Sprintf("record does not decode: %v", err)
			return scan, nil
		}
		scan.entries = append(scan.entries, entry)
		scan.validEnd += 4 + int64(length)
	}
}

// restIsZero reports whether every byte left in r is zero.
func restIsZero(r io.Reader) (bool, error) {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		for _, b := range buf[:n] {
			if b != 0 {
				return false, nil
			}
		}
		if err == io.EOF {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
}

// quarantineTail copies the bytes from offset to the end of the WAL into a
// new sidecar file next to it and fsyncs the copy, so cutting the corrupt
// region out of the WAL destroys nothing. The WAL itself is not modified
// here; on any error the partial sidecar is removed and the caller must
// leave the WAL as it is. Returns nil when there is nothing after offset.
func (w *WAL) quarantineTail(offset int64, reason string) (*WALQuarantine, error) {
	end, err := w.f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("memdb: wal seek end: %w", err)
	}
	if end <= offset {
		return nil, nil
	}

	sidecar := w.path + ".corrupt-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	out, err := os.OpenFile(sidecar, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("memdb: wal quarantine: %w", err)
	}
	n, err := io.Copy(out, io.NewSectionReader(w.f, offset, end-offset))
	if err == nil && n != end-offset {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(sidecar)
		return nil, fmt.Errorf("memdb: wal quarantine %s: %w", sidecar, err)
	}
	// fsync the directory so the sidecar's name survives a crash that follows
	// the truncate. Best effort, as for the snapshot rename.
	if dir, err := os.Open(filepath.Dir(sidecar)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return &WALQuarantine{Path: sidecar, Offset: offset, Bytes: n, Reason: reason}, nil
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

// Truncate clears the WAL after a successful snapshot. The truncation is not
// fsynced: the snapshot records the checkpoint, so entries that reappear
// after a crash are skipped on replay rather than re-applied.
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
