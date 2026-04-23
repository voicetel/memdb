package memdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// walEncBuf is a pooled scratch buffer used by WAL.Append to avoid allocating
// a fresh bytes.Buffer + encoder on every write. The buffer is reset (not
// reallocated) between uses so its underlying storage grows to the largest
// entry ever encoded and then stays stable for the lifetime of the process.
//
// gob encoders maintain per-stream type descriptor state, so we deliberately
// do NOT pool the *gob.Encoder — each Append allocates a new one over the
// pooled buffer so the on-disk record remains independently decodable (see
// the format notes on WAL).
var walEncBuf = sync.Pool{
	New: func() any {
		b := &bytes.Buffer{}
		b.Grow(256) // typical Raft/WAL entry fits in a few hundred bytes
		return b
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
// On-disk format:
//
//	Each record is [4-byte big-endian length][gob-encoded WALEntry].
//	Each record is self-contained — the gob stream is independently
//	decodable because it is terminated after one message. This prevents
//	the "duplicate type descriptor" failure that would occur if we used
//	one long-lived gob.Encoder across process restarts.
//
// Correct usage sequence:
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

// Append encodes entry as a self-contained gob message, writes the
// length-prefixed record, and fsyncs.
//
// Hot path: the encoder scratch buffer is drawn from a sync.Pool so the
// steady-state allocation is a single []byte for the length-prefixed record.
// The 4-byte length prefix is written in the same write(2) call as the body
// to guarantee that no reader (including a crash-recovery Replay) can ever
// observe a header without its payload.
func (w *WAL) Append(entry WALEntry) error {
	buf := walEncBuf.Get().(*bytes.Buffer)
	buf.Reset()
	defer walEncBuf.Put(buf)

	if err := gob.NewEncoder(buf).Encode(entry); err != nil {
		return fmt.Errorf("memdb: wal encode: %w", err)
	}
	bodyLen := buf.Len()
	if bodyLen > 64*1024*1024 {
		return fmt.Errorf("memdb: wal entry too large (%d bytes)", bodyLen)
	}

	// Build [4-byte length][body] in a single allocation so the write is
	// atomic from the kernel's perspective (one write(2) call).
	record := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(record[:4], uint32(bodyLen))
	copy(record[4:], buf.Bytes())

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Write(record); err != nil {
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
		if length == 0 || length > 64*1024*1024 {
			// Corrupt record — stop replay; everything prior is valid.
			break
		}

		body := make([]byte, length)
		if _, err := io.ReadFull(w.f, body); err != nil {
			// Truncated body — stop here; everything prior is valid.
			break
		}

		var entry WALEntry
		if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&entry); err != nil {
			// Corrupt body — stop here.
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
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
