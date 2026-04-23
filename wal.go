package memdb

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// WALEntry records a single write operation for replay on recovery.
type WALEntry struct {
	Seq       uint64
	Timestamp int64
	SQL       string
	Args      []any
}

// WAL is a simple append-only write-ahead log backed by a flat gob file.
//
// Correct usage sequence:
//  1. OpenWAL — opens or creates the file
//  2. Replay  — replays all existing entries (call once at startup)
//  3. Append  — appends entries during normal operation
//  4. Truncate — clears the log after a successful snapshot
//  5. Close   — closes the file
//
// Replay must be called before Append. The fn passed to Replay must NOT
// call Append (it is called without the WAL mutex, but re-entering via
// db.Exec during replay is safe because db.Exec does not go through the
// WAL path during replay — only the raw db.mem.Exec is called).
type WAL struct {
	mu  sync.Mutex
	f   *os.File
	enc *gob.Encoder
	seq atomic.Uint64
}

// OpenWAL opens or creates the WAL file at path.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("memdb: open wal: %w", err)
	}
	return &WAL{f: f, enc: gob.NewEncoder(f)}, nil
}

// NextSeq returns the next monotonically increasing sequence number.
func (w *WAL) NextSeq() uint64 {
	return w.seq.Add(1)
}

// Append writes a WALEntry to the log. Calls fsync before returning.
func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.enc.Encode(entry); err != nil {
		return fmt.Errorf("memdb: wal append: %w", err)
	}
	return w.f.Sync()
}

// Replay decodes all entries from the WAL and calls fn for each.
// Stops at EOF or the first decode error.
// fn is called without holding the WAL mutex so it can safely append.
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
	dec := gob.NewDecoder(w.f)
	for {
		var entry WALEntry
		if err := dec.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("memdb: wal replay: %w", err)
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
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.enc = gob.NewEncoder(w.f)
	return nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.f.Close()
}
