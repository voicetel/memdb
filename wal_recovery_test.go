package memdb_test

// Recovery tests for DurabilityWAL, covering the three problems the M0 spike
// found (docs/spikes/m0-memdb.md): a torn record followed by more writes, a
// crash between the snapshot's rename and the WAL truncate, and a write that
// lands between the snapshot and the truncate.

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/replication"
)

// rawWALEntry returns the on-disk bytes of one WAL record: the 4-byte
// big-endian length prefix followed by the binary-encoded body.
func rawWALEntry(t *testing.T, seq uint64, sql string, args ...any) []byte {
	t.Helper()
	body, err := replication.EncodeEntry(nil, replication.WALEntry{Seq: seq, SQL: sql, Args: args})
	if err != nil {
		t.Fatal(err)
	}
	rec := make([]byte, 4+len(body))
	rec[0], rec[1], rec[2], rec[3] = byte(len(body)>>24), byte(len(body)>>16), byte(len(body)>>8), byte(len(body))
	copy(rec[4:], body)
	return rec
}

// TestWAL_TornRecordDoesNotStrandLaterWrites writes nine records, tears the
// tail of a tenth (a crash mid-write), reopens, appends five more, and reopens
// again: the nine and the five must all survive. Before the fix, replay stopped
// at the torn record and the five later writes landed behind it, where a second
// replay never saw them.
func TestWAL_TornRecordDoesNotStrandLaterWrites(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "w.wal")

	write := func(path string, records [][]byte) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			if _, err := f.Write(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	var records [][]byte
	for i := 1; i <= 9; i++ {
		records = append(records, rawWALEntry(t, uint64(i), `INSERT INTO kv VALUES (?, ?)`, i, "v"))
	}
	torn := rawWALEntry(t, 10, `INSERT INTO kv VALUES (?, ?)`, 10, "v")
	records = append(records, torn[:len(torn)/2]) // half a record
	write(walPath, records)

	count := func() int {
		w, err := memdb.OpenWAL(walPath)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		if err := w.Replay(func(memdb.WALEntry) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := count(); got != 9 {
		t.Fatalf("first replay saw %d records, want 9", got)
	}

	// Five more acknowledged writes land after the (now truncated) torn tail.
	var later [][]byte
	for i := 11; i <= 15; i++ {
		later = append(later, rawWALEntry(t, uint64(i), `INSERT INTO kv VALUES (?, ?)`, i, "v"))
	}
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range later {
		if _, err := f.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if got := count(); got != 14 {
		t.Fatalf("second replay saw %d records, want 14 (9 + 5 later writes)", got)
	}
}

func walTestConfig(t *testing.T) memdb.Config {
	t.Helper()
	return memdb.Config{
		FilePath:      filepath.Join(t.TempDir(), "w.db"),
		FlushInterval: -1,
		Durability:    memdb.DurabilityWAL,
		Logger:        silentLogger(),
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
	}
}

// TestWAL_ReplayOverSnapshotSkipsWhatTheSnapshotHolds simulates a crash between
// the snapshot's rename and the WAL truncate: the snapshot holds two rows, and
// the WAL still holds the entries that wrote them. Reopen must not re-apply
// those entries (a plain INSERT would hit a UNIQUE constraint), and the rows
// survive.
func TestWAL_ReplayOverSnapshotSkipsWhatTheSnapshotHolds(t *testing.T) {
	cfg := walTestConfig(t)
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2"} {
		if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, k, "v"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The crash window: the WAL (as it was before the truncate) holds the two
	// writes the snapshot already contains. Re-append them, with sequence
	// numbers below the snapshot's checkpoint.
	walPath := cfg.FilePath + ".wal"
	w, err := memdb.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range []string{"k1", "k2"} {
		if err := w.Append(memdb.WALEntry{Seq: uint64(i + 1), SQL: `INSERT INTO kv (key, value) VALUES (?, ?)`, Args: []any{k, "v"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("reopen after a crash between snapshot and truncate: %v", err)
	}
	defer func() { _ = db2.Close() }()
	for _, k := range []string{"k1", "k2"} {
		var v string
		if err := db2.QueryRow(`SELECT value FROM kv WHERE key = ?`, k).Scan(&v); err != nil || v != "v" {
			t.Errorf("%s = %q, %v", k, v, err)
		}
	}
}

// TestWAL_ReplayFromSkipsAndSeeds checks ReplayFrom directly: entries at or
// below the checkpoint are not replayed, and the sequence counter is seeded to
// the checkpoint so later appends use fresh numbers.
func TestWAL_ReplayFromSkipsAndSeeds(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "w.wal")
	w, err := memdb.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		if err := w.Append(memdb.WALEntry{Seq: uint64(i), SQL: "nop"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := memdb.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	var got []uint64
	if err := w2.ReplayFrom(2, func(e memdb.WALEntry) error { got = append(got, e.Seq); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("replayed %v, want [3 4]", got)
	}
	if seq := w2.NextSeq(); seq != 5 {
		t.Errorf("NextSeq after a checkpointed replay = %d, want 5", seq)
	}
}

// blockingBackend is an in-memory ExternalBackend whose Write waits for a
// release, so a test can hold a Flush open at a deterministic point.
type blockingBackend struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	data    []byte
}

func newBlockingBackend() *blockingBackend {
	return &blockingBackend{entered: make(chan struct{}, 1), release: make(chan struct{}, 1)}
}

func (b *blockingBackend) Exists(context.Context) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data != nil, nil
}

// Write drains the snapshot fully, then signals that it has the bytes and
// waits for the release. Held here, the Flush sits between "the snapshot was
// written" and "the WAL was truncated" — the exact crash window — and a
// concurrent write must not slip into it.
func (b *blockingBackend) Write(_ context.Context, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	// Signal without blocking: the test consumes only the first Flush's signal;
	// later flushes (Close, reopen) must not block on a full channel.
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = data
	return nil
}

func (b *blockingBackend) Read(context.Context) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.data == nil {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

// TestWAL_FlushDoesNotLoseAConcurrentWrite holds a Flush open and issues a
// write during it. The write must not complete until the Flush does, and both
// must be durable afterwards: the write cannot land in the window between the
// snapshot and the truncate.
func TestWAL_FlushDoesNotLoseAConcurrentWrite(t *testing.T) {
	in := newBlockingBackend()
	// FilePath stays set so the WAL exists beside it; the wrapped backend owns
	// the snapshot, which is how a custom backend and DurabilityWAL combine.
	cfg := walTestConfig(t)
	cfg.Backend = memdb.WrapBackend(in)

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('k1', 'v')`); err != nil {
		t.Fatal(err)
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- db.Flush(context.Background()) }()
	<-in.entered // the backend's Write is now blocked

	wrote := make(chan error, 1)
	go func() {
		_, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('k2', 'v')`)
		wrote <- err
	}()
	select {
	case err := <-wrote:
		t.Fatalf("a write during a Flush completed before the Flush: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(in.release)
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := <-wrote; err != nil {
		t.Fatalf("the write after the Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen on the same backend: both rows survive.
	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	for _, k := range []string{"k1", "k2"} {
		var v string
		if err := db2.QueryRow(`SELECT value FROM kv WHERE key = ?`, k).Scan(&v); err != nil || v != "v" {
			t.Errorf("%s = %q, %v; the concurrent write was lost", k, v, err)
		}
	}
}
