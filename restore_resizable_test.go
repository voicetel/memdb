package memdb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// helper: open a temp DB with the given config; FilePath is auto-filled
// from t.TempDir if the caller did not set it. The returned DB is
// auto-closed at test cleanup.
func openTestDB(t *testing.T, cfg Config) *DB {
	t.Helper()
	if cfg.FilePath == "" {
		cfg.FilePath = filepath.Join(t.TempDir(), "test.db")
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("memdb.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedSnapshot opens a fresh DB, creates a table, inserts numRows wide
// rows, and returns the Serialize'd bytes plus the row count actually
// written.
func seedSnapshot(t *testing.T, numRows int) []byte {
	t.Helper()
	src := openTestDB(t, Config{
		FilePath:      filepath.Join(t.TempDir(), "src.db"),
		FlushInterval: -1,
	})
	if _, err := src.Exec(`CREATE TABLE kv (k INTEGER PRIMARY KEY, v BLOB NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// 1 KB payload per row → numRows KB of user data plus SQLite overhead.
	payload := make([]byte, 1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	for i := 0; i < numRows; i++ {
		if _, err := src.Exec(`INSERT INTO kv (k, v) VALUES (?, ?)`, i, payload); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	data, err := src.Serialize()
	if err != nil {
		t.Fatalf("src.Serialize: %v", err)
	}
	if len(data) < 16 || string(data[:16]) != string(sqliteFileHeader) {
		t.Fatalf("seeded snapshot does not start with SQLite magic")
	}
	return data
}

// TestRestore_GrowsPastSnapshotSize is the regression test for the
// SQLITE_FULL bug fixed in v1.9.0. Before the fix, sqlite3_deserialize
// without SQLITE_DESERIALIZE_RESIZEABLE froze the database page cap at
// len(snapshot) and every post-Restore write returned "database or disk
// is full". With the fix, the database can grow past the snapshot size
// up to RestoreMaxBytes / page_size pages.
func TestRestore_GrowsPastSnapshotSize(t *testing.T) {
	t.Parallel()
	snap := seedSnapshot(t, 64) // ~64 KB of user data
	dst := openTestDB(t, Config{FlushInterval: -1})

	if err := dst.Restore(snap); err != nil {
		t.Fatalf("dst.Restore: %v", err)
	}

	// Write 10x as many rows as were in the snapshot. Pre-fix this would
	// reliably fail with SQLITE_FULL on roughly the (snapshot-fits)+1th row.
	const extraRows = 640
	payload := make([]byte, 1024)
	for i := 0; i < extraRows; i++ {
		if _, err := dst.Exec(`INSERT INTO kv (k, v) VALUES (?, ?)`, 1_000_000+i, payload); err != nil {
			t.Fatalf("post-restore insert %d failed (likely SQLITE_FULL): %v", i, err)
		}
	}

	// Sanity-check the row count to be sure the inserts actually landed
	// and the test didn't pass on a silently-skipped insert.
	var count int
	if err := dst.QueryRow(`SELECT COUNT(*) FROM kv`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if want := 64 + extraRows; count != want {
		t.Fatalf("row count after restore+inserts: got %d want %d", count, want)
	}
}

// TestRestore_MaxPageCountApplied verifies that Config.RestoreMaxBytes
// is honoured via PRAGMA max_page_count: writes past the cap should
// receive SQLITE_FULL, even though the deserialize itself is now
// RESIZEABLE.
func TestRestore_MaxPageCountApplied(t *testing.T) {
	t.Parallel()
	snap := seedSnapshot(t, 16) // small snapshot
	// Pin RestoreMaxBytes to roughly half of restoreMaxBytesFloor so we
	// fit comfortably inside the floor (which guarantees a writable DB
	// at the smallest sensible size) but still hit the cap within a
	// reasonable insert count. The exact byte count is chosen so the
	// page-count cap can be reached without spending the entire test
	// budget inserting rows: 1 MiB / 4 KiB page = 256 pages.
	dst := openTestDB(t, Config{
		FlushInterval:   -1,
		RestoreMaxBytes: 1 << 20, // 1 MiB
	})
	if err := dst.Restore(snap); err != nil {
		t.Fatalf("dst.Restore: %v", err)
	}

	// Confirm the PRAGMA actually took: max_page_count should be (cap / page_size).
	var pageSize, maxPages int64
	if err := dst.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatalf("read page_size: %v", err)
	}
	if err := dst.QueryRow(`PRAGMA max_page_count`).Scan(&maxPages); err != nil {
		t.Fatalf("read max_page_count: %v", err)
	}
	wantMaxPages := int64(1<<20) / pageSize
	if maxPages != wantMaxPages {
		t.Fatalf("max_page_count: got %d, want %d (page_size=%d, RestoreMaxBytes=1MiB)",
			maxPages, wantMaxPages, pageSize)
	}

	// Now write rows until we hit the cap. Each row is ~1 KB user data
	// plus SQLite per-row overhead, so we'll bust the 1 MiB cap well
	// before this loop completes.
	payload := make([]byte, 1024)
	var sawFull bool
	for i := 0; i < 5000; i++ {
		_, err := dst.Exec(`INSERT INTO kv (k, v) VALUES (?, ?)`, 1_000_000+i, payload)
		if err == nil {
			continue
		}
		if strings.Contains(strings.ToLower(err.Error()), "full") ||
			strings.Contains(err.Error(), "SQLITE_FULL") {
			sawFull = true
			break
		}
		t.Fatalf("unexpected non-FULL error at row %d: %v", i, err)
	}
	if !sawFull {
		t.Fatalf("expected SQLITE_FULL once the 1 MiB cap was reached; never saw it after 5000 inserts")
	}
}

// TestRestore_MaxBytesBelowSnapshot pins the behaviour when
// RestoreMaxBytes is configured *below* the snapshot's already-used
// size. SQLite rounds max_page_count up to the current page count, so
// the load succeeds, the cap is set at the current usage, and no
// growth is permitted.
func TestRestore_MaxBytesBelowSnapshot(t *testing.T) {
	t.Parallel()
	snap := seedSnapshot(t, 200) // ~200 KB user data + overhead
	dst := openTestDB(t, Config{
		FlushInterval:   -1,
		RestoreMaxBytes: 4096, // 1 page — far below the snapshot
	})
	if err := dst.Restore(snap); err != nil {
		t.Fatalf("dst.Restore: %v", err)
	}

	// Existing rows must still be readable — the snapshot loaded.
	var count int
	if err := dst.QueryRow(`SELECT COUNT(*) FROM kv`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 200 {
		t.Fatalf("post-restore row count: got %d want 200", count)
	}

	// Writes must eventually fail — the cap was rounded up to current
	// usage, so once SQLite needs to allocate a new page (rather than
	// fit into slack on the existing last page), SQLITE_FULL fires.
	// A single insert may or may not trigger growth depending on
	// per-page slack, so loop until either FULL is observed or a
	// generous upper bound is exceeded.
	payload := make([]byte, 1024)
	var sawFull bool
	for i := 0; i < 5000; i++ {
		_, err := dst.Exec(`INSERT INTO kv (k, v) VALUES (?, ?)`, 9_000_000+i, payload)
		if err == nil {
			continue
		}
		if strings.Contains(strings.ToLower(err.Error()), "full") {
			sawFull = true
			break
		}
		t.Fatalf("unexpected non-FULL error at row %d: %v", i, err)
	}
	if !sawFull {
		t.Fatalf("expected SQLITE_FULL once growth past the snapshot's page count was needed; never saw it")
	}
}

// TestRestore_RejectsCorruptInput exercises the 16-byte magic-byte
// check on the FSM Restore path. Random or short bytes should return
// ErrSnapshotCorrupt without reaching sqlite3_deserialize.
func TestRestore_RejectsCorruptInput(t *testing.T) {
	t.Parallel()
	dst := openTestDB(t, Config{FlushInterval: -1})

	cases := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"short_header", []byte("SQLite format")},
		{"wrong_magic", []byte("not a sqlite db at all yo")},
		{"random_16", func() []byte { b := make([]byte, 16); _, _ = rand.Read(b); return b }()},
		// Just the header with no body should also be rejected — the
		// magic check passes but sqlite3_deserialize will refuse a
		// 16-byte database. Either path is acceptable; we only care
		// that the writer connection survives.
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := dst.Restore(tc.data)
			if !errors.Is(err, ErrSnapshotCorrupt) {
				t.Errorf("Restore(%s): want ErrSnapshotCorrupt, got %v", tc.name, err)
			}
		})
	}

	// After the rejections, the connection must remain usable.
	if _, err := dst.Exec(`CREATE TABLE post_corruption_check (id INTEGER)`); err != nil {
		t.Fatalf("writer broken after corrupt-input rejections: %v", err)
	}
}

// TestRestore_TruncatedHeader covers the edge case where the magic
// prefix matches but the payload is too short to be a real database
// (less than the 100-byte SQLite file header). Restore must reject it
// with ErrSnapshotCorrupt rather than letting sqlite3_deserialize
// install bytes that would corrupt the connection on the next query.
func TestRestore_TruncatedHeader(t *testing.T) {
	t.Parallel()
	dst := openTestDB(t, Config{FlushInterval: -1})

	// Magic prefix + a handful of zero bytes — still way under the
	// 100-byte SQLite file header.
	short := make([]byte, 32)
	copy(short, sqliteFileHeader)

	if err := dst.Restore(short); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("Restore(short): want ErrSnapshotCorrupt, got %v", err)
	}
	// Writer must still be usable — the early reject means
	// sqlite3_deserialize never ran on bad bytes.
	if _, err := dst.Exec(`CREATE TABLE post_short (id INTEGER)`); err != nil {
		t.Fatalf("writer broken after short-payload Restore: %v", err)
	}
}

// TestSQLiteConnMirrorLayout is the layout-guard self-test. It opens a
// fresh memdb, extracts the *C.sqlite3 handle through the unsafe
// mirror, and asks SQLite a benign question through it (via
// verifyLayout, which calls sqlite3_db_filename). If the mirror's
// offset drifts (because mattn/go-sqlite3 changed its struct), this
// test crashes or returns a clear error.
func TestSQLiteConnMirrorLayout(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{FlushInterval: -1})

	if err := withRawConn(context.Background(), db.mem, func(conn *sqlite3.SQLiteConn) error {
		handle := rawConnHandle(conn)
		if handle == nil {
			return fmt.Errorf("rawConnHandle returned nil — mattn SQLiteConn layout drift?")
		}
		// verifyLayout uses a sync.Once and runs at most once per
		// process, so call it again here for documentation purposes;
		// it will be a fast no-op on the second call.
		return verifyLayout(conn)
	}); err != nil {
		t.Fatalf("layout guard: %v", err)
	}
}

// TestReplicaRefresh_HandlesWriterGrowth reframed from the brief's
// test #2 — this is a general refresh-correctness regression, not a
// RESIZEABLE test (replicas are read-only by intent and cannot grow).
// It pins that replica reads stay correct as the writer accumulates
// rows between refresh ticks: small initial state, force-refresh,
// large additional state, force-refresh, verify replica row counts.
func TestReplicaRefresh_HandlesWriterGrowth(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{
		FlushInterval:          -1,
		ReadPoolSize:           4,
		ReplicaRefreshInterval: 20 * time.Millisecond,
	})
	if _, err := db.Exec(`CREATE TABLE n (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Phase 1: write one row, wait two refresh ticks, expect 1 on replicas.
	if _, err := db.Exec(`INSERT INTO n (id) VALUES (?)`, 1); err != nil {
		t.Fatalf("phase1 insert: %v", err)
	}
	waitForReplicaCount(t, db, 1, 2*time.Second)

	// Phase 2: write 1000 more, growing the serialised image
	// materially between refresh ticks. Replicas must converge to
	// 1001 within a reasonable window. Pre-RESIZEABLE refactor this
	// path exercised the same code; the test acts as a regression
	// guard against future divergence.
	for i := 2; i <= 1001; i++ {
		if _, err := db.Exec(`INSERT INTO n (id) VALUES (?)`, i); err != nil {
			t.Fatalf("phase2 insert %d: %v", i, err)
		}
	}
	waitForReplicaCount(t, db, 1001, 2*time.Second)
}

// waitForReplicaCount polls the read-replica path (db.Query / QueryRow
// will use a checked-out replica when ReadPoolSize > 0 and the
// goroutine doesn't yield to a refresh) for up to timeout, asserting
// the count matches want. "no such table" errors during the poll are
// treated as "replica is stale, keep trying" rather than a fatal —
// they fire when the writer has CREATE TABLEd but the replica hasn't
// refreshed yet.
func waitForReplicaCount(t *testing.T, db *DB, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastSeen int
	var lastErr error
	for time.Now().Before(deadline) {
		var got int
		err := db.QueryRow(`SELECT COUNT(*) FROM n`).Scan(&got)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				lastErr = err
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatalf("query: %v", err)
		}
		if got == want {
			return
		}
		lastSeen = got
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replica count never converged to %d within %s (last seen %d, lastErr %v)", want, timeout, lastSeen, lastErr)
}
