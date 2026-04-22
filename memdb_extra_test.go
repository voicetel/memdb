package memdb_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/backends"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func extraTestConfig(t *testing.T) memdb.Config {
	t.Helper()
	return memdb.Config{
		FilePath:      filepath.Join(t.TempDir(), "extra-test.db"),
		FlushInterval: -1,
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
	}
}

func mustOpen(t *testing.T, cfg memdb.Config) *memdb.DB {
	t.Helper()
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ── Open validation ───────────────────────────────────────────────────────────

func TestOpen_ValidationError(t *testing.T) {
	// No FilePath and no Backend — must return an error containing "required".
	_, err := memdb.Open(memdb.Config{})
	if err == nil {
		t.Fatal("expected error from Open with empty Config, got nil")
	}
	if !strings.Contains(err.Error(), "required") {
		t.Errorf("expected error to contain 'required', got: %v", err)
	}
}

func TestOpen_InitSchemaError(t *testing.T) {
	sentinel := errors.New("schema init failed")
	cfg := memdb.Config{
		FilePath:      filepath.Join(t.TempDir(), "schema-err.db"),
		FlushInterval: -1,
		InitSchema: func(_ *memdb.DB) error {
			return sentinel
		},
	}
	_, err := memdb.Open(cfg)
	if err == nil {
		t.Fatal("expected Open to return an error when InitSchema fails")
	}
	if !errors.Is(err, sentinel) && !strings.Contains(err.Error(), sentinel.Error()) {
		t.Errorf("expected error to wrap sentinel, got: %v", err)
	}
}

// ── DB accessor ───────────────────────────────────────────────────────────────

func TestDB_DB_Accessor(t *testing.T) {
	db := mustOpen(t, extraTestConfig(t))

	sqlDB := db.DB()
	if sqlDB == nil {
		t.Fatal("DB() returned nil")
	}

	var n int
	if err := sqlDB.QueryRow("SELECT 1").Scan(&n); err != nil {
		t.Fatalf("QueryRow via DB(): %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

// ── transactions ──────────────────────────────────────────────────────────────

func TestDB_Begin(t *testing.T) {
	db := mustOpen(t, extraTestConfig(t))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if tx == nil {
		t.Fatal("Begin returned nil tx")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestDB_BeginTx(t *testing.T) {
	db := mustOpen(t, extraTestConfig(t))
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if tx == nil {
		t.Fatal("BeginTx returned nil tx")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestDB_Begin_Closed(t *testing.T) {
	db, err := memdb.Open(extraTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err = db.Begin()
	if !errors.Is(err, memdb.ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestDB_BeginTx_Closed(t *testing.T) {
	db, err := memdb.Open(extraTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err = db.BeginTx(context.Background(), nil)
	if !errors.Is(err, memdb.ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestDB_Query_Closed(t *testing.T) {
	db, err := memdb.Open(extraTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err = db.Query("SELECT 1")
	if !errors.Is(err, memdb.ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

// ── Close idempotency ─────────────────────────────────────────────────────────

func TestDB_Close_Idempotent(t *testing.T) {
	db, err := memdb.Open(extraTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second call must not panic and should return nil (stopOnce no-op).
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// ── Flush after close ─────────────────────────────────────────────────────────

func TestDB_Flush_Closed(t *testing.T) {
	db, err := memdb.Open(extraTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	ctx := context.Background()
	if err := db.Flush(ctx); !errors.Is(err, memdb.ErrClosed) {
		t.Errorf("expected ErrClosed from Flush on closed DB, got %v", err)
	}
}

// ── OnFlushComplete callback ──────────────────────────────────────────────────

func TestFlushMetrics_OnFlushComplete(t *testing.T) {
	var called bool
	var gotMetrics memdb.FlushMetrics

	cfg := extraTestConfig(t)
	cfg.OnFlushComplete = func(m memdb.FlushMetrics) {
		called = true
		gotMetrics = m
	}

	db := mustOpen(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if !called {
		t.Fatal("OnFlushComplete was not called")
	}
	if gotMetrics.Duration <= 0 {
		t.Errorf("expected Duration > 0, got %v", gotMetrics.Duration)
	}
	if gotMetrics.Error != nil {
		t.Errorf("expected nil Error in FlushMetrics, got %v", gotMetrics.Error)
	}
}

// ── DurabilityWAL ─────────────────────────────────────────────────────────────

func TestDurabilityWAL_ExecAppendsToWAL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal-test.db")

	cfg := memdb.Config{
		FilePath:      dbPath,
		FlushInterval: -1,
		Durability:    memdb.DurabilityWAL,
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
	}
	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('wal-key', 'wal-val')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	walPath := dbPath + ".wal"

	// WAL file should exist and be non-empty.
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file not found at %s: %v", walPath, err)
	}
	if info.Size() == 0 {
		t.Error("expected WAL file to be non-empty after Exec")
	}

	// Flush should truncate the WAL.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	info2, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file disappeared after flush: %v", err)
	}
	if info2.Size() != 0 {
		t.Errorf("expected WAL file to be empty after flush, got size %d", info2.Size())
	}
}

func TestDurabilityWAL_RestoreIncludesWALReplay(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal-restore.db")

	cfg := memdb.Config{
		FilePath:      dbPath,
		FlushInterval: -1,
		Durability:    memdb.DurabilityWAL,
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
	}

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('replay-key', 'replay-val')`); err != nil {
		db.Close()
		t.Fatalf("Exec: %v", err)
	}

	// Close calls Flush internally, which also truncates the WAL.
	// But the snapshot will contain the row.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — row must be present after restore.
	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	var val string
	if err := db2.QueryRow(`SELECT value FROM kv WHERE key = 'replay-key'`).Scan(&val); err != nil {
		t.Fatalf("QueryRow after restore: %v", err)
	}
	if val != "replay-val" {
		t.Errorf("expected 'replay-val', got %q", val)
	}
}

// ── DurabilitySync ───────────────────────────────────────────────────────────

func TestDurabilitySync_Works(t *testing.T) {
	cfg := extraTestConfig(t)
	cfg.Durability = memdb.DurabilitySync

	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('sync-key', 'sync-val')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	var val string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = 'sync-key'`).Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != "sync-val" {
		t.Errorf("expected 'sync-val', got %q", val)
	}
}

// ── Config defaults ───────────────────────────────────────────────────────────

func TestConfig_Defaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "defaults.db")
	// Open with only FilePath set — applyDefaults fills in the rest.
	db, err := memdb.Open(memdb.Config{FilePath: path})
	if err != nil {
		t.Fatalf("Open with default config: %v", err)
	}
	defer db.Close()

	// Verify the DB is usable.
	var n int
	if err := db.DB().QueryRow("SELECT 1").Scan(&n); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

func TestConfig_NegativeFlushInterval_Disabled(t *testing.T) {
	cfg := extraTestConfig(t)
	cfg.FlushInterval = -1 // should be normalised to 0, disabling background flush

	// mustOpen registers a t.Cleanup for db.Close — no background goroutine
	// should panic.
	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('ni', 'nv')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

// ── ChangeEvent.RowID ─────────────────────────────────────────────────────────

func TestChangeEvent_RowID(t *testing.T) {
	var events []memdb.ChangeEvent
	cfg := extraTestConfig(t)
	// Use a distinct BusyTimeout so this config hashes to a different driver
	// name than TestChangeHook (which uses the default 5000 ms). The driver
	// registration in registerDriver captures the OnChange closure at
	// registration time; reusing the same driver across tests would cause the
	// wrong closure to fire.
	cfg.BusyTimeout = 5001
	cfg.OnChange = func(e memdb.ChangeEvent) {
		events = append(events, e)
	}

	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('ev-key', 'ev-val')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected at least one ChangeEvent")
	}
	if events[0].RowID <= 0 {
		t.Errorf("expected RowID > 0, got %d", events[0].RowID)
	}
	if events[0].Op != "INSERT" {
		t.Errorf("expected Op='INSERT', got %q", events[0].Op)
	}
}

// ── WAL unit tests ────────────────────────────────────────────────────────────

func TestWAL_Truncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	// Append 3 entries.
	for i := 0; i < 3; i++ {
		entry := memdb.WALEntry{
			Seq:       w.NextSeq(),
			Timestamp: time.Now().UnixNano(),
			SQL:       "INSERT INTO kv (key, value) VALUES (?, ?)",
			Args:      []any{"k", "v"},
		}
		if err := w.Append(entry); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Truncate the WAL.
	if err := w.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Replay should yield 0 entries.
	count := 0
	if err := w.Replay(func(_ memdb.WALEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Replay after Truncate: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries after truncate, got %d", count)
	}
}

func TestWAL_Close(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close.wal")
	w, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	entry := memdb.WALEntry{
		Seq:       w.NextSeq(),
		Timestamp: time.Now().UnixNano(),
		SQL:       "SELECT 1",
	}
	if err := w.Append(entry); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// File must still exist on disk after close.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("WAL file missing after Close: %v", err)
	}
}

func TestWAL_Replay_Empty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	w, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	count := 0
	if err := w.Replay(func(_ memdb.WALEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Replay on empty WAL: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries, got %d", count)
	}
}

func TestWAL_Replay_CorruptEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")

	// Write garbage bytes that are not valid gob data.
	if err := os.WriteFile(path, []byte("this is not valid gob data!!!"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	w, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	err = w.Replay(func(_ memdb.WALEntry) error { return nil })
	if err == nil {
		t.Error("expected an error replaying a corrupt WAL, got nil")
	}
}

// ── WrapBackend end-to-end ────────────────────────────────────────────────────

func TestWrapBackend_FlushAndRestore(t *testing.T) {
	dir := t.TempDir()
	backendPath := filepath.Join(dir, "wrapped.db")

	lb := &backends.LocalBackend{Path: backendPath}
	backend := memdb.WrapBackend(lb)

	openCfg := func() memdb.Config {
		return memdb.Config{
			Backend:       backend,
			FlushInterval: -1,
			InitSchema: func(db *memdb.DB) error {
				_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
				return err
			},
		}
	}

	// Open, insert, flush, close.
	db, err := memdb.Open(openCfg())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('wrapped-key', 'wrapped-val')`); err != nil {
		db.Close()
		t.Fatalf("Exec: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		db.Close()
		t.Fatalf("Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with same backend — row must be visible.
	db2, err := memdb.Open(openCfg())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	var val string
	if err := db2.QueryRow(`SELECT value FROM kv WHERE key = 'wrapped-key'`).Scan(&val); err != nil {
		t.Fatalf("QueryRow after restore: %v", err)
	}
	if val != "wrapped-val" {
		t.Errorf("expected 'wrapped-val', got %q", val)
	}
}

// ── WithTx convenience helper ─────────────────────────────────────────────────

// TestDB_WithTx_OnClosedDB verifies that WithTx propagates ErrClosed when the
// underlying DB has been shut down.
func TestExec_WithOnExec_SkipsLocalWrite(t *testing.T) {
	cfg := testConfig(t)
	var called bool
	var gotSQL string
	cfg.OnExec = func(sql string, args []any) error {
		called = true
		gotSQL = sql
		return nil
	}
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "k", "v"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !called {
		t.Error("OnExec was not called")
	}
	if gotSQL != `INSERT INTO kv (key, value) VALUES (?, ?)` {
		t.Errorf("unexpected SQL: %q", gotSQL)
	}
	// Row should NOT be in local DB — Raft would apply it via ExecDirect.
	var val string
	err = db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "k").Scan(&val)
	if err == nil {
		t.Error("expected row to NOT be in local DB when OnExec is set, but it was found")
	}
}

func TestBegin_WithOnExec_ReturnsError(t *testing.T) {
	cfg := testConfig(t)
	cfg.OnExec = func(_ string, _ []any) error { return nil }
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Begin()
	if !errors.Is(err, memdb.ErrTransactionNotSupported) {
		t.Errorf("expected ErrTransactionNotSupported, got: %v", err)
	}
}

func TestDB_WithTx_OnClosedDB(t *testing.T) {
	db, err := memdb.Open(extraTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	err = memdb.WithTx(context.Background(), db, func(_ *sql.Tx) error {
		return nil
	})
	if !errors.Is(err, memdb.ErrClosed) {
		t.Errorf("expected ErrClosed from WithTx on closed DB, got %v", err)
	}
}
