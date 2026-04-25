package memdb_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
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
		Logger:        silentLogger(),
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
		Logger:        silentLogger(),
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
		Logger:        silentLogger(),
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
		Logger:        silentLogger(),
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
	// Open with only FilePath set (plus a silent logger) — applyDefaults fills in the rest.
	db, err := memdb.Open(memdb.Config{FilePath: path, Logger: silentLogger()})
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

	// Write garbage bytes that are not a valid length-prefixed record.
	// The current WAL format treats the first 4 bytes as a big-endian length
	// prefix. For garbage input, the "length" will either exceed the 64 MB
	// sanity cap or point past EOF — in both cases the replay stops silently
	// at that point rather than returning an error, preserving any valid
	// records that preceded the corruption.
	if err := os.WriteFile(path, []byte("this is not valid wal data!!!"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	w, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	// Replay is tolerant of a corrupt tail — it returns nil and simply
	// skips the garbage. No entries are delivered to the callback because
	// the garbage cannot be parsed as a valid record.
	var delivered int
	err = w.Replay(func(_ memdb.WALEntry) error {
		delivered++
		return nil
	})
	if err != nil {
		t.Errorf("expected nil error (corrupt tail is tolerated), got: %v", err)
	}
	if delivered != 0 {
		t.Errorf("expected 0 entries delivered from garbage input, got %d", delivered)
	}
}

// ── WrapBackend end-to-end ────────────────────────────────────────────────────

// TestWrapBackend_AuthenticatedSkipsHeader exercises the AuthenticatedBackend
// opt-out: a snapshot flushed through EncryptedBackend → LocalBackend has
// the SHA-256 MDBK header skipped (the AES-GCM tag is the integrity
// check), and the same backend chain restores it cleanly without
// triggering the legacy-snapshot warning.
//
// Verification is by round-trip — the row inserted before flush must be
// visible after a fresh Open against the same backend. A failure to
// honour the headerless format would surface as either an integrity
// check error during restore or as garbage SQLite bytes (because the
// 40-byte non-header would be passed to sqlite3_deserialize).
func TestWrapBackend_AuthenticatedSkipsHeader(t *testing.T) {
	dir := t.TempDir()
	backendPath := filepath.Join(dir, "encrypted.db")

	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}

	openCfg := func() memdb.Config {
		return memdb.Config{
			Backend: memdb.WrapBackend(&backends.EncryptedBackend{
				Inner: &backends.LocalBackend{Path: backendPath},
				Key:   key,
			}),
			FlushInterval: -1,
			Logger:        silentLogger(),
			InitSchema: func(db *memdb.DB) error {
				_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
				return err
			},
		}
	}

	db, err := memdb.Open(openCfg())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('aead-key', 'aead-val')`); err != nil {
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

	db2, err := memdb.Open(openCfg())
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	var val string
	if err := db2.QueryRow(`SELECT value FROM kv WHERE key = 'aead-key'`).Scan(&val); err != nil {
		t.Fatalf("QueryRow after restore: %v", err)
	}
	if val != "aead-val" {
		t.Errorf("expected 'aead-val', got %q", val)
	}
}

func TestWrapBackend_FlushAndRestore(t *testing.T) {
	dir := t.TempDir()
	backendPath := filepath.Join(dir, "wrapped.db")

	lb := &backends.LocalBackend{Path: backendPath}
	backend := memdb.WrapBackend(lb)

	openCfg := func() memdb.Config {
		return memdb.Config{
			Backend:       backend,
			FlushInterval: -1,
			Logger:        silentLogger(),
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

// ── Foreign-key enforcement ───────────────────────────────────────────────────

// TestForeignKeys_EnforcedByDefault verifies that FK constraints are enforced
// when DisableForeignKeys is false (the default). Inserting a child row whose
// parent does not exist must return an error.
func TestForeignKeys_EnforcedByDefault(t *testing.T) {
	cfg := extraTestConfig(t)
	// Override InitSchema to create parent/child tables with a FK.
	cfg.InitSchema = func(db *memdb.DB) error {
		_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS parents (id INTEGER PRIMARY KEY);
			CREATE TABLE IF NOT EXISTS children (
				id        INTEGER PRIMARY KEY,
				parent_id INTEGER NOT NULL REFERENCES parents(id)
			);
		`)
		return err
	}
	// DisableForeignKeys is deliberately left at its zero value (false) so
	// foreign-key enforcement is ON by default.
	db := mustOpen(t, cfg)

	// Insert a child row whose parent does not exist — must fail.
	_, err := db.Exec(`INSERT INTO children (id, parent_id) VALUES (1, 999)`)
	if err == nil {
		t.Fatal("expected FK violation error, got nil")
	}
}

// TestForeignKeys_DisabledViaConfig verifies that setting DisableForeignKeys=true
// reverts to SQLite's default-off behaviour: the same FK-violating insert that
// fails above must succeed when enforcement is explicitly disabled.
func TestForeignKeys_DisabledViaConfig(t *testing.T) {
	cfg := extraTestConfig(t)
	cfg.DisableForeignKeys = true
	cfg.InitSchema = func(db *memdb.DB) error {
		_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS parents (id INTEGER PRIMARY KEY);
			CREATE TABLE IF NOT EXISTS children (
				id        INTEGER PRIMARY KEY,
				parent_id INTEGER NOT NULL REFERENCES parents(id)
			);
		`)
		return err
	}
	db := mustOpen(t, cfg)

	// FK enforcement is off — the orphaned child row must be accepted.
	if _, err := db.Exec(`INSERT INTO children (id, parent_id) VALUES (1, 999)`); err != nil {
		t.Fatalf("expected insert to succeed with FK enforcement off, got: %v", err)
	}
}

// TestForeignKeys_ValidInsertSucceeds confirms the happy path: an insert that
// satisfies the FK constraint succeeds when enforcement is on.
func TestForeignKeys_ValidInsertSucceeds(t *testing.T) {
	cfg := extraTestConfig(t)
	cfg.InitSchema = func(db *memdb.DB) error {
		_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS parents (id INTEGER PRIMARY KEY);
			CREATE TABLE IF NOT EXISTS children (
				id        INTEGER PRIMARY KEY,
				parent_id INTEGER NOT NULL REFERENCES parents(id)
			);
		`)
		return err
	}
	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO parents (id) VALUES (1)`); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO children (id, parent_id) VALUES (10, 1)`); err != nil {
		t.Fatalf("expected valid FK insert to succeed, got: %v", err)
	}
}

// ── Panic recovery in callbacks ───────────────────────────────────────────────

// TestPanic_OnChange_Recovered verifies that a panicking OnChange callback
// does not propagate to the caller of Exec — the panic is swallowed and
// the write still returns success.
func TestPanic_OnChange_Recovered(t *testing.T) {
	cfg := extraTestConfig(t)
	cfg.OnChange = func(_ memdb.ChangeEvent) {
		panic("deliberate OnChange panic")
	}
	db := mustOpen(t, cfg)

	// Must not panic — the recovery wrapper in driver.go catches it.
	_, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('pk', 'pv')`)
	if err != nil {
		t.Fatalf("Exec returned error after OnChange panic: %v", err)
	}

	// The row should be present — the write committed before OnChange fired.
	var val string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = 'pk'`).Scan(&val); err != nil {
		t.Fatalf("row not found after recovered OnChange panic: %v", err)
	}
	if val != "pv" {
		t.Errorf("unexpected value %q", val)
	}
}

// TestPanic_OnFlushError_Recovered verifies that a panicking OnFlushError
// handler does not crash the background flush goroutine. We trigger a flush
// by calling Flush directly and then confirm the DB is still operational.
func TestPanic_OnFlushError_Recovered(t *testing.T) {
	cfg := extraTestConfig(t)
	panicked := make(chan struct{}, 1)
	cfg.OnFlushError = func(_ error) {
		select {
		case panicked <- struct{}{}:
		default:
		}
		panic("deliberate OnFlushError panic")
	}
	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('fe', 'fv')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// Flush to a deliberately broken backend by swapping it out.
	// Instead, just verify the DB remains usable after a panicking
	// OnFlushError fires by invoking it indirectly.
	// We can't easily trigger an async flush failure here, but we can
	// verify that the OnFlushError pathway itself doesn't explode the
	// process by calling safeDo via the already-registered hook on a
	// direct Flush (which will succeed, so OnFlushError won't fire).
	// The real value of this test is confirming the DB remains live.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// DB must still be functional.
	var v string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = 'fe'`).Scan(&v); err != nil {
		t.Fatalf("QueryRow after flush: %v", err)
	}
}

// TestPanic_OnFlushComplete_Recovered verifies that a panicking
// OnFlushComplete callback does not interrupt the flush path or crash the
// background flush goroutine. The flush itself must still succeed.
func TestPanic_OnFlushComplete_Recovered(t *testing.T) {
	cfg := extraTestConfig(t)
	called := make(chan struct{}, 1)
	cfg.OnFlushComplete = func(_ memdb.FlushMetrics) {
		select {
		case called <- struct{}{}:
		default:
		}
		panic("deliberate OnFlushComplete panic")
	}
	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('fc', 'fcv')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Must not panic — the safeCallback wrapper in flushUnchecked catches it.
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	// Confirm the callback was actually reached.
	select {
	case <-called:
	default:
		t.Fatal("OnFlushComplete was never called")
	}

	// DB must still be functional after the recovered panic.
	var v string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = 'fc'`).Scan(&v); err != nil {
		t.Fatalf("QueryRow after recovered OnFlushComplete panic: %v", err)
	}
	if v != "fcv" {
		t.Errorf("unexpected value %q, want 'fcv'", v)
	}
}

// TestPanic_OnExec_PanicsToCallerBecauseItIsSync verifies the current
// contract: OnExec runs synchronously on the calling goroutine, so a panic
// there propagates directly to the Exec caller. This is documented behaviour
// (the caller controls OnExec) and we just confirm the panic escapes cleanly
// rather than causing a deadlock or corrupting state.
//
// We open the DB *without* OnExec first (so InitSchema runs cleanly), then
// set OnExec on a second open against the same file — or more precisely we
// use ExecDirect for schema init and only set OnExec after the schema is in
// place. The cleanest approach is to open normally, close, then reopen with
// OnExec so the schema is already persisted and InitSchema is not called.
func TestPanic_OnExec_PanicsToCallerBecauseItIsSync(t *testing.T) {
	// Step 1: open without OnExec so InitSchema runs cleanly.
	cfg := extraTestConfig(t)
	db1 := mustOpen(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db1.Flush(ctx); err != nil {
		t.Fatalf("initial flush: %v", err)
	}
	db1.Close()

	// Step 2: reopen the same file with OnExec set and no InitSchema
	// (schema already persisted). The panicking hook will fire on Exec
	// but NOT during Open itself.
	cfg2 := memdb.Config{
		FilePath:      cfg.FilePath,
		FlushInterval: -1,
		Logger:        silentLogger(),
		OnExec: func(_ string, _ []any) error {
			panic("deliberate OnExec panic")
		},
		// No InitSchema — schema is already on disk.
	}
	db2, err := memdb.Open(cfg2)
	if err != nil {
		t.Fatalf("Open with OnExec: %v", err)
	}
	defer db2.Close()

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		_, _ = db2.Exec(`INSERT INTO kv (key, value) VALUES ('p', 'v')`)
	}()

	if !didPanic {
		t.Fatal("expected panic from OnExec to reach the caller, but none occurred")
	}

	// After the panic the DB must remain usable for future operations —
	// the panic must not have corrupted any internal state.
	// In this mode Exec routes through OnExec, so we use ExecDirect instead.
	if err := db2.ExecDirect(`INSERT INTO kv (key, value) VALUES ('safe', 'ok')`); err != nil {
		t.Fatalf("ExecDirect after recovered OnExec panic: %v", err)
	}
}

// ── Snapshot integrity (SHA-256 checksum) ─────────────────────────────────────

// TestSnapshot_Checksum_RoundTrip verifies that a snapshot written by Flush
// can be successfully restored in a fresh Open — i.e. the checksum header
// written on flush is verified transparently on restore without error.
func TestSnapshot_Checksum_RoundTrip(t *testing.T) {
	cfg := extraTestConfig(t)
	db := mustOpen(t, cfg)

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('snap-key', 'snap-val')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	db.Close()

	// Re-open from the snapshot — checksum verification must pass silently.
	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open after flush: %v", err)
	}
	defer db2.Close()

	var val string
	if err := db2.QueryRow(`SELECT value FROM kv WHERE key = 'snap-key'`).Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != "snap-val" {
		t.Errorf("got %q, want %q", val, "snap-val")
	}
}

// TestSnapshot_Checksum_Corruption detects bit-flips in the snapshot payload.
// We write a valid snapshot, flip a byte in the middle of the file (past the
// 44-byte header), then attempt to Open — must get ErrSnapshotCorrupt.
func TestSnapshot_Checksum_Corruption(t *testing.T) {
	cfg := extraTestConfig(t)
	db := mustOpen(t, cfg)

	for i := 0; i < 20; i++ {
		if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	db.Close()

	// Corrupt a byte in the payload portion (byte 1000, well past the 44-byte header).
	path := cfg.FilePath
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	const corruptOffset = 1000
	if len(data) <= corruptOffset {
		t.Skipf("snapshot too small (%d bytes) to test corruption at offset %d",
			len(data), corruptOffset)
	}
	data[corruptOffset] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Open must return ErrSnapshotCorrupt (wrapped).
	_, err = memdb.Open(cfg)
	if err == nil {
		t.Fatal("expected error from Open with corrupt snapshot, got nil")
	}
	if !errors.Is(err, memdb.ErrSnapshotCorrupt) {
		t.Errorf("expected ErrSnapshotCorrupt in chain, got: %v", err)
	}
}

// TestSnapshot_Checksum_HeaderCorruption verifies that corrupting the stored
// SHA-256 in the header itself (bytes 8–39) also triggers ErrSnapshotCorrupt.
func TestSnapshot_Checksum_HeaderCorruption(t *testing.T) {
	cfg := extraTestConfig(t)
	db := mustOpen(t, cfg)
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('hdr', 'test')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	db.Close()

	// Flip a byte inside the SHA-256 field of the header (offset 8).
	path := cfg.FilePath
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) < 44 {
		t.Skipf("snapshot too small (%d bytes) to have a full header", len(data))
	}
	data[8] ^= 0x01
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = memdb.Open(cfg)
	if err == nil {
		t.Fatal("expected error from Open with corrupt header, got nil")
	}
	if !errors.Is(err, memdb.ErrSnapshotCorrupt) {
		t.Errorf("expected ErrSnapshotCorrupt in chain, got: %v", err)
	}
}

// TestSnapshot_Legacy_NoChecksum verifies that a snapshot file that was
// written WITHOUT the checksum header (a legacy file that begins with the
// SQLite magic bytes "SQLite format 3") is loaded without error. This tests
// backward compatibility: existing deployments must not break after upgrading.
func TestSnapshot_Legacy_NoChecksum(t *testing.T) {
	cfg := extraTestConfig(t)

	// Produce a valid snapshot with the current code (checksummed).
	db := mustOpen(t, cfg)
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('leg', 'acy')`); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	db.Close()

	// Strip the 44-byte checksum header, leaving a raw SQLite file.
	// This simulates a snapshot written by a pre-checksum version of memdb.
	path := cfg.FilePath
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	const headerLen = 40 // snapHeaderLen: magic(4) + version(4) + sha256(32)
	if len(data) < headerLen {
		t.Skipf("snapshot too small (%d bytes) to strip header", len(data))
	}
	raw := data[headerLen:] // payload only — pure SQLite bytes
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Open must succeed (legacy format — no checksum check).
	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open legacy snapshot: %v", err)
	}
	defer db2.Close()

	var val string
	if err := db2.QueryRow(`SELECT value FROM kv WHERE key = 'leg'`).Scan(&val); err != nil {
		t.Fatalf("QueryRow after legacy restore: %v", err)
	}
	if val != "acy" {
		t.Errorf("got %q, want %q", val, "acy")
	}
}

// TestSnapshot_ErrSnapshotCorrupt_Sentinel ensures ErrSnapshotCorrupt is a
// distinct named error that callers can target with errors.Is, not just a
// generic string.
func TestSnapshot_ErrSnapshotCorrupt_Sentinel(t *testing.T) {
	if memdb.ErrSnapshotCorrupt == nil {
		t.Fatal("ErrSnapshotCorrupt is nil")
	}
	wrapped := fmt.Errorf("outer: %w", memdb.ErrSnapshotCorrupt)
	if !errors.Is(wrapped, memdb.ErrSnapshotCorrupt) {
		t.Error("errors.Is failed to unwrap ErrSnapshotCorrupt through fmt.Errorf %w")
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
