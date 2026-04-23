package memdb_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"fmt"

	"github.com/voicetel/memdb"
)

// silentLogger returns a *slog.Logger that discards all output. Used in tests
// so that INFO-level flush/restore/WAL-replay events do not pollute test
// output with lines like "INFO memdb: flushed to backend ...".
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func TestReplicaPool_ReadsConverge(t *testing.T) {
	cfg := testConfig(t)
	cfg.ReadPoolSize = 4
	cfg.ReplicaRefreshInterval = 5 * time.Millisecond

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write a row via the writer connection.
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "pool-key", "pool-val"); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// The replicas are refreshed on a background ticker. Poll until the value
	// becomes visible or we exceed a generous timeout.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		var val string
		err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "pool-key").Scan(&val)
		if err == nil && val == "pool-val" {
			return // success
		}
		time.Sleep(cfg.ReplicaRefreshInterval)
	}
	t.Fatal("replica never converged: value not visible after 500ms")
}

func TestReplicaPool_WritesAlwaysVisible(t *testing.T) {
	// Writes must be immediately visible when read back through WithTx (which
	// uses the writer connection directly, bypassing the replica pool).
	cfg := testConfig(t)
	cfg.ReadPoolSize = 4
	cfg.ReplicaRefreshInterval = time.Hour // effectively frozen replicas

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "tx-key", "tx-val"); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// WithTx always uses the writer, so it must see the row immediately.
	err = memdb.WithTx(context.Background(), db, func(tx *sql.Tx) error {
		var val string
		return tx.QueryRow(`SELECT value FROM kv WHERE key = ?`, "tx-key").Scan(&val)
	})
	if err != nil {
		t.Fatalf("WithTx read-back: %v", err)
	}
}

func TestReplicaPool_ConcurrentReads(t *testing.T) {
	cfg := testConfig(t)
	cfg.ReadPoolSize = 4
	cfg.ReplicaRefreshInterval = 5 * time.Millisecond

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Seed some rows.
	for i := 0; i < 20; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("ck-%d", i), fmt.Sprintf("cv-%d", i),
		); err != nil {
			t.Fatalf("seed Exec: %v", err)
		}
	}

	// Wait for replicas to pick up the seed data.
	time.Sleep(50 * time.Millisecond)

	// Fire off concurrent readers; none should error.
	const goroutines = 16
	errc := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			var val string
			errc <- db.QueryRow(
				`SELECT value FROM kv WHERE key = ?`,
				fmt.Sprintf("ck-%d", g%20),
			).Scan(&val)
		}()
	}
	for i := 0; i < goroutines; i++ {
		if err := <-errc; err != nil {
			t.Errorf("concurrent read %d: %v", i, err)
		}
	}
}

func TestReplicaPool_Disabled(t *testing.T) {
	// ReadPoolSize == 0 must behave identically to the original single-connection path.
	cfg := testConfig(t)
	// cfg.ReadPoolSize is 0 by default

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "npool", "yes"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	var val string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "npool").Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != "yes" {
		t.Errorf("got %q, want %q", val, "yes")
	}
}

func testConfig(t *testing.T) memdb.Config {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "memdb-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	os.Remove(f.Name()) // start fresh

	return memdb.Config{
		FilePath:      f.Name(),
		FlushInterval: -1, // disable background flush in tests
		Logger:        silentLogger(),
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`
				CREATE TABLE IF NOT EXISTS kv (
					key   TEXT PRIMARY KEY,
					value TEXT NOT NULL
				)
			`)
			return err
		},
	}
}

func TestOpenClose(t *testing.T) {
	db, err := memdb.Open(testConfig(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestExecQuery(t *testing.T) {
	db, err := memdb.Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "foo", "bar"); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	var val string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "foo").Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != "bar" {
		t.Errorf("got %q, want %q", val, "bar")
	}
}

func TestFlushAndRestore(t *testing.T) {
	cfg := testConfig(t)

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "hello", "world"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	db.Close()

	// Re-open — should restore from snapshot
	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	var val string
	if err := db2.QueryRow(`SELECT value FROM kv WHERE key = ?`, "hello").Scan(&val); err != nil {
		t.Fatalf("QueryRow after restore: %v", err)
	}
	if val != "world" {
		t.Errorf("got %q, want %q", val, "world")
	}
}

func TestWithTx_Commit(t *testing.T) {
	db, err := memdb.Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = memdb.WithTx(context.Background(), db, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "tx", "ok")
		return err
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	var val string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "tx").Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != "ok" {
		t.Errorf("got %q, want %q", val, "ok")
	}
}

func TestWithTx_Rollback(t *testing.T) {
	db, err := memdb.Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_ = memdb.WithTx(context.Background(), db, func(tx *sql.Tx) error {
		_, _ = tx.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "rollback", "nope")
		return context.Canceled // force rollback
	})

	var val string
	err = db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "rollback").Scan(&val)
	if err != sql.ErrNoRows {
		t.Errorf("expected ErrNoRows after rollback, got: %v (val=%q)", err, val)
	}
}

func TestChangeHook(t *testing.T) {
	events := make([]memdb.ChangeEvent, 0)
	cfg := testConfig(t)
	cfg.OnChange = func(e memdb.ChangeEvent) {
		events = append(events, e)
	}

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, "k", "v"); err != nil {
		t.Fatalf("Exec INSERT: %v", err)
	}
	if _, err := db.Exec(`UPDATE kv SET value = ? WHERE key = ?`, "v2", "k"); err != nil {
		t.Fatalf("Exec UPDATE: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM kv WHERE key = ?`, "k"); err != nil {
		t.Fatalf("Exec DELETE: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	ops := []string{events[0].Op, events[1].Op, events[2].Op}
	want := []string{"INSERT", "UPDATE", "DELETE"}
	for i := range want {
		if ops[i] != want[i] {
			t.Errorf("event[%d].Op = %q, want %q", i, ops[i], want[i])
		}
	}
}

func TestClosedDB(t *testing.T) {
	db, err := memdb.Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := db.Exec(`SELECT 1`); err != memdb.ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}
