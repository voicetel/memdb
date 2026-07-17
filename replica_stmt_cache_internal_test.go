package memdb

// Guards the invariant the replica prepared-statement cache depends on:
// an idle prepared statement on a replica connection survives a refresh
// tick — deserializeReadonlyShared succeeds while the connection holds
// idle prepared statements, and SQLite lazily re-prepares them against
// the new snapshot on next use (prepare_v2 semantics). If this test ever
// fails after a SQLite/mattn upgrade, replicaPool.refresh must flush
// every replica's stmtCache between inUse.Wait() and deserialize.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplicaPreparedStmtSurvivesRefresh(t *testing.T) {
	dir, err := os.MkdirTemp("", "memdb-probe-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	cfg := Config{
		FilePath:      filepath.Join(dir, "probe.db"),
		FlushInterval: -1,
		ReadPoolSize:  1,
		// Long interval so the background loop never ticks during the
		// test; we drive refresh by hand.
		ReplicaRefreshInterval: time.Hour,
		InitSchema: func(db *DB) error {
			if _, err := db.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)`); err != nil {
				return err
			}
			_, err := db.Exec(`INSERT INTO kv VALUES ('k', 'v1')`)
			return err
		},
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Checkout the sole replica, prepare a statement on it, release.
	r, rel := db.replica.checkout()
	if r == nil {
		t.Fatal("checkout returned no replica")
	}
	stmt, err := r.PrepareContext(context.Background(), `SELECT value FROM kv WHERE key = ?`)
	if err != nil {
		rel.Release()
		t.Fatalf("prepare on replica: %v", err)
	}
	var v string
	if err := stmt.QueryRow("k").Scan(&v); err != nil {
		rel.Release()
		t.Fatalf("initial stmt query: %v", err)
	}
	if v != "v1" {
		rel.Release()
		t.Fatalf("initial value = %q, want v1", v)
	}
	rel.Release()

	// Write on the writer, then drive a refresh by hand. The prepared
	// statement is idle but NOT closed.
	if _, err := db.Exec(`UPDATE kv SET value = 'v2' WHERE key = 'k'`); err != nil {
		t.Fatal(err)
	}
	if err := db.replica.refresh(context.Background(), db); err != nil {
		t.Fatalf("refresh with idle prepared stmt: %v", err)
	}

	// Does the statement still work, and does it see the new snapshot?
	if err := stmt.QueryRow("k").Scan(&v); err != nil {
		t.Fatalf("stmt after refresh: %v", err)
	}
	if v != "v2" {
		t.Fatalf("stmt after refresh sees %q, want v2 (stale snapshot)", v)
	}

	// A second round for confidence.
	if _, err := db.Exec(`UPDATE kv SET value = 'v3' WHERE key = 'k'`); err != nil {
		t.Fatal(err)
	}
	if err := db.replica.refresh(context.Background(), db); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if err := stmt.QueryRow("k").Scan(&v); err != nil {
		t.Fatalf("stmt after second refresh: %v", err)
	}
	if v != "v3" {
		t.Fatalf("stmt after second refresh sees %q, want v3", v)
	}
	_ = stmt.Close()
}
