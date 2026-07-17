package memdb_test

// Tests for Config.OnExecResult — the replication hook that carries the
// rows-affected count back from the Raft FSM apply so Exec's sql.Result
// reports the real count instead of the synthetic 0 the legacy OnExec
// hook is limited to.

import (
	"errors"
	"testing"

	"github.com/voicetel/memdb"
)

// TestExec_WithOnExecResult_ReportsCount verifies the count returned by
// the hook surfaces through sql.Result.RowsAffected, and that the local
// DB is not written directly (consensus applies via ExecDirect).
func TestExec_WithOnExecResult_ReportsCount(t *testing.T) {
	cfg := testConfig(t)
	var gotSQL string
	cfg.OnExecResult = func(sql string, args []any) (int64, error) {
		gotSQL = sql
		return 7, nil
	}
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	res, err := db.Exec(`UPDATE kv SET value = ? WHERE key = ?`, "v", "k")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if n != 7 {
		t.Errorf("RowsAffected = %d, want 7 (the hook's count)", n)
	}
	if gotSQL != `UPDATE kv SET value = ? WHERE key = ?` {
		t.Errorf("unexpected SQL: %q", gotSQL)
	}

	// The write must not have applied locally — that is the FSM's job.
	var val string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "k").Scan(&val); err == nil {
		t.Error("expected row to NOT be in local DB when OnExecResult is set")
	}
}

// TestExec_WithOnExecResult_PropagatesError verifies a hook error reaches
// the caller and no synthetic result is fabricated.
func TestExec_WithOnExecResult_PropagatesError(t *testing.T) {
	cfg := testConfig(t)
	hookErr := errors.New("no quorum")
	// InitSchema's Exec also routes through the hook during Open; only
	// fail once the test body runs.
	var armed bool
	cfg.OnExecResult = func(string, []any) (int64, error) {
		if armed {
			return 0, hookErr
		}
		return 0, nil
	}
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	armed = true

	if _, err := db.Exec(`DELETE FROM kv`); !errors.Is(err, hookErr) {
		t.Errorf("Exec error = %v, want %v", err, hookErr)
	}
}

// TestOpen_BothExecHooks_Rejected verifies the mutual-exclusion rule.
func TestOpen_BothExecHooks_Rejected(t *testing.T) {
	cfg := testConfig(t)
	cfg.OnExec = func(string, []any) error { return nil }
	cfg.OnExecResult = func(string, []any) (int64, error) { return 0, nil }
	if _, err := memdb.Open(cfg); err == nil {
		t.Fatal("Open succeeded with both OnExec and OnExecResult set; want error")
	}
}

// TestTransactions_WithOnExecResult_Rejected verifies the replication-mode
// transaction gate also covers the new hook.
func TestTransactions_WithOnExecResult_Rejected(t *testing.T) {
	cfg := testConfig(t)
	cfg.OnExecResult = func(string, []any) (int64, error) { return 0, nil }
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Begin(); !errors.Is(err, memdb.ErrTransactionNotSupported) {
		t.Errorf("Begin error = %v, want ErrTransactionNotSupported", err)
	}
}

// TestExecDirectResult_ReturnsCount verifies the FSM-facing apply path
// reports the real SQLite rows-affected count.
func TestExecDirectResult_ReturnsCount(t *testing.T) {
	cfg := testConfig(t)
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('a','1'), ('b','2'), ('c','3')`); err != nil {
		t.Fatal(err)
	}

	n, err := db.ExecDirectResult(`UPDATE kv SET value = 'x'`)
	if err != nil {
		t.Fatalf("ExecDirectResult: %v", err)
	}
	if n != 3 {
		t.Errorf("ExecDirectResult count = %d, want 3", n)
	}

	n, err = db.ExecDirectResult(`DELETE FROM kv WHERE key = 'a'`)
	if err != nil {
		t.Fatalf("ExecDirectResult: %v", err)
	}
	if n != 1 {
		t.Errorf("ExecDirectResult delete count = %d, want 1", n)
	}
}
