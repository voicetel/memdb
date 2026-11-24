package memdb_test

// Tests for Config.OnExecContext — the replication hook that carries the
// caller's context, so the context given to ExecContext bounds the
// consensus round trip and reaches the forward to the leader.

import (
	"context"
	"errors"
	"testing"

	"github.com/voicetel/memdb"
)

type hookKey struct{}

// TestExecContext_WithOnExecContext_PassesTheContext verifies the hook gets the
// caller's own context and its count reaches sql.Result, and that Exec gives it
// a context too.
func TestExecContext_WithOnExecContext_PassesTheContext(t *testing.T) {
	cfg := testConfig(t)
	var got []any
	cfg.OnExecContext = func(ctx context.Context, sql string, args []any) (int64, error) {
		got = append(got, ctx.Value(hookKey{}))
		return 4, ctx.Err()
	}
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	got = nil

	ctx := context.WithValue(context.Background(), hookKey{}, "the caller's")
	res, err := db.ExecContext(ctx, `DELETE FROM kv`)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 4 {
		t.Errorf("RowsAffected = %d, want the hook's 4", n)
	}
	if _, err := db.Exec(`DELETE FROM kv`); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "the caller's" || got[1] != nil {
		t.Errorf("the hook saw %v", got)
	}

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.ExecContext(ended, `DELETE FROM kv`); !errors.Is(err, context.Canceled) {
		t.Errorf("an ended context: %v", err)
	}
	if len(got) != 2 {
		t.Error("the hook ran for an ended context")
	}
}

// TestOpen_ExecHooks_OneAtMost verifies no two replication hooks can be set,
// and that each alone, including the context one, makes the DB replicated.
func TestOpen_ExecHooks_OneAtMost(t *testing.T) {
	onExec := func(cfg *memdb.Config) { cfg.OnExec = func(string, []any) error { return nil } }
	onResult := func(cfg *memdb.Config) { cfg.OnExecResult = func(string, []any) (int64, error) { return 0, nil } }
	onContext := func(cfg *memdb.Config) {
		cfg.OnExecContext = func(context.Context, string, []any) (int64, error) { return 0, nil }
	}
	for name, set := range map[string][]func(*memdb.Config){
		"OnExec and OnExecContext":       {onExec, onContext},
		"OnExecResult and OnExecContext": {onResult, onContext},
		"all three":                      {onExec, onResult, onContext},
	} {
		cfg := testConfig(t)
		for _, s := range set {
			s(&cfg)
		}
		if _, err := memdb.Open(cfg); err == nil {
			t.Errorf("Open succeeded with %s set", name)
		}
	}
	cfg := testConfig(t)
	onContext(&cfg)
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Begin(); !errors.Is(err, memdb.ErrTransactionNotSupported) {
		t.Errorf("Begin with OnExecContext = %v, want ErrTransactionNotSupported", err)
	}
}
