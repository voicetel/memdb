package raft

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

// realSQLiteErrors returns genuine SQLite errors: a primary key violation, a NOT
// NULL violation, and a busy database, each as the driver reports it.
func realSQLiteErrors(t *testing.T) map[string]error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "errors.db")
	holder, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Close() })
	holder.SetMaxOpenConns(1)
	if _, err := holder.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL); INSERT INTO kv VALUES ('a', '1')`); err != nil {
		t.Fatal(err)
	}
	out := map[string]error{}
	_, out["primary key"] = holder.Exec(`INSERT INTO kv VALUES ('a', '2')`)
	_, out["not null"] = holder.Exec(`INSERT INTO kv VALUES ('b', NULL)`)
	tx, err := holder.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.Exec(`INSERT INTO kv VALUES ('c', '3')`); err != nil {
		t.Fatal(err)
	}
	other, err := sql.Open("sqlite3", path+"?_busy_timeout=0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	_, out["busy"] = other.Exec(`INSERT INTO kv VALUES ('d', '4')`)
	for name, err := range out {
		var se sqlite3.Error
		if !errors.As(err, &se) {
			t.Fatalf("%s: %v is not a SQLite error", name, err)
		}
	}
	return out
}

// TestForwardedErrorsKeepTheirType forwards writes that fail on the leader with
// each kind of error and holds what the follower sees to what the leader saw:
// the same message, the same SQLite codes through errors.As, ErrNotLeader
// through errors.Is, and no SQLite error where there was none.
func TestForwardedErrorsKeepTheirType(t *testing.T) {
	leaderErrs := realSQLiteErrors(t)
	leaderErrs["not leader"] = fmt.Errorf("%w: lost leadership", ErrNotLeader)
	leaderErrs["plain"] = errors.New("raft: apply: timed out enqueuing operation")
	for name, err := range leaderErrs {
		if name != "not leader" && name != "plain" {
			leaderErrs[name] = fmt.Errorf("raft fsm: exec: %w", err)
		}
	}
	f, err := newForwarderWith("127.0.0.1:0", forwarderTLS(t), nil, func(sql string, _ ...any) (int64, error) {
		return 0, leaderErrs[sql]
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.close() })
	pool := NewConnPool(f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}, 1)
	t.Cleanup(pool.Close)

	for name, onLeader := range leaderErrs {
		t.Run(name, func(t *testing.T) {
			_, forwarded := sendForward(context.Background(), pool, ForwardRequest{SQL: name}, 5*time.Second)
			if forwarded == nil {
				t.Fatal("the forwarded write succeeded")
			}
			if errors.Is(onLeader, ErrNotLeader) {
				if !errors.Is(forwarded, ErrNotLeader) {
					t.Errorf("forwarded %v is not ErrNotLeader", forwarded)
				}
				return
			}
			if forwarded.Error() != onLeader.Error() {
				t.Errorf("forwarded message %q, leader's %q", forwarded.Error(), onLeader.Error())
			}
			var want, got sqlite3.Error
			wantSQLite, gotSQLite := errors.As(onLeader, &want), errors.As(forwarded, &got)
			if wantSQLite != gotSQLite || want.Code != got.Code || want.ExtendedCode != got.ExtendedCode || want.SystemErrno != got.SystemErrno {
				t.Errorf("forwarded SQLite error %v (%+v), leader's %v (%+v)", gotSQLite, got, wantSQLite, want)
			}
		})
	}
}

// TestForwardedErrorUnwrap holds the error a forwarded write returns: its
// message is the leader's, and it unwraps to a SQLite error only when it was
// one.
func TestForwardedErrorUnwrap(t *testing.T) {
	plain := forwardedError(ForwardResponse{ErrMsg: "boom"})
	if plain.Error() != "boom" || plain.Unwrap() != nil {
		t.Errorf("a plain error: %q, %v", plain.Error(), plain.Unwrap())
	}
	typed := forwardedError(ForwardResponse{ErrMsg: "raft fsm: exec: UNIQUE constraint failed: kv.k",
		SQLiteCode: int(sqlite3.ErrConstraint), SQLiteExtendedCode: int(sqlite3.ErrConstraintPrimaryKey)})
	var se sqlite3.Error
	if !errors.As(typed, &se) || se.Code != sqlite3.ErrConstraint || se.ExtendedCode != sqlite3.ErrConstraintPrimaryKey {
		t.Errorf("a SQLite error unwraps to %+v", se)
	}
}
