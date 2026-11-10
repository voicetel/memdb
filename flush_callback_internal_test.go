package memdb

// White-box tests that need to break the WAL underneath a live DB.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestOnFlushError_WALAppendFailure_MayCallFlush fails a write's WAL append
// and has OnFlushError call back into the DB. The handler used to run inside
// the write's locked section, so the nested Flush deadlocked.
//
// The nested Flush then truncates a WAL whose file is closed, which also
// covers the truncate-failure path: the flush still succeeds (the snapshot's
// checkpoint makes leftover entries harmless) and a warning is logged.
func TestOnFlushError_WALAppendFailure_MayCallFlush(t *testing.T) {
	var (
		logs     bytes.Buffer
		dbp      atomic.Pointer[DB]
		calls    atomic.Int32
		seen     atomic.Pointer[error]
		flushErr atomic.Pointer[error]
	)
	cfg := Config{
		FilePath:      filepath.Join(t.TempDir(), "w.db"),
		FlushInterval: -1,
		Durability:    DurabilityWAL,
		Logger:        slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})),
		InitSchema: func(db *DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
		OnFlushError: func(err error) {
			calls.Add(1)
			seen.Store(&err)
			ferr := dbp.Load().Flush(context.Background())
			flushErr.Store(&ferr)
		},
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dbp.Store(db)

	// Break the WAL: every Append (and Truncate) now fails.
	if err := db.wal.f.Close(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('k', 'v')`)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrFlushFailed) {
			t.Errorf("Exec with a broken WAL = %v, want ErrFlushFailed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Exec deadlocked: OnFlushError called Flush while the write lock was held")
	}
	defer func() { _ = db.Close() }()

	if n := calls.Load(); n != 1 {
		t.Fatalf("OnFlushError fired %d times for one failed WAL append, want 1", n)
	}
	if p := seen.Load(); p == nil || !errors.Is(*p, ErrFlushFailed) || !strings.Contains((*p).Error(), "wal") {
		t.Errorf("OnFlushError got %v, want ErrFlushFailed mentioning the wal", p)
	}
	if p := flushErr.Load(); p == nil || *p != nil {
		t.Errorf("Flush inside OnFlushError: %v; a failed WAL truncate must not fail the flush", p)
	}
	if out := logs.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "WAL truncate after flush failed") {
		t.Errorf("no warning for the failed WAL truncate in:\n%s", out)
	}
}

// TestAcquireWrite_UncontendedDoesNotAllocate guards the write hot path: the
// context-aware lock must cost no allocation, with or without a deadline.
func TestAcquireWrite_UncontendedDoesNotAllocate(t *testing.T) {
	db := &DB{writeSem: make(chan struct{}, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for name, c := range map[string]context.Context{"Background": context.Background(), "WithTimeout": ctx} {
		if n := testing.AllocsPerRun(1000, func() {
			if err := db.acquireWrite(c); err != nil {
				t.Fatal(err)
			}
			db.releaseWrite()
		}); n != 0 {
			t.Errorf("%s: acquireWrite+releaseWrite allocated %.1f times per call", name, n)
		}
	}
}
