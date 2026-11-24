package memdb_test

// OnFlushComplete / OnFlushError are never invoked while the write lock is
// held, so they may call Exec or Flush; and a caller waiting for that lock
// gives up when its context ends.

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voicetel/memdb"
)

// callbackTimeout bounds every step that used to deadlock.
const callbackTimeout = 5 * time.Second

// within runs fn and fails the test if it has not returned in time. It
// reports whether fn returned, so a caller can skip Close on a deadlocked DB.
func within(t *testing.T, what string, fn func()) bool {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		return true
	case <-time.After(callbackTimeout):
		t.Errorf("%s did not return within %s (deadlock)", what, callbackTimeout)
		return false
	}
}

// flushRecorder collects OnFlushComplete calls.
type flushRecorder struct {
	mu      sync.Mutex
	metrics []memdb.FlushMetrics
}

func (r *flushRecorder) add(m memdb.FlushMetrics) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, m)
}

func (r *flushRecorder) take() []memdb.FlushMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.metrics
	r.metrics = nil
	return m
}

// TestOnFlushComplete_MayCallExec runs a write from inside OnFlushComplete.
// Under DurabilityWAL and DurabilitySync the callback used to run with the
// write lock held, so the nested Exec waited for a lock its own caller owned.
func TestOnFlushComplete_MayCallExec(t *testing.T) {
	cases := []struct {
		name string
		mode memdb.DurabilityMode
		// trigger starts the flush whose callback writes.
		trigger func(db *memdb.DB) error
		// callbacks is how many OnFlushComplete calls the trigger causes:
		// under DurabilitySync the nested Exec flushes too.
		callbacks int
	}{
		{"DurabilityNone/Flush", memdb.DurabilityNone, func(db *memdb.DB) error { return db.Flush(context.Background()) }, 1},
		{"DurabilityWAL/Flush", memdb.DurabilityWAL, func(db *memdb.DB) error { return db.Flush(context.Background()) }, 1},
		{"DurabilitySync/Flush", memdb.DurabilitySync, func(db *memdb.DB) error { return db.Flush(context.Background()) }, 2},
		{"DurabilitySync/Exec", memdb.DurabilitySync, func(db *memdb.DB) error {
			_, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('trigger', 'v')`)
			return err
		}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				rec     flushRecorder
				dbp     atomic.Pointer[memdb.DB]
				armed   atomic.Bool
				execErr atomic.Pointer[error]
			)
			cfg := walTestConfig(t)
			cfg.Durability = tc.mode
			cfg.OnFlushComplete = func(m memdb.FlushMetrics) {
				rec.add(m)
				// Write once: under DurabilitySync the nested Exec flushes
				// and re-enters this callback.
				if db := dbp.Load(); db != nil && armed.CompareAndSwap(true, false) {
					_, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('from-callback', 'v')`)
					execErr.Store(&err)
				}
			}
			db, err := memdb.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			dbp.Store(db)
			rec.take() // drop the flushes InitSchema caused under DurabilitySync
			armed.Store(true)

			var trigErr error
			if !within(t, "the flush whose OnFlushComplete calls Exec", func() { trigErr = tc.trigger(db) }) {
				return // deadlocked: Close would hang too
			}
			defer func() { _ = db.Close() }()
			if trigErr != nil {
				t.Fatalf("trigger: %v", trigErr)
			}
			if p := execErr.Load(); p == nil {
				t.Fatal("OnFlushComplete never ran its Exec")
			} else if *p != nil {
				t.Fatalf("Exec inside OnFlushComplete: %v", *p)
			}
			got := rec.take()
			if len(got) != tc.callbacks {
				t.Fatalf("OnFlushComplete fired %d times, want %d", len(got), tc.callbacks)
			}
			for i, m := range got {
				if m.Error != nil {
					t.Errorf("callback %d: Error = %v, want nil", i, m.Error)
				}
			}
			var v string
			if err := db.QueryRow(`SELECT value FROM kv WHERE key = 'from-callback'`).Scan(&v); err != nil || v != "v" {
				t.Errorf("row written from the callback = %q, %v", v, err)
			}
		})
	}
}

// TestOnFlushComplete_MayCallFlush calls Flush from inside OnFlushComplete —
// a deadlock in every durability mode while the callback ran under the lock.
func TestOnFlushComplete_MayCallFlush(t *testing.T) {
	for _, mode := range []memdb.DurabilityMode{memdb.DurabilityNone, memdb.DurabilityWAL, memdb.DurabilitySync} {
		t.Run(durabilityName(mode), func(t *testing.T) {
			var (
				rec      flushRecorder
				dbp      atomic.Pointer[memdb.DB]
				armed    atomic.Bool
				flushErr atomic.Pointer[error]
			)
			cfg := walTestConfig(t)
			cfg.Durability = mode
			cfg.OnFlushComplete = func(m memdb.FlushMetrics) {
				rec.add(m)
				if db := dbp.Load(); db != nil && armed.CompareAndSwap(true, false) {
					err := db.Flush(context.Background())
					flushErr.Store(&err)
				}
			}
			db, err := memdb.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			dbp.Store(db)
			rec.take()
			armed.Store(true)

			var outer error
			if !within(t, "the Flush whose OnFlushComplete calls Flush", func() { outer = db.Flush(context.Background()) }) {
				return
			}
			defer func() { _ = db.Close() }()
			if outer != nil {
				t.Fatalf("Flush: %v", outer)
			}
			if p := flushErr.Load(); p == nil || *p != nil {
				t.Fatalf("Flush inside OnFlushComplete: ran=%v err=%v", p != nil, p)
			}
			// The outer flush and the nested one: one callback each.
			if got := rec.take(); len(got) != 2 {
				t.Fatalf("OnFlushComplete fired %d times for 2 flushes", len(got))
			}
		})
	}
}

func durabilityName(m memdb.DurabilityMode) string {
	switch m {
	case memdb.DurabilityWAL:
		return "DurabilityWAL"
	case memdb.DurabilitySync:
		return "DurabilitySync"
	default:
		return "DurabilityNone"
	}
}

// toggleFailBackend is an in-memory ExternalBackend whose Write fails while
// fail is set.
type toggleFailBackend struct {
	fail atomic.Bool
	mu   sync.Mutex
	data []byte
}

var errBackendDown = errors.New("backend down")

func (b *toggleFailBackend) Exists(context.Context) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data != nil, nil
}

func (b *toggleFailBackend) Write(_ context.Context, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if b.fail.Load() {
		return errBackendDown
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = data
	return nil
}

func (b *toggleFailBackend) Read(context.Context) (io.ReadCloser, error) {
	return nil, errors.New("not used")
}

// TestOnFlushComplete_FiresOncePerFlushWithItsError checks the callback
// contract survived moving it out of the locked section: exactly one call per
// flush, carrying that flush's error — for an explicit Flush, for the flush a
// DurabilitySync write performs, and for the flush in Close.
func TestOnFlushComplete_FiresOncePerFlushWithItsError(t *testing.T) {
	for _, mode := range []memdb.DurabilityMode{memdb.DurabilityNone, memdb.DurabilityWAL, memdb.DurabilitySync} {
		t.Run(durabilityName(mode), func(t *testing.T) {
			var rec flushRecorder
			backend := &toggleFailBackend{}
			cfg := walTestConfig(t)
			cfg.Durability = mode
			cfg.Backend = memdb.WrapBackend(backend)
			cfg.OnFlushComplete = rec.add
			db, err := memdb.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			rec.take()

			expectOne := func(what string, wantErr error) {
				t.Helper()
				got := rec.take()
				if len(got) != 1 {
					t.Fatalf("%s: OnFlushComplete fired %d times, want 1", what, len(got))
				}
				if wantErr == nil && got[0].Error != nil {
					t.Errorf("%s: Error = %v, want nil", what, got[0].Error)
				}
				if wantErr != nil && (!errors.Is(got[0].Error, wantErr) || !errors.Is(got[0].Error, memdb.ErrFlushFailed)) {
					t.Errorf("%s: Error = %v, want ErrFlushFailed wrapping %v", what, got[0].Error, wantErr)
				}
			}

			if err := db.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			expectOne("successful Flush", nil)

			backend.fail.Store(true)
			if err := db.Flush(context.Background()); !errors.Is(err, errBackendDown) {
				t.Fatalf("Flush with the backend down = %v", err)
			}
			expectOne("failed Flush", errBackendDown)

			if mode == memdb.DurabilitySync {
				if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('s1', 'v')`); !errors.Is(err, errBackendDown) {
					t.Fatalf("DurabilitySync Exec with the backend down = %v", err)
				}
				expectOne("failed DurabilitySync write", errBackendDown)

				backend.fail.Store(false)
				if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('s2', 'v')`); err != nil {
					t.Fatal(err)
				}
				expectOne("successful DurabilitySync write", nil)
			}

			backend.fail.Store(false)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			expectOne("Close", nil)
		})
	}
}

// holdWriteLock opens a DurabilityWAL database on a blocking backend and
// parks a Flush inside the backend's Write, which holds the write lock until
// release is called. release also waits for that Flush and closes the DB.
func holdWriteLock(t *testing.T) (db *memdb.DB, release func()) {
	t.Helper()
	in := newBlockingBackend()
	cfg := walTestConfig(t)
	cfg.Backend = memdb.WrapBackend(in)
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan error, 1)
	go func() { held <- db.Flush(context.Background()) }()
	<-in.entered // the Flush now owns the write lock

	var once sync.Once
	release = func() {
		once.Do(func() {
			close(in.release)
			if err := <-held; err != nil {
				t.Errorf("the Flush holding the lock: %v", err)
			}
			_ = db.Close()
		})
	}
	t.Cleanup(release)
	return db, release
}

// TestFlush_ContextExpiresWhileWaitingForWriteLock: a Flush queued behind the
// write lock returns when its context ends instead of waiting indefinitely.
func TestFlush_ContextExpiresWhileWaitingForWriteLock(t *testing.T) {
	db, release := holdWriteLock(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	start := time.Now()
	go func() { errc <- db.Flush(ctx) }()

	select {
	case err := <-errc:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Flush = %v, want context.DeadlineExceeded", err)
		}
		if !errors.Is(err, memdb.ErrFlushFailed) {
			t.Errorf("Flush = %v, want it wrapped in ErrFlushFailed like other flush errors", err)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("Flush took %s to notice a 50ms deadline", d)
		}
	case <-time.After(2 * time.Second):
		t.Error("Flush ignored its context while waiting for the write lock")
		release()
		t.Logf("after release: %v", <-errc)
	}
}

// TestExecContext_ContextExpiresWhileWaitingForWriteLock: the same for a
// write. The write must not have been applied.
func TestExecContext_ContextExpiresWhileWaitingForWriteLock(t *testing.T) {
	db, release := holdWriteLock(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(ctx, `INSERT INTO kv (key, value) VALUES ('late', 'v')`)
		errc <- err
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("ExecContext = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("ExecContext ignored its context while waiting for the write lock")
		release()
		t.Logf("after release: %v", <-errc)
		return
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM kv WHERE key = 'late'`).Scan(&n); err != nil || n != 0 {
		t.Errorf("rows written by the timed-out ExecContext = %d, %v; want 0", n, err)
	}
}

// TestFlush_CancelledContextWhileWaitingForWriteLock covers cancellation
// (as opposed to a deadline) and that the failed attempt is reported.
func TestFlush_CancelledContextWhileWaitingForWriteLock(t *testing.T) {
	db, release := holdWriteLock(t)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- db.Flush(ctx) }()
	time.Sleep(20 * time.Millisecond) // let it queue behind the lock
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, memdb.ErrFlushFailed) {
			t.Errorf("Flush = %v, want ErrFlushFailed wrapping context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Flush ignored cancellation while waiting for the write lock")
		release()
		t.Logf("after release: %v", <-errc)
	}
}
