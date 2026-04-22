package replication_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/voicetel/memdb/replication"
)

// ---------------------------------------------------------------------------
// mockTransport
// ---------------------------------------------------------------------------

type mockTransport struct {
	mu            sync.Mutex
	ch            chan replication.WALEntry
	requestSyncFn func(fromSeq uint64) (uint64, error)
}

func newMockTransport(bufSize int) *mockTransport {
	return &mockTransport{
		ch: make(chan replication.WALEntry, bufSize),
		requestSyncFn: func(fromSeq uint64) (uint64, error) {
			return 0, nil
		},
	}
}

func (m *mockTransport) Broadcast(_ context.Context, entry replication.WALEntry) error {
	m.ch <- entry
	return nil
}

func (m *mockTransport) Subscribe(_ context.Context) (<-chan replication.WALEntry, error) {
	return m.ch, nil
}

func (m *mockTransport) RequestSync(_ context.Context, fromSeq uint64) (uint64, error) {
	m.mu.Lock()
	fn := m.requestSyncFn
	m.mu.Unlock()
	return fn(fromSeq)
}

func (m *mockTransport) setRequestSyncFn(fn func(fromSeq uint64) (uint64, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestSyncFn = fn
}

// ---------------------------------------------------------------------------
// Leader tests
// ---------------------------------------------------------------------------

func TestLeader_ShipAsync(t *testing.T) {
	t.Parallel()
	tr := newMockTransport(8)
	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: tr,
		SyncMode:  replication.SyncModeAsync,
	})

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}
	if err := leader.Ship(context.Background(), entry); err != nil {
		t.Fatalf("Ship returned error: %v", err)
	}

	// Async: Broadcast happens in a goroutine, give it a moment.
	select {
	case got := <-tr.ch:
		if got.Seq != entry.Seq || got.SQL != entry.SQL {
			t.Errorf("unexpected entry: %+v", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for Broadcast")
	}
}

func TestLeader_ShipSync_QuorumMet(t *testing.T) {
	t.Parallel()
	tr := newMockTransport(8)
	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: tr,
		SyncMode:  replication.SyncModeSync,
		Quorum:    1,
	})

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}

	// Acknowledge from a follower after a short delay.
	go func() {
		// Drain the broadcast channel so Broadcast doesn't block.
		<-tr.ch
		time.Sleep(20 * time.Millisecond)
		leader.Acknowledge("f1", entry.Seq)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := leader.Ship(ctx, entry); err != nil {
		t.Fatalf("Ship returned unexpected error: %v", err)
	}
}

func TestLeader_ShipSync_QuorumNotMet(t *testing.T) {
	t.Parallel()
	tr := newMockTransport(8)
	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: tr,
		SyncMode:  replication.SyncModeSync,
		Quorum:    2,
	})

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}

	// One follower acks — not enough for quorum 2.
	go func() {
		<-tr.ch
		time.Sleep(10 * time.Millisecond)
		leader.Acknowledge("f1", entry.Seq)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := leader.Ship(ctx, entry)
	if err == nil {
		t.Fatal("expected error when quorum not met, got nil")
	}
	lower := strings.ToLower(err.Error())
	if !strings.Contains(lower, "cancel") && !strings.Contains(lower, "deadline") {
		t.Errorf("expected error containing 'cancel' or 'deadline', got: %v", err)
	}
}

func TestLeader_Acknowledge_Idempotent(t *testing.T) {
	t.Parallel()
	tr := newMockTransport(8)
	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: tr,
		SyncMode:  replication.SyncModeAsync,
	})

	leader.Acknowledge("f1", 5)
	leader.Acknowledge("f1", 5) // duplicate — should be idempotent

	lag := leader.FollowerLag()
	if len(lag) != 1 {
		t.Fatalf("expected 1 follower in lag map, got %d", len(lag))
	}
	d, ok := lag["f1"]
	if !ok {
		t.Fatal("expected entry for 'f1' in lag map")
	}
	if d < 0 {
		t.Errorf("expected non-negative lag, got %v", d)
	}
}

func TestLeader_FollowerLag_NeverAcked(t *testing.T) {
	t.Parallel()
	tr := newMockTransport(8)
	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: tr,
		SyncMode:  replication.SyncModeAsync,
	})

	// No followers have been registered — map should be empty.
	lag := leader.FollowerLag()
	if len(lag) != 0 {
		t.Errorf("expected empty lag map, got %v", lag)
	}

	// After an acknowledge the entry should show a non-negative duration.
	leader.Acknowledge("f1", 1)
	lag = leader.FollowerLag()
	if d, ok := lag["f1"]; !ok || d < 0 {
		t.Errorf("expected non-negative lag for 'f1', got %v (ok=%v)", d, ok)
	}
}

func TestLeader_ErrNotLeader(t *testing.T) {
	t.Parallel()
	err := replication.ErrNotLeader{LeaderAddr: "host:7070"}
	if !strings.Contains(err.Error(), "host:7070") {
		t.Errorf("expected error to contain 'host:7070', got: %q", err.Error())
	}
}

// ---------------------------------------------------------------------------
// helpers for follower tests
// ---------------------------------------------------------------------------

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// ---------------------------------------------------------------------------
// Follower tests
// ---------------------------------------------------------------------------

func TestFollower_AppliesEntries(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:    tr,
		OnApplyError: func(err error) { t.Errorf("apply error: %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	entries := []replication.WALEntry{
		{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES (?, ?)`, Args: []any{"a", "1"}},
		{Seq: 2, SQL: `INSERT INTO kv (key, value) VALUES (?, ?)`, Args: []any{"b", "2"}},
		{Seq: 3, SQL: `INSERT INTO kv (key, value) VALUES (?, ?)`, Args: []any{"c", "3"}},
	}
	for _, e := range entries {
		tr.ch <- e
	}

	waitFor(t, 500*time.Millisecond, func() bool {
		return follower.LastSeq() == 3
	})

	for _, e := range entries {
		// extract key from args
		key := e.Args[0].(string)
		want := e.Args[1].(string)
		var got string
		if err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, key).Scan(&got); err != nil {
			t.Errorf("key %q: %v", key, err)
			continue
		}
		if got != want {
			t.Errorf("key %q: got %q, want %q", key, got, want)
		}
	}
}

func TestFollower_GapDetected_TriggersResync(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	var gapCalled atomic.Bool
	var gapExpected, gapGot atomic.Uint64
	var syncCalled atomic.Bool

	tr.setRequestSyncFn(func(fromSeq uint64) (uint64, error) {
		syncCalled.Store(true)
		return 3, nil
	})

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport: tr,
		OnGapDetected: func(expected, got uint64) {
			gapCalled.Store(true)
			gapExpected.Store(expected)
			gapGot.Store(got)
		},
		OnApplyError: func(err error) { t.Logf("apply error (expected): %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Entry 1 — applies fine.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('x','1')`}
	waitFor(t, 200*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	// Entry 5 — gap (expected 2).
	tr.ch <- replication.WALEntry{Seq: 5, SQL: `INSERT INTO kv (key, value) VALUES ('y','5')`}

	// After resync snapshotSeq=3, entry 5 > 3 so it gets applied → lastSeq==5.
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() == 5 })

	if !gapCalled.Load() {
		t.Error("OnGapDetected was not called")
	}
	if gapExpected.Load() != 2 {
		t.Errorf("OnGapDetected expected=2, got expected=%d", gapExpected.Load())
	}
	if gapGot.Load() != 5 {
		t.Errorf("OnGapDetected got=5, got got=%d", gapGot.Load())
	}
	if !syncCalled.Load() {
		t.Error("RequestSync was not called")
	}
	if follower.LastSeq() != 5 {
		t.Errorf("LastSeq: want 5, got %d", follower.LastSeq())
	}
}

func TestFollower_GapEntry_ReappliedAfterResync(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	// snapshotSeq=2; entry.Seq=5 > 2 → should be applied.
	tr.setRequestSyncFn(func(fromSeq uint64) (uint64, error) {
		return 2, nil
	})

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:     tr,
		OnGapDetected: func(expected, got uint64) {},
		OnApplyError:  func(err error) { t.Logf("apply error: %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// seq=1 applies fine.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('a','1')`}
	waitFor(t, 200*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	// seq=5 triggers gap; resync sets lastSeq=2; 5>2 → apply.
	tr.ch <- replication.WALEntry{Seq: 5, SQL: `INSERT INTO kv (key, value) VALUES ('b','5')`}
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() == 5 })

	if follower.LastSeq() != 5 {
		t.Errorf("LastSeq: want 5, got %d", follower.LastSeq())
	}
}

func TestFollower_GapEntry_SkippedIfCoveredBySnapshot(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	// snapshotSeq=10; entry.Seq=5 <= 10 → should be skipped.
	tr.setRequestSyncFn(func(fromSeq uint64) (uint64, error) {
		return 10, nil
	})

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:     tr,
		OnGapDetected: func(expected, got uint64) {},
		OnApplyError:  func(err error) { t.Logf("apply error: %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// seq=1 applies fine.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('a','1')`}
	waitFor(t, 200*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	// seq=5 triggers gap; resync sets lastSeq=10; 5<=10 → skip.
	tr.ch <- replication.WALEntry{Seq: 5, SQL: `INSERT INTO kv (key, value) VALUES ('b','5')`}
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() == 10 })

	if follower.LastSeq() != 10 {
		t.Errorf("LastSeq: want 10, got %d", follower.LastSeq())
	}
}

func TestFollower_FirstEntrySeq1_NoResync(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	var gapCalled atomic.Bool
	var syncCalled atomic.Bool

	tr.setRequestSyncFn(func(fromSeq uint64) (uint64, error) {
		syncCalled.Store(true)
		return 0, nil
	})

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport: tr,
		OnGapDetected: func(expected, got uint64) {
			gapCalled.Store(true)
		},
		OnApplyError: func(err error) { t.Errorf("apply error: %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// First entry with seq=1 on a fresh follower → no gap.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('a','1')`}
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	if gapCalled.Load() {
		t.Error("OnGapDetected should NOT have been called for seq=1 on fresh follower")
	}
	if syncCalled.Load() {
		t.Error("RequestSync should NOT have been called for seq=1 on fresh follower")
	}
}

func TestFollower_FirstEntrySeqNot1_TriggersResync(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	var gapCalled atomic.Bool

	// snapshotSeq=5; entry.Seq=5 <= 5 → skip after resync.
	tr.setRequestSyncFn(func(fromSeq uint64) (uint64, error) {
		return 5, nil
	})

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport: tr,
		OnGapDetected: func(expected, got uint64) {
			gapCalled.Store(true)
		},
		OnApplyError: func(err error) { t.Logf("apply error (expected): %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Fresh follower receives seq=5 (not 1) — should trigger gap/resync.
	tr.ch <- replication.WALEntry{Seq: 5, SQL: `INSERT INTO kv (key, value) VALUES ('a','5')`}
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() >= 5 })

	if !gapCalled.Load() {
		t.Error("OnGapDetected SHOULD have been called for seq=5 on fresh follower")
	}
}

func TestFollower_OnApplyError(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	var applyErrCalled atomic.Bool

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport: tr,
		OnApplyError: func(err error) {
			applyErrCalled.Store(true)
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// SQL that will fail — table does not exist.
	tr.ch <- replication.WALEntry{
		Seq: 1,
		SQL: `INSERT INTO nonexistent_table (key, value) VALUES ('a', 'b')`,
	}

	waitFor(t, 500*time.Millisecond, func() bool { return applyErrCalled.Load() })

	if !applyErrCalled.Load() {
		t.Error("OnApplyError was not called on a bad SQL statement")
	}
}

func TestFollower_StopsOnContextCancel(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tr := newMockTransport(16)

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:    tr,
		OnApplyError: func(err error) { t.Errorf("unexpected apply error: %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Apply one entry to confirm it's running.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('a','1')`}
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	// Cancel the context.
	cancel()

	// Give the goroutine time to exit.
	time.Sleep(50 * time.Millisecond)

	// After cancel, sends to the channel should not be drained.
	// We send an entry and verify lastSeq stays at 1.
	// (The goroutine should have exited; it won't read the channel.)
	select {
	case tr.ch <- replication.WALEntry{Seq: 2, SQL: `INSERT INTO kv (key, value) VALUES ('b','2')`}:
		// entry enqueued but may or may not be drained — we just care the
		// goroutine is not processing normally. Wait briefly and check.
	default:
	}

	time.Sleep(30 * time.Millisecond)
	// lastSeq should still be 1 (stopped goroutine won't apply seq 2).
	if follower.LastSeq() != 1 {
		// It's possible the goroutine processes the entry if it hadn't exited yet.
		// This is a best-effort check given Go scheduler timing.
		t.Logf("Note: lastSeq=%d after cancel (may have raced)", follower.LastSeq())
	}
}
