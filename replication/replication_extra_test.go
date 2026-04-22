package replication_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/voicetel/memdb/replication"
)

// ---------------------------------------------------------------------------
// failTransport — Broadcast always returns an error
// ---------------------------------------------------------------------------

type failTransport struct{ broadcastErr error }

func (f *failTransport) Broadcast(_ context.Context, _ replication.WALEntry) error {
	return f.broadcastErr
}

func (f *failTransport) Subscribe(_ context.Context) (<-chan replication.WALEntry, error) {
	ch := make(chan replication.WALEntry)
	return ch, nil
}

func (f *failTransport) RequestSync(_ context.Context, _ uint64) (uint64, error) {
	return 0, nil
}

// ---------------------------------------------------------------------------
// errorSubscribeTransport — Subscribe always returns an error
// ---------------------------------------------------------------------------

type errorSubscribeTransport struct{}

func (e *errorSubscribeTransport) Broadcast(_ context.Context, _ replication.WALEntry) error {
	return nil
}

func (e *errorSubscribeTransport) Subscribe(_ context.Context) (<-chan replication.WALEntry, error) {
	return nil, errors.New("subscribe failed")
}

func (e *errorSubscribeTransport) RequestSync(_ context.Context, _ uint64) (uint64, error) {
	return 0, nil
}

// ---------------------------------------------------------------------------
// Leader extra tests
// ---------------------------------------------------------------------------

// TestLeader_ShipSync_BroadcastError verifies that when Transport.Broadcast
// returns an error in SyncModeSync, Ship returns that error immediately.
func TestLeader_ShipSync_BroadcastError(t *testing.T) {
	t.Parallel()

	broadcastErr := errors.New("broadcast failed")
	ft := &failTransport{broadcastErr: broadcastErr}

	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: ft,
		SyncMode:  replication.SyncModeSync,
		Quorum:    1,
	})

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}
	err := leader.Ship(context.Background(), entry)
	if err == nil {
		t.Fatal("expected error from Ship when Broadcast fails, got nil")
	}
	if !errors.Is(err, broadcastErr) {
		t.Errorf("expected error to wrap broadcastErr; got: %v", err)
	}
}

// TestLeader_ShipSync_QuorumTwo_BothAck verifies that Ship returns nil when
// Quorum=2 and two distinct followers acknowledge the entry.
func TestLeader_ShipSync_QuorumTwo_BothAck(t *testing.T) {
	t.Parallel()

	tr := newMockTransport(8)
	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: tr,
		SyncMode:  replication.SyncModeSync,
		Quorum:    2,
	})

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}

	go func() {
		// Drain the broadcast channel first.
		<-tr.ch
		time.Sleep(10 * time.Millisecond)
		leader.Acknowledge("f1", entry.Seq)
		leader.Acknowledge("f2", entry.Seq)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := leader.Ship(ctx, entry); err != nil {
		t.Fatalf("Ship returned unexpected error: %v", err)
	}
}

// TestLeader_NewLeader_DefaultQuorum verifies that Quorum=0 (zero value) is
// treated as Quorum=1: a single ack is sufficient for sync ship to succeed.
func TestLeader_NewLeader_DefaultQuorum(t *testing.T) {
	t.Parallel()

	tr := newMockTransport(8)
	// Quorum left at zero value — should default to 1.
	leader := replication.NewLeader(replication.LeaderConfig{
		Transport: tr,
		SyncMode:  replication.SyncModeSync,
		Quorum:    0,
	})

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}

	go func() {
		<-tr.ch
		time.Sleep(10 * time.Millisecond)
		leader.Acknowledge("f1", entry.Seq)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := leader.Ship(ctx, entry); err != nil {
		t.Fatalf("Ship returned unexpected error with default quorum: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Follower extra tests
// ---------------------------------------------------------------------------

// TestFollower_Subscribe_Error verifies that Start returns a non-nil error
// when Transport.Subscribe returns an error.
func TestFollower_Subscribe_Error(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:    &errorSubscribeTransport{},
		OnApplyError: func(err error) { t.Errorf("unexpected apply error: %v", err) },
	})

	err := follower.Start(context.Background())
	if err == nil {
		t.Fatal("expected non-nil error from Start when Subscribe fails, got nil")
	}
}

// TestFollower_RequestSync_Error verifies that when requestSyncFn returns an
// error, OnApplyError is called with an error that wraps the resync error.
func TestFollower_RequestSync_Error(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	tr := newMockTransport(16)

	resyncErr := errors.New("resync failed")
	tr.setRequestSyncFn(func(fromSeq uint64) (uint64, error) {
		return 0, resyncErr
	})

	var applyErrReceived atomic.Value
	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:     tr,
		OnGapDetected: func(expected, got uint64) {},
		OnApplyError: func(err error) {
			applyErrReceived.Store(err)
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// seq=1 applies fine.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('x','1')`}
	waitFor(t, 200*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	// seq=5 triggers a gap → resync → which returns resyncErr.
	tr.ch <- replication.WALEntry{Seq: 5, SQL: `INSERT INTO kv (key, value) VALUES ('y','5')`}

	waitFor(t, 500*time.Millisecond, func() bool {
		return applyErrReceived.Load() != nil
	})

	got, ok := applyErrReceived.Load().(error)
	if !ok || got == nil {
		t.Fatal("OnApplyError was not called with an error")
	}
	if !errors.Is(got, resyncErr) {
		t.Errorf("expected error to wrap resyncErr; got: %v", got)
	}
}

// TestFollower_ChannelClosed_StopsGoroutine verifies that closing the
// transport channel causes the follower goroutine to exit cleanly (no panic)
// and that LastSeq() remains stable afterwards.
func TestFollower_ChannelClosed_StopsGoroutine(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	tr := newMockTransport(16)

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:    tr,
		OnApplyError: func(err error) { t.Logf("apply error: %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Apply one entry to confirm the goroutine is running.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('a','1')`}
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	// Close the channel — should cause the goroutine to return.
	close(tr.ch)

	// Give the goroutine time to notice the close and exit.
	time.Sleep(50 * time.Millisecond)

	// LastSeq should remain stable at 1.
	seqAfterClose := follower.LastSeq()
	time.Sleep(30 * time.Millisecond)
	if follower.LastSeq() != seqAfterClose {
		t.Errorf("LastSeq changed after channel close: was %d, now %d", seqAfterClose, follower.LastSeq())
	}
	if seqAfterClose != 1 {
		t.Errorf("expected LastSeq=1 after channel close, got %d", seqAfterClose)
	}
}

// TestFollower_OnGapDetected_Nil_NoResync_Panic verifies that a nil
// OnGapDetected callback does not cause a panic when a gap is detected.
// The resync still happens; only the callback is skipped.
func TestFollower_OnGapDetected_Nil_NoResync_Panic(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	tr := newMockTransport(16)

	// snapshotSeq=5; the gap entry (seq=5) will be skipped.
	tr.setRequestSyncFn(func(fromSeq uint64) (uint64, error) {
		return 5, nil
	})

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:     tr,
		OnGapDetected: nil, // intentionally nil
		OnApplyError:  func(err error) { t.Logf("apply error (expected): %v", err) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// seq=1 applies fine.
	tr.ch <- replication.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES ('a','1')`}
	waitFor(t, 200*time.Millisecond, func() bool { return follower.LastSeq() == 1 })

	// seq=5 triggers a gap; with nil OnGapDetected this must not panic.
	// resync returns snapshotSeq=5; entry.Seq=5 <= 5 → skipped; lastSeq=5.
	tr.ch <- replication.WALEntry{Seq: 5, SQL: `INSERT INTO kv (key, value) VALUES ('b','5')`}
	waitFor(t, 500*time.Millisecond, func() bool { return follower.LastSeq() == 5 })

	if follower.LastSeq() != 5 {
		t.Errorf("expected LastSeq=5 after gap+resync, got %d", follower.LastSeq())
	}
}

// TestFollower_OnApplyError_Nil_NoWrite_Panic verifies that a nil
// OnApplyError callback does not panic when SQL execution fails.
func TestFollower_OnApplyError_Nil_NoWrite_Panic(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	tr := newMockTransport(16)

	follower := replication.NewFollower(db, replication.FollowerConfig{
		Transport:    tr,
		OnApplyError: nil, // intentionally nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := follower.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send SQL that will fail (table does not exist).
	tr.ch <- replication.WALEntry{
		Seq: 1,
		SQL: `INSERT INTO nonexistent_table (key, value) VALUES ('a', 'b')`,
	}

	// Give the goroutine time to process — it must not panic.
	time.Sleep(100 * time.Millisecond)

	// The entry failed, so lastSeq should still be 0.
	if follower.LastSeq() != 0 {
		t.Errorf("expected LastSeq=0 after failed apply, got %d", follower.LastSeq())
	}
}
