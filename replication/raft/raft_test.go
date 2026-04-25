package raft_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	"github.com/voicetel/memdb/replication"
	memraft "github.com/voicetel/memdb/replication/raft"
)

// ---------------------------------------------------------------------------
// testSnapshotSink
// ---------------------------------------------------------------------------

type testSnapshotSink struct {
	buf    bytes.Buffer
	closed bool
}

func (s *testSnapshotSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *testSnapshotSink) Close() error                { s.closed = true; return nil }
func (s *testSnapshotSink) Cancel() error               { return nil }
func (s *testSnapshotSink) ID() string                  { return "test-sink" }

// ---------------------------------------------------------------------------
// errReader — always returns an error on Read
// ---------------------------------------------------------------------------

type errReader struct{ err error }

func (e errReader) Read(_ []byte) (int, error) { return 0, e.err }

// ---------------------------------------------------------------------------
// newSingleNodeRaft — bootstrapped in-memory single-node Raft cluster
// ---------------------------------------------------------------------------

func newSingleNodeRaft(t *testing.T, fsm hraft.FSM) *hraft.Raft {
	t.Helper()
	cfg := hraft.DefaultConfig()
	cfg.LocalID = "node-1"
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.Logger = hclog.NewNullLogger()

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()

	transport, err := hraft.NewTCPTransport("127.0.0.1:0", nil, 3, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.Close() })

	r, err := hraft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Shutdown() })

	bootstrap := hraft.Configuration{
		Servers: []hraft.Server{
			{ID: cfg.LocalID, Address: transport.LocalAddr()},
		},
	}
	if f := r.BootstrapCluster(bootstrap); f.Error() != nil {
		t.Fatal(f.Error())
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == hraft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if r.State() != hraft.Leader {
		t.Fatal("node did not become leader in time")
	}
	return r
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func encodeEntry(t *testing.T, entry replication.WALEntry) []byte {
	t.Helper()
	data, err := replication.EncodeEntry(nil, entry)
	if err != nil {
		t.Fatalf("encode entry: %v", err)
	}
	return data
}

func noopFSM() *memraft.FSM {
	return memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return nil },
	)
}

// ---------------------------------------------------------------------------
// FSM.Apply tests
// ---------------------------------------------------------------------------

func TestFSM_Apply_NoArgs(t *testing.T) {
	t.Parallel()

	var gotSQL string
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error {
			gotSQL = sql
			return nil
		},
		func() ([]byte, error) { return nil, nil },
		func([]byte) error { return nil },
	)

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}
	log := &hraft.Log{Data: encodeEntry(t, entry)}
	result := fsm.Apply(log)

	if result != nil {
		t.Errorf("expected nil result, got: %v", result)
	}
	if gotSQL != entry.SQL {
		t.Errorf("execFn got SQL=%q, want %q", gotSQL, entry.SQL)
	}
}

func TestFSM_Apply_WithArgs(t *testing.T) {
	t.Parallel()

	var gotArgs []any
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error {
			gotArgs = args
			return nil
		},
		func() ([]byte, error) { return nil, nil },
		func([]byte) error { return nil },
	)

	wantArgs := []any{"hello", int64(42)}
	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (?, ?)", Args: wantArgs}
	log := &hraft.Log{Data: encodeEntry(t, entry)}
	result := fsm.Apply(log)

	if result != nil {
		t.Errorf("expected nil result, got: %v", result)
	}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("args mismatch: got %#v, want %#v", gotArgs, wantArgs)
	}
}

func TestFSM_Apply_BadGob(t *testing.T) {
	t.Parallel()

	fsm := noopFSM()

	log := &hraft.Log{Data: []byte("not a valid binary entry at all")}
	result := fsm.Apply(log)

	if result == nil {
		t.Fatal("expected non-nil error result for malformed log data")
	}
	if _, ok := result.(error); !ok {
		t.Errorf("expected result to be an error, got: %T %v", result, result)
	}
}

func TestFSM_Apply_ExecError(t *testing.T) {
	t.Parallel()

	execErr := errors.New("exec failed")
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return execErr },
		func() ([]byte, error) { return nil, nil },
		func([]byte) error { return nil },
	)

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}
	log := &hraft.Log{Data: encodeEntry(t, entry)}
	result := fsm.Apply(log)

	if result == nil {
		t.Fatal("expected non-nil error result when execFn fails")
	}
	if _, ok := result.(error); !ok {
		t.Errorf("expected result to be an error, got: %T %v", result, result)
	}
}

// ---------------------------------------------------------------------------
// FSM.Snapshot / Persist / Restore
// ---------------------------------------------------------------------------

func TestFSM_Snapshot_Persist_Restore(t *testing.T) {
	t.Parallel()

	snapshotData := []byte("snapshot-data")

	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return snapshotData, nil },
		func([]byte) error { return nil },
	)

	// Step 1: take snapshot.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}

	// Step 2: persist into sink.
	sink := &testSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist() error: %v", err)
	}

	if !sink.closed {
		t.Error("expected sink.Close() to have been called")
	}
	if !bytes.Equal(sink.buf.Bytes(), snapshotData) {
		t.Errorf("sink data mismatch: got %q, want %q", sink.buf.Bytes(), snapshotData)
	}

	// Step 3: restore into a second FSM.
	var restored []byte
	fsm2 := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return nil, nil },
		func(data []byte) error {
			restored = data
			return nil
		},
	)

	rc := io.NopCloser(bytes.NewReader(snapshotData))
	if err := fsm2.Restore(rc); err != nil {
		t.Fatalf("Restore() error: %v", err)
	}
	if !bytes.Equal(restored, snapshotData) {
		t.Errorf("restoreFn data mismatch: got %q, want %q", restored, snapshotData)
	}
}

func TestFSM_Snapshot_SerializeError(t *testing.T) {
	t.Parallel()

	serializeErr := errors.New("serialize failed")
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return nil, serializeErr },
		func([]byte) error { return nil },
	)

	_, err := fsm.Snapshot()
	if err == nil {
		t.Fatal("expected non-nil error from Snapshot() when serializeFn fails")
	}
	if !errors.Is(err, serializeErr) {
		t.Errorf("expected error chain to contain serializeErr; got: %v", err)
	}
}

func TestFSM_Restore_ReadError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("read failed")
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return nil, nil },
		func([]byte) error { return nil },
	)

	rc := io.NopCloser(errReader{err: readErr})
	err := fsm.Restore(rc)
	if err == nil {
		t.Fatal("expected non-nil error from Restore() when reader fails")
	}
	if !errors.Is(err, readErr) {
		t.Errorf("expected error chain to contain readErr; got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// package-level Apply tests
// ---------------------------------------------------------------------------

func TestApply_NotLeader(t *testing.T) {
	t.Parallel()

	cfg := hraft.DefaultConfig()
	cfg.LocalID = "node-notleader"
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.Logger = hclog.NewNullLogger()

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()

	transport, err := hraft.NewTCPTransport("127.0.0.1:0", nil, 3, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.Close() })

	r, err := hraft.NewRaft(cfg, noopFSM(), logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Shutdown() })

	// NOT bootstrapped — node stays Follower/Candidate; Apply should fail.
	entry := replication.WALEntry{Seq: 1, SQL: "SELECT 1"}
	applyErr := memraft.Apply(r, entry, 200*time.Millisecond)
	if applyErr == nil {
		t.Fatal("expected error from Apply when not leader, got nil")
	}
}

func TestApply_LeaderSuccess(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error {
			called.Store(true)
			return nil
		},
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return nil },
	)

	r := newSingleNodeRaft(t, fsm)

	entry := replication.WALEntry{Seq: 1, SQL: "test"}
	if err := memraft.Apply(r, entry, time.Second); err != nil {
		t.Fatalf("Apply() returned unexpected error: %v", err)
	}

	// execFn is called in FSM.Apply on the same goroutine as Raft commit;
	// give a small window for it to propagate.
	deadline := time.Now().Add(2 * time.Second)
	for !called.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !called.Load() {
		t.Error("execFn was not called after Apply")
	}
}

func TestApply_WithStringArgs(t *testing.T) {
	t.Parallel()

	var gotArgs []any
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error {
			gotArgs = args
			return nil
		},
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return nil },
	)

	r := newSingleNodeRaft(t, fsm)

	wantArgs := []any{"key", int64(1)}
	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (?, ?)", Args: wantArgs}
	if err := memraft.Apply(r, entry, time.Second); err != nil {
		t.Fatalf("Apply() returned unexpected error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for gotArgs == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("args mismatch: got %#v, want %#v", gotArgs, wantArgs)
	}
}

// ---------------------------------------------------------------------------
// fsmSnapshot.Release (no-op — must not panic)
// ---------------------------------------------------------------------------

func TestFSMSnapshot_Release(t *testing.T) {
	t.Parallel()

	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return []byte("data"), nil },
		func([]byte) error { return nil },
	)

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}

	// Must not panic.
	snap.Release()
}

// ---------------------------------------------------------------------------
// Context plumbing smoke-test (Apply honours context via Raft timeout)
// ---------------------------------------------------------------------------

func TestApply_ContextCancelled(t *testing.T) {
	t.Parallel()

	fsm := noopFSM()
	r := newSingleNodeRaft(t, fsm)

	// Already-cancelled context — Apply should still work because the node
	// is a live leader and the call itself is fast; but if the implementation
	// propagates context cancellation the error should be non-nil.
	// Either outcome is acceptable; we just verify it doesn't panic.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	_ = ctx  // context not used by the current Apply signature, kept for clarity

	entry := replication.WALEntry{Seq: 99, SQL: "SELECT 1"}
	// Apply uses a timeout duration, not a context — just verify no panic.
	_ = memraft.Apply(r, entry, 500*time.Millisecond)
}
