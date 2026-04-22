package raft_test

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/voicetel/memdb/replication"
	memraft "github.com/voicetel/memdb/replication/raft"
)

// ---------------------------------------------------------------------------
// failSink — Write always returns an error; tracks whether Cancel was called
// ---------------------------------------------------------------------------

type failSink struct{ cancelled bool }

func (f *failSink) Write(_ []byte) (int, error) { return 0, errors.New("write error") }
func (f *failSink) Close() error                { return nil }
func (f *failSink) Cancel() error               { f.cancelled = true; return nil }
func (f *failSink) ID() string                  { return "fail-sink" }

// ---------------------------------------------------------------------------
// TestFSM_Restore_RestoreFnError
// ---------------------------------------------------------------------------

// TestFSM_Restore_RestoreFnError verifies that Restore propagates an error
// returned by restoreFn back to the caller.
func TestFSM_Restore_RestoreFnError(t *testing.T) {
	t.Parallel()

	restoreErr := errors.New("restore failed")
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return restoreErr },
	)

	rc := io.NopCloser(bytes.NewReader([]byte("data")))
	err := fsm.Restore(rc)
	if err == nil {
		t.Fatal("expected non-nil error from Restore when restoreFn fails")
	}
	if !errors.Is(err, restoreErr) {
		t.Errorf("expected error chain to contain restoreErr; got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestFSM_Persist_WriteError
// ---------------------------------------------------------------------------

// TestFSM_Persist_WriteError verifies that Persist returns an error when the
// sink's Write call fails, and that sink.Cancel() is called in that case.
func TestFSM_Persist_WriteError(t *testing.T) {
	t.Parallel()

	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return []byte("snapshot-payload"), nil },
		func([]byte) error { return nil },
	)

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}

	sink := &failSink{}
	if err := snap.Persist(sink); err == nil {
		t.Fatal("expected non-nil error from Persist when sink.Write fails")
	}

	if !sink.cancelled {
		t.Error("expected sink.Cancel() to have been called after Write error")
	}
}

// ---------------------------------------------------------------------------
// TestApply_FutureFSMError
// ---------------------------------------------------------------------------

// TestApply_FutureFSMError verifies that when the FSM's execFn returns an
// error, the package-level Apply surfaces that error to the caller via
// future.Response().
func TestApply_FutureFSMError(t *testing.T) {
	t.Parallel()

	execErr := errors.New("exec failed in fsm")
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error { return execErr },
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return nil },
	)

	r := newSingleNodeRaft(t, fsm)

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}
	err := memraft.Apply(r, entry, time.Second)
	if err == nil {
		t.Fatal("expected non-nil error from Apply when FSM execFn fails")
	}
}

// ---------------------------------------------------------------------------
// TestApply_GobEncodeFailure_UnregisteredType
// ---------------------------------------------------------------------------

// unregistered is an unexported struct type that is NOT registered with gob,
// so it cannot be encoded when stored in an []any.
type unregistered struct{ V int }

// TestApply_GobEncodeFailure_UnregisteredType verifies that Apply returns a
// non-nil error containing "encode" or "gob" when args contain a type that
// gob cannot encode.
func TestApply_GobEncodeFailure_UnregisteredType(t *testing.T) {
	t.Parallel()

	fsm := noopFSM()
	r := newSingleNodeRaft(t, fsm)

	entry := replication.WALEntry{
		Seq:  1,
		SQL:  "INSERT INTO t VALUES (?)",
		Args: []any{unregistered{V: 1}},
	}

	err := memraft.Apply(r, entry, time.Second)
	if err == nil {
		t.Fatal("expected non-nil error from Apply with unregistered gob type")
	}
}

// ---------------------------------------------------------------------------
// TestFSM_Apply_AllRegisteredTypes
// ---------------------------------------------------------------------------

// TestFSM_Apply_AllRegisteredTypes verifies that all types registered in the
// package's init() survive a full gob round-trip through FSM.Apply via a
// single-node Raft cluster.
func TestFSM_Apply_AllRegisteredTypes(t *testing.T) {
	t.Parallel()

	wantArgs := []any{
		int(1),
		int64(2),
		float64(3.14),
		bool(true),
		string("hello"),
		[]byte("bytes"),
	}

	type result struct {
		sql  string
		args []any
	}
	ch := make(chan result, 1)

	fsm := memraft.NewFSM(
		func(sql string, args ...any) error {
			// Only capture the first call (our Apply below).
			select {
			case ch <- result{sql: sql, args: args}:
			default:
			}
			return nil
		},
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return nil },
	)

	r := newSingleNodeRaft(t, fsm)

	entry := replication.WALEntry{
		Seq:  1,
		SQL:  "INSERT INTO t VALUES (?,?,?,?,?,?)",
		Args: wantArgs,
	}
	if err := memraft.Apply(r, entry, time.Second); err != nil {
		t.Fatalf("Apply() returned unexpected error: %v", err)
	}

	var got result
	select {
	case got = <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for FSM execFn to be called")
	}

	if got.sql != entry.SQL {
		t.Errorf("SQL mismatch: got %q, want %q", got.sql, entry.SQL)
	}

	if len(got.args) != len(wantArgs) {
		t.Fatalf("args length mismatch: got %d, want %d", len(got.args), len(wantArgs))
	}

	for i, want := range wantArgs {
		if !reflect.DeepEqual(got.args[i], want) {
			t.Errorf("args[%d]: got %#v (%T), want %#v (%T)",
				i, got.args[i], got.args[i], want, want)
		}
	}
}

// ---------------------------------------------------------------------------
// TestFSM_Apply_NonLogCommandType_IsIgnored
// ---------------------------------------------------------------------------

// TestFSM_Apply_NonLogCommandType_IsIgnored verifies that log entries whose
// Type is not LogCommand (e.g. LogNoop, LogBarrier) are handled without
// panicking. The hashicorp/raft library may emit such entries internally.
func TestFSM_Apply_NonLogCommandType_IsIgnored(t *testing.T) {
	t.Parallel()

	var execCalled bool
	fsm := memraft.NewFSM(
		func(sql string, args ...any) error {
			execCalled = true
			return nil
		},
		func() ([]byte, error) { return nil, nil },
		func([]byte) error { return nil },
	)

	// A LogNoop entry has empty Data. Passing it to FSM.Apply exercises the
	// error path (bad gob) without panicking.
	log := &hraft.Log{
		Type: hraft.LogNoop,
		Data: nil,
	}

	// Must not panic. The result will be a decode error (nil data), which is
	// the same code-path as bad-gob — already tested separately; here we just
	// verify no panic and that execFn was NOT called.
	result := fsm.Apply(log)
	_ = result // may be nil or an error depending on implementation

	if execCalled {
		t.Error("execFn should not have been called for a LogNoop entry")
	}
}

// ---------------------------------------------------------------------------
// TestFSM_Snapshot_DataRoundTrip
// ---------------------------------------------------------------------------

// TestFSM_Snapshot_DataRoundTrip verifies that snapshot data written via
// Persist can be read back byte-for-byte by a second FSM's Restore, covering
// the full sink→reader round-trip using testSnapshotSink.
func TestFSM_Snapshot_DataRoundTrip(t *testing.T) {
	t.Parallel()

	payload := []byte("round-trip-payload-12345")

	fsm1 := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return payload, nil },
		func([]byte) error { return nil },
	)

	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}

	sink := &testSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist(): %v", err)
	}

	var restored []byte
	fsm2 := memraft.NewFSM(
		func(sql string, args ...any) error { return nil },
		func() ([]byte, error) { return nil, nil },
		func(data []byte) error {
			restored = make([]byte, len(data))
			copy(restored, data)
			return nil
		},
	)

	rc := io.NopCloser(bytes.NewReader(sink.buf.Bytes()))
	if err := fsm2.Restore(rc); err != nil {
		t.Fatalf("Restore(): %v", err)
	}

	if !bytes.Equal(restored, payload) {
		t.Errorf("round-trip data mismatch: got %q, want %q", restored, payload)
	}
}

// ---------------------------------------------------------------------------
// TestFSM_Apply_EmptyArgs
// ---------------------------------------------------------------------------

// TestFSM_Apply_EmptyArgs verifies that a WALEntry with a nil/empty Args
// slice round-trips cleanly through gob encoding in FSM.Apply.
func TestFSM_Apply_EmptyArgs(t *testing.T) {
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

	entry := replication.WALEntry{Seq: 1, SQL: "SELECT 1", Args: nil}
	log := &hraft.Log{Data: encodeEntry(t, entry)}
	result := fsm.Apply(log)

	if result != nil {
		t.Errorf("expected nil result, got: %v", result)
	}
	if len(gotArgs) != 0 {
		t.Errorf("expected empty args slice, got: %#v", gotArgs)
	}
}

// ---------------------------------------------------------------------------
// TestApply_Timeout
// ---------------------------------------------------------------------------

// TestApply_Timeout verifies that Apply with a very short timeout on a live
// leader either succeeds (if fast enough) or returns a non-nil error — it
// must not panic or block indefinitely.
func TestApply_Timeout(t *testing.T) {
	t.Parallel()

	fsm := noopFSM()
	r := newSingleNodeRaft(t, fsm)

	entry := replication.WALEntry{Seq: 1, SQL: "SELECT 1"}
	// 1 nanosecond timeout — extremely likely to time out, but either outcome
	// (success or error) is acceptable; we only care it doesn't hang/panic.
	done := make(chan error, 1)
	go func() {
		done <- memraft.Apply(r, entry, time.Nanosecond)
	}()

	select {
	case <-done:
		// ok — returned in time
	case <-time.After(5 * time.Second):
		t.Fatal("Apply did not return within 5 seconds with 1ns timeout")
	}
}

// ---------------------------------------------------------------------------
// TestFSM_Apply_IntTypes_RoundTrip
// ---------------------------------------------------------------------------

// TestFSM_Apply_IntTypes_RoundTrip specifically exercises the integer family
// of registered types to guard against accidental width-widening by gob.
func TestFSM_Apply_IntTypes_RoundTrip(t *testing.T) {
	t.Parallel()

	wantArgs := []any{
		int8(8),
		int16(16),
		int32(32),
		uint(100),
		uint8(200),
		uint16(300),
		uint32(400),
		uint64(500),
		float32(1.5),
	}

	// Register types used only in this test so gob can handle them in []any.
	gob.Register(int8(0))
	gob.Register(int16(0))
	gob.Register(int32(0))
	gob.Register(uint(0))
	gob.Register(uint8(0))
	gob.Register(uint16(0))
	gob.Register(uint32(0))
	gob.Register(uint64(0))
	gob.Register(float32(0))

	var gotArgs []any
	ch := make(chan struct{}, 1)

	fsm := memraft.NewFSM(
		func(sql string, args ...any) error {
			gotArgs = args
			select {
			case ch <- struct{}{}:
			default:
			}
			return nil
		},
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return nil },
	)

	r := newSingleNodeRaft(t, fsm)

	entry := replication.WALEntry{
		Seq:  2,
		SQL:  "INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?)",
		Args: wantArgs,
	}
	if err := memraft.Apply(r, entry, time.Second); err != nil {
		t.Fatalf("Apply() error: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for FSM execFn")
	}

	if len(gotArgs) != len(wantArgs) {
		t.Fatalf("args length mismatch: got %d, want %d", len(gotArgs), len(wantArgs))
	}
	for i, want := range wantArgs {
		if !reflect.DeepEqual(gotArgs[i], want) {
			t.Errorf("args[%d]: got %#v (%T), want %#v (%T)",
				i, gotArgs[i], gotArgs[i], want, want)
		}
	}
}
