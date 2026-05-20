package raft_test

// Direct tests for FSM hooks and the snapshot Release path. The cluster
// integration tests cover Apply/Snapshot/Restore but not the
// SetApplyErrorHandler hook (no apply errors in the happy paths) or the
// fsmSnapshot Release no-op (no log compaction in the short tests).

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/voicetel/memdb/replication"
	mraft "github.com/voicetel/memdb/replication/raft"
)

func newTestFSM(execFn func(string, ...any) error) *mraft.FSM {
	return mraft.NewFSM(execFn,
		func() ([]byte, error) { return []byte("snap"), nil },
		func([]byte) error { return nil },
	)
}

func TestFSM_SetApplyErrorHandler_CalledOnExecError(t *testing.T) {
	wantErr := errors.New("exec failed")
	var seen atomic.Pointer[error]
	fsm := newTestFSM(func(string, ...any) error { return wantErr })
	fsm.SetApplyErrorHandler(func(err error) { seen.Store(&err) })

	entry := replication.WALEntry{Seq: 1, SQL: "INSERT INTO t VALUES (1)"}
	data, err := replication.EncodeEntry(nil, entry)
	if err != nil {
		t.Fatal(err)
	}
	resp := fsm.Apply(&hraft.Log{Data: data})
	gotErr, ok := resp.(error)
	if !ok || gotErr == nil {
		t.Fatalf("Apply returned %T, want error", resp)
	}
	if !errors.Is(gotErr, wantErr) {
		t.Errorf("error chain missing wantErr: %v", gotErr)
	}
	if seen.Load() == nil {
		t.Fatal("apply-error handler was never called")
	}
}

func TestFSM_SetApplyErrorHandler_NotCalledOnSuccess(t *testing.T) {
	var called atomic.Bool
	fsm := newTestFSM(func(string, ...any) error { return nil })
	fsm.SetApplyErrorHandler(func(error) { called.Store(true) })

	entry := replication.WALEntry{Seq: 1, SQL: "SELECT 1"}
	data, _ := replication.EncodeEntry(nil, entry)
	if resp := fsm.Apply(&hraft.Log{Data: data}); resp != nil {
		t.Errorf("Apply returned %v, want nil", resp)
	}
	if called.Load() {
		t.Error("apply-error handler was called on success")
	}
}

func TestFSM_SetApplyErrorHandler_PanicIsContained(t *testing.T) {
	// A panicking handler must not propagate out of Apply — the FSM
	// guards both the inner execFn panic and the user-supplied handler
	// with deferred recover().
	fsm := newTestFSM(func(string, ...any) error { return errors.New("boom") })
	fsm.SetApplyErrorHandler(func(error) { panic("handler panic") })

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("handler panic escaped Apply: %v", r)
		}
	}()
	entry := replication.WALEntry{Seq: 1, SQL: "x"}
	data, _ := replication.EncodeEntry(nil, entry)
	resp := fsm.Apply(&hraft.Log{Data: data})
	if _, ok := resp.(error); !ok {
		t.Errorf("Apply returned %T, want error", resp)
	}
}

func TestFSM_Apply_RecoverFromExecPanic(t *testing.T) {
	fsm := newTestFSM(func(string, ...any) error { panic("exec panic") })

	entry := replication.WALEntry{Seq: 1, SQL: "x"}
	data, _ := replication.EncodeEntry(nil, entry)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Apply did not recover panic: %v", r)
		}
	}()
	resp := fsm.Apply(&hraft.Log{Data: data})
	gotErr, ok := resp.(error)
	if !ok || gotErr == nil {
		t.Fatalf("Apply returned %T, want error", resp)
	}
}

// recordingSink implements raft.SnapshotSink with an in-memory buffer
// so we can assert what fsmSnapshot.Persist wrote without a real Raft
// store.
type recordingSink struct {
	bytes.Buffer
	cancelled bool
	closed    bool
}

func (s *recordingSink) Cancel() error { s.cancelled = true; return nil }
func (s *recordingSink) Close() error  { s.closed = true; return nil }
func (s *recordingSink) ID() string    { return "test-sink" }

func TestFSM_Snapshot_PersistAndRelease(t *testing.T) {
	fsm := mraft.NewFSM(
		func(string, ...any) error { return nil },
		func() ([]byte, error) { return []byte("payload"), nil },
		func([]byte) error { return nil },
	)
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &recordingSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if !sink.closed {
		t.Error("Persist did not Close the sink")
	}
	if got := sink.Bytes(); string(got) != "payload" {
		t.Errorf("sink bytes=%q, want payload", got)
	}
	// Release is a no-op in this implementation; we just need to
	// confirm it doesn't panic and is callable any number of times.
	snap.Release()
	snap.Release()
}

// failingSink rejects writes so Persist must return its error and
// cancel the sink.
type failingSink struct{ recordingSink }

func (s *failingSink) Write(p []byte) (int, error) {
	return 0, errors.New("disk full")
}

func TestFSM_Snapshot_PersistWriteError(t *testing.T) {
	fsm := mraft.NewFSM(
		func(string, ...any) error { return nil },
		func() ([]byte, error) { return []byte("data"), nil },
		func([]byte) error { return nil },
	)
	snap, _ := fsm.Snapshot()
	sink := &failingSink{}
	if err := snap.Persist(sink); err == nil {
		t.Fatal("expected error from failing sink, got nil")
	}
	if !sink.cancelled {
		t.Error("failing Persist did not Cancel the sink")
	}
}

func TestFSM_Restore_ReadsAllBytes(t *testing.T) {
	var got []byte
	fsm := mraft.NewFSM(
		func(string, ...any) error { return nil },
		func() ([]byte, error) { return nil, nil },
		func(b []byte) error {
			got = make([]byte, len(b))
			copy(got, b)
			return nil
		},
	)
	src := io.NopCloser(bytes.NewReader([]byte("restored-payload")))
	if err := fsm.Restore(src); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if string(got) != "restored-payload" {
		t.Errorf("restored=%q", got)
	}
}

func TestFSM_Restore_PropagatesUserError(t *testing.T) {
	wantErr := errors.New("restore rejected")
	fsm := mraft.NewFSM(
		func(string, ...any) error { return nil },
		func() ([]byte, error) { return nil, nil },
		func(b []byte) error { return wantErr },
	)
	src := io.NopCloser(bytes.NewReader([]byte("x")))
	if err := fsm.Restore(src); !errors.Is(err, wantErr) {
		t.Errorf("Restore err=%v, want chain to %v", err, wantErr)
	}
}
