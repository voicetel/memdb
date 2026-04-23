package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/voicetel/memdb/replication"
)

func init() {
	// Register concrete types that may appear as SQL argument values in
	// WALEntry.Args so that gob can encode and decode []any correctly
	// across process boundaries and after restarts.
	gob.Register(int(0))
	gob.Register(int8(0))
	gob.Register(int16(0))
	gob.Register(int32(0))
	gob.Register(int64(0))
	gob.Register(uint(0))
	gob.Register(uint8(0))
	gob.Register(uint16(0))
	gob.Register(uint32(0))
	gob.Register(uint64(0))
	gob.Register(float32(0))
	gob.Register(float64(0))
	gob.Register(bool(false))
	gob.Register(string(""))
	gob.Register([]byte(nil))
	gob.Register(time.Time{})
}

// FSM implements raft.FSM using a *sql.DB-compatible exec function as the
// state machine. All nodes in the cluster apply the same WAL entries in
// the same order via Raft consensus.
type FSM struct {
	execFn       func(sql string, args ...any) error
	serializeFn  func() ([]byte, error)
	restoreFn    func([]byte) error
	onApplyError func(error) // optional; called when Apply fails
}

// NewFSM constructs a Raft FSM from caller-provided functions.
// execFn applies a SQL statement to the local state.
// serializeFn returns the full DB as a byte slice (sqlite3_serialize).
// restoreFn replaces the full DB from a byte slice (sqlite3_deserialize).
func NewFSM(
	execFn func(sql string, args ...any) error,
	serializeFn func() ([]byte, error),
	restoreFn func([]byte) error,
) *FSM {
	return &FSM{
		execFn:      execFn,
		serializeFn: serializeFn,
		restoreFn:   restoreFn,
	}
}

// SetApplyErrorHandler registers an optional callback that is invoked whenever
// the FSM's execFn returns an error during Apply. The callback receives the
// wrapped error that will also be returned as the Raft future response.
func (f *FSM) SetApplyErrorHandler(fn func(error)) {
	f.onApplyError = fn
}

// Apply is called by Raft on every committed log entry on every node.
func (f *FSM) Apply(log *raft.Log) any {
	var entry replication.WALEntry
	if err := gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&entry); err != nil {
		return fmt.Errorf("raft fsm: decode: %w", err)
	}
	if err := f.execFn(entry.SQL, entry.Args...); err != nil {
		result := fmt.Errorf("raft fsm: exec: %w", err)
		if f.onApplyError != nil {
			f.onApplyError(result)
		}
		return result
	}
	return nil
}

// Snapshot produces a point-in-time snapshot for Raft log compaction.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.serializeFn()
	if err != nil {
		return nil, fmt.Errorf("raft fsm: snapshot: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore installs a Raft snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("raft fsm: restore read: %w", err)
	}
	return f.restoreFn(data)
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// Apply encodes a WALEntry and submits it to the Raft cluster.
// Only the leader may call this. Followers receive the entry via FSM.Apply.
//
// No pre-check of r.State() is performed here — hashicorp/raft returns
// raft.ErrNotLeader from r.Apply if this node is not the leader. A pre-check
// would introduce a TOCTOU race (leadership can be lost between the check and
// the actual Apply call).
func Apply(r *raft.Raft, entry replication.WALEntry, timeout time.Duration) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return fmt.Errorf("raft: encode entry: %w", err)
	}

	future := r.Apply(buf.Bytes(), timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft: apply: %w", err)
	}
	if resp, ok := future.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}
