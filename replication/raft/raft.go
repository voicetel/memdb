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

// FSM implements raft.FSM using a *sql.DB-compatible exec function as the
// state machine. All nodes in the cluster apply the same WAL entries in
// the same order via Raft consensus.
type FSM struct {
	execFn      func(sql string, args ...any) error
	serializeFn func() ([]byte, error)
	restoreFn   func([]byte) error
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

// Apply is called by Raft on every committed log entry on every node.
func (f *FSM) Apply(log *raft.Log) any {
	var entry replication.WALEntry
	if err := gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&entry); err != nil {
		return fmt.Errorf("raft fsm: decode: %w", err)
	}
	if err := f.execFn(entry.SQL, entry.Args...); err != nil {
		return fmt.Errorf("raft fsm: exec: %w", err)
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
func Apply(r *raft.Raft, entry replication.WALEntry, timeout time.Duration) error {
	if r.State() != raft.Leader {
		_, leaderID := r.LeaderWithID()
		return fmt.Errorf("raft: not leader — current leader: %s", leaderID)
	}

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
