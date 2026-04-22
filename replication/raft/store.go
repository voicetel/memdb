//go:build !purego

package raft

import (
	hraft "github.com/hashicorp/raft"
)

// newLogStore returns a LogStore for Raft log entries.
//
// TODO: Replace with a file-backed store (e.g. github.com/hashicorp/raft-boltdb)
// for production crash-safety. The current in-memory implementation loses all
// log entries on process restart. Snapshots (FileSnapshotStore) survive
// restarts and bound the amount of log that must be replayed, but the node
// will still need to re-join the cluster after a crash when using InmemStore.
func newLogStore(_ string) (hraft.LogStore, error) {
	return hraft.NewInmemStore(), nil
}

// newStableStore returns a StableStore for Raft metadata (current term, last vote).
//
// TODO: Replace with a file-backed store (e.g. github.com/hashicorp/raft-boltdb)
// for production crash-safety. Losing the stable store on restart means the
// node may violate Raft's election safety guarantees (voting twice in the same
// term) if it crashes between recording a vote and a leader being elected.
func newStableStore(_ string) (hraft.StableStore, error) {
	return hraft.NewInmemStore(), nil
}
