package replication

import "context"

// WALEntry mirrors memdb.WALEntry for the replication layer.
type WALEntry struct {
	Seq       uint64
	Timestamp int64
	SQL       string
	Args      []any
}

// Transport abstracts the network layer for WAL entry delivery.
type Transport interface {
	// Broadcast sends a WALEntry to all connected followers.
	Broadcast(ctx context.Context, entry WALEntry) error

	// Subscribe returns a channel of incoming WALEntries (follower side).
	Subscribe(ctx context.Context) (<-chan WALEntry, error)

	// RequestSync asks the leader to stream a full snapshot starting from fromSeq.
	// Returns the WAL sequence number at the time the snapshot was taken.
	RequestSync(ctx context.Context, fromSeq uint64) (snapshotSeq uint64, err error)
}
