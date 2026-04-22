// Package replication defines shared types used by replication backends
// (e.g. Raft) that sit alongside the core memdb package.
package replication

// WALEntry records a single write operation that must be replicated across
// all nodes in the cluster. It mirrors memdb.WALEntry but lives in a separate
// package so that replication backends can import it without pulling in the
// full memdb package (which would create an import cycle).
type WALEntry struct {
	// Seq is a monotonically increasing sequence number assigned by the leader.
	Seq uint64

	// Timestamp is the Unix nanosecond timestamp at which the entry was created.
	Timestamp int64

	// SQL is the query to execute on every replica.
	SQL string

	// Args are the bound parameters for SQL. Only types registered with
	// encoding/gob are safe to use here; the raft package's init() registers
	// the common scalar types.
	Args []any
}
