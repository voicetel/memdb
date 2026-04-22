//go:build !purego

package memdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// replicaPool holds N independent in-memory SQLite databases that mirror the
// writer via sqlite3_serialize / sqlite3_deserialize. Reads are distributed
// across replicas in round-robin order so multiple goroutines can read
// concurrently without contending on the single writer connection.
//
// Staleness is bounded by Config.ReplicaRefreshInterval (default 1 ms).
type replicaPool struct {
	replicas   []*sql.DB
	driverName string
	idx        atomic.Uint64 // round-robin counter; wraps safely on overflow
	mu         sync.RWMutex  // RLock for next(); Lock for refresh()
}

// newReplicaPool creates n replica databases and populates each with a
// snapshot of the current writer state before returning.
func newReplicaPool(d *DB, n int, driverName string) (*replicaPool, error) {
	p := &replicaPool{
		replicas:   make([]*sql.DB, n),
		driverName: driverName,
	}
	for i := range p.replicas {
		r, err := sql.Open(driverName, ":memory:")
		if err != nil {
			p.close()
			return nil, fmt.Errorf("memdb: replica %d open: %w", i, err)
		}
		// Each replica is its own independent SQLite instance —
		// pin it to exactly one connection so there is no pool churn.
		r.SetMaxOpenConns(1)
		r.SetMaxIdleConns(1)
		p.replicas[i] = r
	}
	// Seed all replicas before the pool goes live.
	if err := p.refresh(d); err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}

// next returns the next replica in round-robin order. Safe for concurrent use.
func (p *replicaPool) next() *sql.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := uint64(len(p.replicas))
	i := p.idx.Add(1) % n
	return p.replicas[i]
}

// refresh serializes the writer's current state and deserializes it into every
// replica. The serialize step acquires a connection from the writer pool (which
// is pinned to one connection, so it waits for any in-flight write to finish)
// ensuring a consistent snapshot. The deserialize step runs under the write
// lock so that a concurrent next() never observes a partially-updated pool.
func (p *replicaPool) refresh(d *DB) error {
	// Step 1: serialize the writer — this is safe to do outside the replica
	// lock because the writer pool is single-connection (writes serialise
	// themselves) and we are just reading its page contents.
	var data []byte
	if err := withRawConn(context.Background(), d.mem, func(conn *sqlite3.SQLiteConn) error {
		var err error
		data, err = conn.Serialize("main")
		return err
	}); err != nil {
		return fmt.Errorf("memdb: replica serialize: %w", err)
	}

	// Step 2: push the snapshot into every replica under the write lock.
	// Readers calling next() block for the duration, but refresh is fast
	// (pure in-process memcpy) so the window is tiny.
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, r := range p.replicas {
		if err := withRawConn(context.Background(), r, func(conn *sqlite3.SQLiteConn) error {
			return conn.Deserialize(data, "main")
		}); err != nil {
			return fmt.Errorf("memdb: replica %d deserialize: %w", i, err)
		}
	}
	return nil
}

// close shuts down all replica connections.
func (p *replicaPool) close() {
	for _, r := range p.replicas {
		if r != nil {
			r.Close()
		}
	}
}
