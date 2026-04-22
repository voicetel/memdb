//go:build purego

package memdb

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
)

// replicaPool is not supported under the purego build because modernc/sqlite
// does not expose sqlite3_serialize / sqlite3_deserialize. All read operations
// fall back to the single writer connection, identical to ReadPoolSize == 0.
type replicaPool struct {
	replicas   []*sql.DB
	driverName string
	idx        atomic.Uint64
	mu         sync.RWMutex
}

func newReplicaPool(_ *DB, _ int, _ string) (*replicaPool, error) {
	return nil, fmt.Errorf("memdb: ReadPoolSize > 0 is not supported in purego builds")
}

func (p *replicaPool) next() *sql.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.replicas) == 0 {
		return nil
	}
	i := p.idx.Add(1) % uint64(len(p.replicas))
	return p.replicas[i]
}

func (p *replicaPool) refresh(_ *DB) error {
	return fmt.Errorf("memdb: replicaPool.refresh not supported in purego builds")
}

func (p *replicaPool) close() {
	for _, r := range p.replicas {
		if r != nil {
			r.Close()
		}
	}
}
