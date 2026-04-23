//go:build purego

package memdb

import (
	"context"
	"database/sql"
	"fmt"
)

// replicaPool is not supported under the purego build because modernc/sqlite
// does not expose sqlite3_serialize / sqlite3_deserialize. All read operations
// fall back to the single writer connection, identical to ReadPoolSize == 0.
type replicaPool struct {
	idle       chan *sql.DB
	driverName string
}

func newReplicaPool(_ *DB, _ int, _ string) (*replicaPool, error) {
	return nil, fmt.Errorf("memdb: ReadPoolSize > 0 is not supported in purego builds")
}

func (p *replicaPool) refresh(_ context.Context, _ *DB) error {
	return fmt.Errorf("memdb: replicaPool.refresh not supported in purego builds")
}

func (p *replicaPool) close() {}
