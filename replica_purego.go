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
//
// The struct fields and methods here exist only to satisfy the compiler —
// newReplicaPool always returns an error so this type is never actually
// instantiated at runtime in a purego build. checkout is therefore a
// trivial stub that signals "no replica available" so the read path falls
// back to the writer connection, matching the documented behaviour of
// ReadPoolSize == 0.
type replicaPool struct {
	idle       chan *sql.DB
	driverName string
}

func newReplicaPool(_ *DB, _ int, _ string) (*replicaPool, error) {
	return nil, fmt.Errorf("memdb: ReadPoolSize > 0 is not supported in purego builds")
}

// checkout is a stub for purego builds. It always returns (nil, nil) so that
// callers fall through to the writer connection. This method exists only so
// that the shared memdb.go compiles under the purego tag — at runtime the
// replicaPool is never constructed because newReplicaPool returns an error,
// so this code path is unreachable in practice.
func (p *replicaPool) checkout() (*sql.DB, func()) {
	return nil, nil
}

func (p *replicaPool) refresh(_ context.Context, _ *DB) error {
	return fmt.Errorf("memdb: replicaPool.refresh not supported in purego builds")
}

func (p *replicaPool) close() {}
