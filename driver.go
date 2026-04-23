//go:build !purego

package memdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	sqlite3 "github.com/mattn/go-sqlite3"
)

var registeredDrivers sync.Map // key: string fingerprint → registered driver name

var hookCounter atomic.Uint64

var registeredDriverCount atomic.Uint64

// registerDriver registers a named sqlite3 driver with pragmas applied via
// ConnectHook. Each unique combination of CacheSize, BusyTimeout, and OnChange
// gets its own registered driver name, avoiding sql.Register panics on
// duplicate names across multiple Open calls.
//
// Configs without OnChange share a driver when pragmas match. Configs with
// OnChange always get a fresh driver registration (the atomic hookid makes the
// key unique) so two callers with different hooks never share a registration.
//
// Limitation: when cfg.OnChange is set, each call registers a new driver
// (the hookid counter ensures uniqueness so that concurrent Opens with
// different OnChange functions don't share a hook). These registrations
// are never reclaimed because database/sql provides no API to unregister a
// driver. Applications that open and close many DBs with OnChange will
// accumulate unbounded driver registrations in database/sql's global
// registry. For such workloads, either use the same OnChange function
// across all Opens (which would still allocate a new driver per call here
// because the function pointer is not used in the key — see hookID above —
// so the practical mitigation is to construct and reuse a single *DB), or
// avoid OnChange entirely.
func registerDriver(cfg Config) string {
	var hookID string
	if cfg.OnChange != nil {
		hookID = fmt.Sprintf(",hookid=%d", hookCounter.Add(1))
	}
	key := fmt.Sprintf("cache=%d,busy=%d%s", cfg.CacheSize, cfg.BusyTimeout, hookID)

	if name, ok := registeredDrivers.Load(key); ok {
		return name.(string)
	}

	name := "sqlite3_memdb_" + fmt.Sprintf("%x", fnv32(key))
	actual, loaded := registeredDrivers.LoadOrStore(key, name)
	if loaded {
		return actual.(string)
	}

	count := registeredDriverCount.Add(1)
	if count > 100 && count%100 == 1 {
		slog.Default().Warn("memdb: high sqlite3 driver registration count — "+
			"OnChange-bearing configs are not deduplicated across Open calls and the "+
			"database/sql global driver registry grows unboundedly",
			"count", count,
		)
	}

	onChangeFn := cfg.OnChange
	sql.Register(name, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			pragmas := []string{
				"PRAGMA journal_mode=WAL",
				fmt.Sprintf("PRAGMA cache_size=%d", cfg.CacheSize),
				"PRAGMA temp_store=MEMORY",
				"PRAGMA synchronous=NORMAL",
				fmt.Sprintf("PRAGMA busy_timeout=%d", cfg.BusyTimeout),
			}
			for _, p := range pragmas {
				if _, err := conn.Exec(p, nil); err != nil {
					return fmt.Errorf("memdb: pragma %q: %w", p, err)
				}
			}
			if onChangeFn != nil {
				conn.RegisterUpdateHook(func(op int, _ string, table string, rowid int64) {
					onChangeFn(ChangeEvent{
						Op:    opName(op),
						Table: table,
						RowID: rowid,
					})
				})
			}
			return nil
		},
	})
	return name
}

// opName maps a SQLite update hook op code to its string representation.
func opName(op int) string {
	switch op {
	case sqlite3.SQLITE_INSERT:
		return "INSERT"
	case sqlite3.SQLITE_UPDATE:
		return "UPDATE"
	case sqlite3.SQLITE_DELETE:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// withRawConn acquires a connection from the pool and exposes the underlying
// *sqlite3.SQLiteConn via fn. This is required to call the backup API.
func withRawConn(ctx context.Context, db *sql.DB, fn func(*sqlite3.SQLiteConn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(func(c any) error {
		sc, ok := c.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("memdb: unexpected driver type %T", c)
		}
		return fn(sc)
	})
}

// openFileDB opens a file-backed SQLite DB using the registered cgo driver.
func openFileDB(path string) (*sql.DB, error) {
	db, err := sql.Open(driverNameForFile(), path)
	if err != nil {
		return nil, fmt.Errorf("memdb: open file db: %w", err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// driverNameForFile returns a driver name suitable for opening plain file DBs.
// We register a minimal driver (no pragmas, no hooks) for internal use.
var fileDriverOnce sync.Once
var fileDriverName string

func driverNameForFile() string {
	fileDriverOnce.Do(func() {
		fileDriverName = "sqlite3_memdb_file_internal"
		sql.Register(fileDriverName, &sqlite3.SQLiteDriver{})
	})
	return fileDriverName
}
