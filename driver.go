//go:build !purego

package memdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	sqlite3 "github.com/mattn/go-sqlite3"
)

var registeredDrivers sync.Map // key: string fingerprint → registered driver name

// registerDriver registers a named sqlite3 driver with pragmas applied via
// ConnectHook. Each unique combination of CacheSize, BusyTimeout, and OnChange
// presence gets its own registered driver name, avoiding sql.Register panics
// on duplicate names across multiple Open calls.
func registerDriver(cfg Config) string {
	hasHook := cfg.OnChange != nil
	key := fmt.Sprintf("cache=%d,busy=%d,hook=%v", cfg.CacheSize, cfg.BusyTimeout, hasHook)

	if name, ok := registeredDrivers.Load(key); ok {
		return name.(string)
	}

	name := "sqlite3_memdb_" + fmt.Sprintf("%x", fnv32(key))
	actual, loaded := registeredDrivers.LoadOrStore(key, name)
	if loaded {
		return actual.(string)
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

// fnv32 is a simple FNV-1a string hash used for driver name generation.
func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
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
