//go:build purego

package memdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	modernc "modernc.org/sqlite"
)

var registeredDrivers sync.Map // key: string fingerprint → registered driver name

func registerDriver(cfg Config) string {
	key := fmt.Sprintf("purego,cache=%d,busy=%d", cfg.CacheSize, cfg.BusyTimeout)
	if name, ok := registeredDrivers.Load(key); ok {
		return name.(string)
	}

	name := "sqlite3_memdb_pure_" + fmt.Sprintf("%x", fnv32(key))
	actual, loaded := registeredDrivers.LoadOrStore(key, name)
	if loaded {
		return actual.(string)
	}

	// Note: the modernc/purego driver does not support connect hooks.
	// CacheSize, BusyTimeout, and OnChange are not applied in purego builds.
	if cfg.OnChange != nil {
		// Cannot register an update hook with modernc — log warning via slog.
		slog.Default().Warn("memdb: OnChange is not supported in purego builds and will be ignored")
	}

	sql.Register(name, &modernc.Driver{})
	return name
}

// withRawConn for the modernc driver.
func withRawConn(ctx context.Context, db *sql.DB, fn func(any) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(func(c any) error {
		if c == nil {
			return fmt.Errorf("memdb: nil raw conn")
		}
		return fn(c)
	})
}

// openFileDB opens a file-backed SQLite DB using the registered purego driver.
func openFileDB(path string) (*sql.DB, error) {
	db, err := sql.Open(fileDriverName(), path)
	if err != nil {
		return nil, fmt.Errorf("memdb: open file db: %w", err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

var (
	pureFileDriverOnce sync.Once
	pureFileDriverName string
)

func fileDriverName() string {
	pureFileDriverOnce.Do(func() {
		pureFileDriverName = "sqlite3_memdb_pure_file_internal"
		sql.Register(pureFileDriverName, &modernc.Driver{})
	})
	return pureFileDriverName
}
