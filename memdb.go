package memdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DB is a SQL database backed by an in-memory SQLite instance that
// periodically persists to a Backend via the SQLite Online Backup API.
//
// Durability guarantee: writes are durable only after Flush() completes.
// Data written since the last flush may be lost on crash unless
// DurabilityWAL or DurabilitySync is configured.
//
// When Config.ReadPoolSize > 0, a pool of independent in-memory replica
// databases is maintained. Each replica is populated from the writer via
// sqlite3_serialize / sqlite3_deserialize and is kept up-to-date by a
// background goroutine. Query and QueryRow are load-balanced across replicas,
// allowing multiple goroutines to read concurrently without contending on the
// writer connection. Reads may observe a replica that is slightly behind the
// writer (bounded by Config.ReplicaRefreshInterval).
type DB struct {
	mem      *sql.DB // single writer connection (also used for Flush/WAL)
	cfg      Config
	wal      *WAL
	replica  *replicaPool // nil when ReadPoolSize == 0
	mu       sync.Mutex   // serialises Flush calls
	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
	closed   atomic.Bool
}

// readDB returns the *sql.DB to use for read operations. When a replica pool
// is running the next replica is returned via round-robin; otherwise the
// writer connection is returned directly (original single-connection behaviour).
func (d *DB) readDB() *sql.DB {
	if d.replica != nil {
		return d.replica.next()
	}
	return d.mem
}

// Open opens or creates a memdb database using the provided Config.
//
// If the backend already contains a snapshot it is restored into memory.
// Config.InitSchema is called after restore (or on first open if no snapshot
// exists). Background flushing starts immediately if FlushInterval > 0.
//
// When Config.ReadPoolSize > 0, a pool of read replicas is created. Each
// replica is an independent in-memory SQLite database populated from the
// writer via sqlite3_serialize / sqlite3_deserialize. A background goroutine
// refreshes replicas on Config.ReplicaRefreshInterval. Reads may be slightly
// stale (bounded by the refresh interval).
func Open(cfg Config) (*DB, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	name := registerDriver(cfg)

	mem, err := sql.Open(name, ":memory:")
	if err != nil {
		return nil, fmt.Errorf("memdb: open memory db: %w", err)
	}
	// The writer is always pinned to exactly one connection.
	mem.SetMaxOpenConns(1)
	mem.SetMaxIdleConns(1)

	db := &DB{
		mem:  mem,
		cfg:  cfg,
		stop: make(chan struct{}),
	}

	if err := db.restore(); err != nil {
		mem.Close()
		return nil, err
	}

	if cfg.Durability == DurabilityWAL && cfg.FilePath != "" {
		walPath := cfg.FilePath + ".wal"
		db.wal, err = OpenWAL(walPath)
		if err != nil {
			mem.Close()
			return nil, err
		}
		// Replay any entries written after the last snapshot.
		if err := db.wal.Replay(func(e WALEntry) error {
			_, err := db.mem.Exec(e.SQL, e.Args...)
			return err
		}); err != nil {
			mem.Close()
			db.wal.Close()
			return nil, fmt.Errorf("memdb: wal replay: %w", err)
		}
	}

	if cfg.InitSchema != nil {
		if err := cfg.InitSchema(db); err != nil {
			mem.Close()
			return nil, fmt.Errorf("memdb: InitSchema: %w", err)
		}
	}

	// Build the replica pool after the schema and any WAL replay are done,
	// so the first snapshot includes the fully-initialised state.
	if cfg.ReadPoolSize > 0 {
		pool, err := newReplicaPool(db, cfg.ReadPoolSize, name)
		if err != nil {
			mem.Close()
			return nil, fmt.Errorf("memdb: replica pool: %w", err)
		}
		db.replica = pool

		// Start the background refresh goroutine.
		db.wg.Add(1)
		go db.replicaRefreshLoop()
	}

	if cfg.FlushInterval > 0 {
		db.wg.Add(1)
		go db.flushLoop()
	}

	return db, nil
}

// DB returns the underlying *sql.DB for use with sqlx, bun, ent, sqlc, etc.
// Do not call Close() on the returned value directly — use memdb.DB.Close().
func (d *DB) DB() *sql.DB {
	return d.mem
}

// Exec executes a query against the in-memory DB.
func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	result, err := d.mem.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	if d.cfg.Durability == DurabilityWAL && d.wal != nil {
		entry := WALEntry{
			Seq:       d.wal.NextSeq(),
			Timestamp: time.Now().UnixNano(),
			SQL:       query,
			Args:      args,
		}
		if walErr := d.wal.Append(entry); walErr != nil && d.cfg.OnFlushError != nil {
			d.cfg.OnFlushError(fmt.Errorf("%w: wal: %v", ErrFlushFailed, walErr))
		}
	}
	return result, nil
}

// Query executes a query that returns rows. When a read pool is configured
// the query runs on a pooled reader connection, allowing concurrent reads.
func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	return d.readDB().Query(query, args...)
}

// QueryRow executes a query that returns a single row. When a read pool is
// configured the query runs on a pooled reader connection.
func (d *DB) QueryRow(query string, args ...any) *sql.Row {
	return d.readDB().QueryRow(query, args...)
}

// Begin starts a transaction against the in-memory DB.
func (d *DB) Begin() (*sql.Tx, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	return d.mem.Begin()
}

// BeginTx starts a transaction with the provided context and options.
func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	return d.mem.BeginTx(ctx, opts)
}

// Flush copies the current in-memory state to the backend.
// Safe to call concurrently — serialised internally.
func (d *DB) Flush(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed.Load() {
		return ErrClosed
	}

	start := time.Now()
	err := d.cfg.Backend.flush(ctx, d)
	if err != nil {
		err = fmt.Errorf("%w: %v", ErrFlushFailed, err)
	}

	if d.wal != nil && err == nil {
		_ = d.wal.Truncate()
	}

	if d.cfg.OnFlushComplete != nil {
		d.cfg.OnFlushComplete(FlushMetrics{
			Duration: time.Since(start),
			Error:    err,
		})
	}

	return err
}

// Close flushes to the backend, stops the background goroutine, and closes
// the in-memory DB. Subsequent calls are no-ops.
func (d *DB) Close() error {
	var closeErr error
	d.stopOnce.Do(func() {
		close(d.stop)
		d.wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		closeErr = d.Flush(ctx)

		d.closed.Store(true) // mark closed before tearing down resources
		if d.replica != nil {
			d.replica.close()
		}
		d.mem.Close()
		if d.wal != nil {
			d.wal.Close()
		}
	})
	return closeErr
}

// restore loads the backend snapshot into the memory DB.
// If no snapshot exists this is a no-op.
func (d *DB) restore() error {
	exists, err := d.cfg.Backend.Exists(context.Background())
	if err != nil {
		return fmt.Errorf("%w: exists check: %v", ErrRestoreFailed, err)
	}
	if !exists {
		return nil
	}
	return d.cfg.Backend.restore(context.Background(), d)
}

// replicaRefreshLoop periodically serializes the writer and deserializes into
// every replica. Runs as a background goroutine; stops when d.stop is closed.
func (d *DB) replicaRefreshLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.ReplicaRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := d.replica.refresh(d); err != nil && d.cfg.OnFlushError != nil {
				d.cfg.OnFlushError(fmt.Errorf("memdb: replica refresh: %w", err))
			}
		case <-d.stop:
			return
		}
	}
}

func (d *DB) flushLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			timeout := d.cfg.FlushInterval / 2
			if timeout < 5*time.Second {
				timeout = 5 * time.Second
			}
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				if err := d.Flush(ctx); err != nil && d.cfg.OnFlushError != nil {
					d.cfg.OnFlushError(err)
				}
			}()
		case <-d.stop:
			return
		}
	}
}

// WithTx runs fn inside a transaction. Commits on nil return, rolls back otherwise.
// Transactions always run on the single writer connection regardless of ReadPoolSize.
func WithTx(ctx context.Context, d *DB, fn func(*sql.Tx) error) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
