package memdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
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
//
// When Config.OnExec is set (Raft replication mode), Exec routes writes
// exclusively through OnExec — the local database is only written by the
// Raft FSM via ExecLocal after consensus is reached. Transactions (Begin,
// BeginTx, WithTx) are not supported in this mode because they cannot
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

// logger returns the slog.Logger to use, falling back to slog.Default() when
// no logger was provided in Config.
func (d *DB) logger() *slog.Logger {
	if d.cfg.Logger != nil {
		return d.cfg.Logger
	}
	return slog.Default()
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

	// restore() checks Exists internally and is a no-op when no snapshot
	// exists. Errors are surfaced by restore() itself.
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
		db.logger().Info("memdb: WAL replayed")
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

// execDirect writes SQL directly to the local in-memory database, bypassing
// the OnExec hook. Used by the Raft FSM to apply committed log entries on
// every node without re-entering the replication path.
func (d *DB) execDirect(query string, args ...any) error {
	if d.closed.Load() {
		return ErrClosed
	}
	if _, err := d.mem.Exec(query, args...); err != nil {
		return err
	}
	if d.cfg.Durability == DurabilityWAL && d.wal != nil {
		entry := WALEntry{
			Seq:       d.wal.NextSeq(),
			Timestamp: time.Now().UnixNano(),
			SQL:       query,
			Args:      args,
		}
		if walErr := d.wal.Append(entry); walErr != nil {
			if d.cfg.OnFlushError != nil {
				d.cfg.OnFlushError(fmt.Errorf("%w: wal: %w", ErrFlushFailed, walErr))
			}
			return fmt.Errorf("%w: wal append: %w", ErrFlushFailed, walErr)
		}
	}
	return nil
}

// ExecDirect writes SQL directly to the local in-memory database, bypassing
// the OnExec hook. This is used by the Raft FSM adapter to apply committed
// log entries without re-entering the replication path. Do not call this
// directly in application code.
func (d *DB) ExecDirect(query string, args ...any) error {
	return d.execDirect(query, args...)
}

// Exec executes a query against the in-memory DB.
//
// When OnExec is set the write must go through Raft consensus first.
// The FSM will call ExecDirect on every node (including this one) once
// the entry is committed. Writing locally here would cause split-brain
// on non-leader nodes.
func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}

	// When OnExec is set the write must go through Raft consensus first.
	// The FSM will call execDirect on every node (including this one) once
	// the entry is committed. Writing locally here would cause split-brain
	// on non-leader nodes.
	if d.cfg.OnExec != nil {
		if err := d.cfg.OnExec(query, args); err != nil {
			return nil, err
		}
		// Return a synthetic result — the actual rows-affected count is not
		// available after a Raft round-trip without significant extra work.
		return driver.RowsAffected(0), nil
	}

	// Standalone (no replication): write locally.
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
		if walErr := d.wal.Append(entry); walErr != nil {
			if d.cfg.OnFlushError != nil {
				d.cfg.OnFlushError(fmt.Errorf("%w: wal: %w", ErrFlushFailed, walErr))
			}
			return nil, fmt.Errorf("%w: wal append: %w", ErrFlushFailed, walErr)
		}
	}
	return result, nil
}

// Query executes a query that returns rows. When a read pool is configured
// a replica is checked out for exclusive use and returned immediately after
// the query is dispatched — the underlying *sql.DB connection is managed by
// the sql package's own pool (MaxOpenConns=1). Because each replica is pinned
// to exactly one connection, the checkout/release here serialises access at
// the replicaPool level while the sql package manages the connection lifecycle.
//
// refresh() calls inUse.Wait() before deserializing, which blocks until every
// checked-out replica has been released. This guarantees no open cursor can
// race with sqlite3_deserialize.
//
// release() is called immediately after r.Query returns even though the
// caller still holds an open *sql.Rows. The actual mutual-exclusion guarantee
// against refresh's Deserialize comes from MaxOpenConns=1 on the replica:
// refresh acquires the replica's sole connection via withRawConn, which
// blocks until the *sql.Rows cursor is closed. To prevent a leaked *sql.Rows
// from blocking refresh forever, refresh runs under a context that is
// cancelled when the parent DB is shutting down (see replicaRefreshLoop).
func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	if d.replica == nil {
		return d.mem.Query(query, args...)
	}
	r, release := d.replica.checkout()
	if r == nil {
		// All replicas busy or refresh in progress — fall back to the writer.
		return d.mem.Query(query, args...)
	}
	rows, err := r.Query(query, args...)
	release()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// QueryRow executes a query that returns a single row. When a read pool is
// configured a replica is checked out, the query is executed, and the replica
// is immediately returned. QueryRow buffers the result internally so the
// replica is not needed after the call returns.
func (d *DB) QueryRow(query string, args ...any) *sql.Row {
	if d.closed.Load() {
		return d.mem.QueryRow(query, args...) // returns sql: database is closed on Scan
	}
	if d.replica == nil {
		return d.mem.QueryRow(query, args...)
	}
	r, release := d.replica.checkout()
	if r == nil {
		return d.mem.QueryRow(query, args...)
	}
	// Return the replica immediately — QueryRow buffers its result internally
	// so the replica connection is not needed once QueryRow returns.
	defer release()
	return r.QueryRow(query, args...)
}

// Begin starts a transaction against the in-memory DB.
// Returns ErrTransactionNotSupported when OnExec is set (Raft mode).
func (d *DB) Begin() (*sql.Tx, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	if d.cfg.OnExec != nil {
		return nil, ErrTransactionNotSupported
	}
	return d.mem.Begin()
}

// BeginTx starts a transaction with the provided context and options.
// Returns ErrTransactionNotSupported when OnExec is set (Raft mode).
func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	if d.cfg.OnExec != nil {
		return nil, ErrTransactionNotSupported
	}
	return d.mem.BeginTx(ctx, opts)
}

// flushUnchecked performs a flush without checking d.closed. It is called
// from Close() after d.closed has already been set to true, so the normal
// Flush guard must be bypassed. All background goroutines have exited by the
// time this is called, so no concurrent writes can occur.
func (d *DB) flushUnchecked(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	start := time.Now()
	err := d.cfg.Backend.flush(ctx, d)
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrFlushFailed, err)
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

	if err != nil {
		d.logger().Error("memdb: flush failed",
			"error", err,
			"backend", fmt.Sprintf("%T", d.cfg.Backend),
		)
	} else {
		d.logger().Info("memdb: flushed to backend",
			"duration", time.Since(start).String(),
			"backend", fmt.Sprintf("%T", d.cfg.Backend),
		)
	}
	return err
}

// Flush copies the current in-memory state to the backend.
// Safe to call concurrently — serialised internally.
func (d *DB) Flush(ctx context.Context) error {
	if d.closed.Load() {
		return ErrClosed
	}
	return d.flushUnchecked(ctx)
}

// Close flushes to the backend, stops the background goroutine, and closes
// the in-memory DB. Subsequent calls are no-ops.
func (d *DB) Close() error {
	var closeErr error
	d.stopOnce.Do(func() {
		// Mark closed first so new Exec/Query/etc. callers fail immediately
		// with ErrClosed instead of racing teardown.
		//
		// In-flight Exec/Query calls that already passed the closed.Load()
		// check may still execute against d.mem before Close() proceeds to
		// tear it down. This is a narrow window — the caller sees
		// sql.ErrConnDone from the closed connection rather than ErrClosed.
		// Tracking in-flight ops with a WaitGroup would close this window
		// entirely at the cost of hot-path atomic contention.
		d.closed.Store(true)
		close(d.stop)
		d.wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		closeErr = d.flushUnchecked(ctx)

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
		return fmt.Errorf("%w: exists check: %w", ErrRestoreFailed, err)
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
			// Build a context that is cancelled if the DB is shutting down,
			// so refresh can be interrupted mid-Serialize/Deserialize if a
			// leaked *sql.Rows is holding a replica's only connection.
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				select {
				case <-d.stop:
					cancel()
				case <-done:
				}
			}()
			if err := d.replica.refresh(ctx, d); err != nil {
				d.logger().Error("memdb: replica refresh failed", "error", err)
				if d.cfg.OnFlushError != nil {
					d.cfg.OnFlushError(fmt.Errorf("memdb: replica refresh: %w", err))
				}
			}
			close(done)
			cancel()
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
			// Cap the per-flush timeout at half the interval so timeouts are
			// always shorter than the tick. The mutex in Flush prevents
			// overlap; this timeout is about giving up on a stuck backend.
			timeout := d.cfg.FlushInterval / 2
			if timeout <= 0 {
				// Shouldn't happen — flushLoop only starts when FlushInterval > 0.
				timeout = time.Second
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
// Returns ErrTransactionNotSupported when OnExec is set (Raft mode).
func WithTx(ctx context.Context, d *DB, fn func(*sql.Tx) error) error {
	if d.cfg.OnExec != nil {
		return ErrTransactionNotSupported
	}
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
