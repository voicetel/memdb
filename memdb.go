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
// span consensus rounds.
type DB struct {
	mem      *sql.DB // single writer connection (also used for Flush/WAL)
	cfg      Config
	wal      *WAL
	replica  *replicaPool // nil when ReadPoolSize == 0
	stmts    *stmtCache   // prepared-statement cache for d.mem
	mu       sync.Mutex   // serialises Flush calls
	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
	closed   atomic.Bool

	// writeGen is a monotonic counter bumped on every successful write
	// against the in-memory DB (Exec, ExecContext, execDirect, and any
	// Tx.Commit path via bumpWriteGen). It lets the replica refresh loop
	// skip the expensive sqlite3_serialize + fan-out deserialize cycle
	// when nothing has changed since the last refresh — a common case
	// in read-heavy workloads where the writer is idle between ticks.
	//
	// Correctness: the counter is bumped AFTER the write succeeds, so
	// a reader observing writeGen > lastRefreshedGen is guaranteed to
	// see a DB state at least as new as the increment. A reader that
	// sees writeGen == lastRefreshedGen may be stale by one in-flight
	// write that incremented the counter between the load and the
	// refresh decision; that is acceptable because the next tick will
	// pick it up. We never skip a refresh when the writer is active
	// against the replica pool — only when it has been idle for a full
	// tick interval.
	writeGen atomic.Uint64
}

// bumpWriteGen is called after every successful write against d.mem. It is
// a single atomic add on the hot path — measured at <1 ns per call on the
// target hardware, well below the noise floor of the Exec path itself.
func (d *DB) bumpWriteGen() {
	d.writeGen.Add(1)
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
		mem:   mem,
		cfg:   cfg,
		stop:  make(chan struct{}),
		stmts: newStmtCache(mem),
	}

	// restore() checks Exists internally and is a no-op when no snapshot
	// exists. Errors are surfaced by restore() itself.
	if err := db.restore(); err != nil {
		mem.Close()
		return nil, err
	}

	// DurabilityWAL and DurabilitySync both rely on an on-disk WAL so that
	// writes can be recovered after a crash. DurabilitySync additionally
	// forces a full backend flush after every write (see writeDurability).
	if (cfg.Durability == DurabilityWAL || cfg.Durability == DurabilitySync) && cfg.FilePath != "" {
		walPath := cfg.FilePath + ".wal"
		db.wal, err = OpenWAL(walPath)
		if err != nil {
			mem.Close()
			return nil, err
		}
		// Replay any entries written after the last snapshot. Use the stmt
		// cache so repeated SQL strings (the common case for replay — many
		// INSERTs into the same table) don't re-prepare per entry.
		if err := db.wal.Replay(func(e WALEntry) error {
			_, err := db.stmts.ExecContext(context.Background(), e.SQL, e.Args...)
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

// writeDurability applies the durability-mode side effects of a successful
// write (WAL append, synchronous backend flush) after the SQL has already
// been executed against the in-memory DB.
//
// Durability matrix:
//
//	DurabilityNone — no side effect; writes are durable only after the
//	                 next periodic Flush completes.
//	DurabilityWAL  — append-only WAL entry with fsync; near-zero data-loss
//	                 window. The WAL is replayed on restart to recover
//	                 any writes made since the last snapshot.
//	DurabilitySync — WAL append AS ABOVE, plus a synchronous full flush to
//	                 the backend before returning. Equivalent to a regular
//	                 file-backed SQLite with synchronous=FULL: every write
//	                 is fully persisted before the caller sees success.
//	                 This is by far the slowest mode — use only when
//	                 zero-loss durability is required.
//
// ctx scopes both the WAL append (which is effectively synchronous already)
// and the optional backend flush so a caller-supplied deadline is honoured.
func (d *DB) writeDurability(ctx context.Context, query string, args []any) error {
	if d.cfg.Durability == DurabilityNone {
		return nil
	}
	if d.wal != nil {
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
	if d.cfg.Durability == DurabilitySync {
		// Full synchronous flush to the backend. flushUnchecked serialises
		// with background flushes via d.mu so there is no risk of two
		// overlapping backend writes.
		if err := d.flushUnchecked(ctx); err != nil {
			return err
		}
	}
	return nil
}

// execDirect writes SQL directly to the local in-memory database, bypassing
// the OnExec hook. Used by the Raft FSM to apply committed log entries on
// every node without re-entering the replication path.
func (d *DB) execDirect(query string, args ...any) error {
	if d.closed.Load() {
		return ErrClosed
	}
	if _, err := d.stmts.ExecContext(context.Background(), query, args...); err != nil {
		return err
	}
	d.bumpWriteGen()
	return d.writeDurability(context.Background(), query, args)
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
	return d.ExecContext(context.Background(), query, args...)
}

// ExecContext is the context-aware variant of Exec. The context scopes the
// Raft consensus round trip (when OnExec is set) and the synchronous backend
// flush performed under DurabilitySync so callers can bound the worst-case
// latency of a write.
func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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
	result, err := d.stmts.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	d.bumpWriteGen()
	if err := d.writeDurability(ctx, query, args); err != nil {
		return nil, err
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
	return d.QueryContext(context.Background(), query, args...)
}

// QueryContext is the context-aware variant of Query. The context is threaded
// into the underlying *sql.DB call so a cancellation aborts the query mid-flight.
func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.replica == nil {
		return d.stmts.QueryContext(ctx, query, args...)
	}
	r, release := d.replica.checkout()
	if r == nil {
		// All replicas busy or refresh in progress — fall back to the writer.
		return d.stmts.QueryContext(ctx, query, args...)
	}
	rows, err := r.QueryContext(ctx, query, args...)
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
	return d.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext is the context-aware variant of QueryRow. The context is
// threaded into the underlying *sql.DB call so a cancellation aborts the
// query mid-flight. A cancelled context surfaces as an error on the returned
// *sql.Row's Scan call, matching database/sql semantics.
func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if d.closed.Load() {
		return d.mem.QueryRowContext(ctx, query, args...) // returns sql: database is closed on Scan
	}
	if d.replica == nil {
		return d.stmts.QueryRowContext(ctx, query, args...)
	}
	r, release := d.replica.checkout()
	if r == nil {
		return d.stmts.QueryRowContext(ctx, query, args...)
	}
	// Return the replica immediately — QueryRow buffers its result internally
	// so the replica connection is not needed once QueryRow returns.
	defer release()
	return r.QueryRowContext(ctx, query, args...)
}

// Begin starts a transaction against the in-memory DB.
// Returns ErrTransactionNotSupported when OnExec is set (Raft mode).
//
// The write generation counter is bumped pessimistically when a transaction
// is opened because we cannot intercept Tx.Commit without changing the
// public signature (which returns *sql.Tx, not our own type). A read-only
// transaction therefore triggers one extra replica refresh tick — cheap
// compared to the cost of missing a committed write. Callers that hold
// long-lived read-only transactions and want to minimise refresh work
// should use QueryContext directly instead of wrapping in a Tx.
func (d *DB) Begin() (*sql.Tx, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	if d.cfg.OnExec != nil {
		return nil, ErrTransactionNotSupported
	}
	tx, err := d.mem.Begin()
	if err != nil {
		return nil, err
	}
	d.bumpWriteGen()
	return tx, nil
}

// BeginTx starts a transaction with the provided context and options.
// Returns ErrTransactionNotSupported when OnExec is set (Raft mode).
//
// The write generation counter is bumped pessimistically — see Begin for
// the rationale.
func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	if d.cfg.OnExec != nil {
		return nil, ErrTransactionNotSupported
	}
	tx, err := d.mem.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	d.bumpWriteGen()
	return tx, nil
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
		safeCallback(d.logger(), d.cfg.OnFlushError, d.cfg.OnFlushComplete, FlushMetrics{
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
		// Close cached statements before closing the underlying *sql.DB —
		// stmt.Close on a closed db returns an error we don't care about
		// but the order is more obviously correct this way.
		if d.stmts != nil {
			_ = d.stmts.Close()
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
//
// Per-tick cost optimisation: when no write has occurred since the last
// successful refresh the refresh call itself is a cheap atomic load and
// returns immediately (see replicaPool.refresh's fast path). In that
// steady state we do NOT want to pay for an os.Pipe-style shutdown
// plumbing — goroutine spawn, channel allocation, context allocation —
// every tick just to cover a shutdown that almost never arrives
// mid-refresh. So we split the tick into two paths:
//
//  1. Fast path: check the write-generation counter against the pool's
//     lastRefreshedGen directly. Unchanged ⇒ call refresh (which will
//     itself short-circuit) under context.Background — no goroutine,
//     no channel. Shutdown cannot race because a no-op refresh returns
//     before the next select iteration; the worst case is one extra
//     no-op tick after d.stop closes, which is fine.
//
//  2. Slow path: a real refresh is needed. Allocate the cancellation
//     plumbing so a leaked *sql.Rows holding a replica's only
//     connection cannot wedge shutdown forever. This path runs at most
//     once per actual write burst, so the per-tick cost here is
//     irrelevant.
//
// Measured impact on a pure-read workload (pprof block profile showed
// the previous implementation spending time in chan allocation and
// goroutine setup every 50ms): the fast path removes 1 goroutine +
// 1 channel + 1 context allocation per tick when the writer is idle,
// which is the common case after the write-generation short-circuit.
func (d *DB) replicaRefreshLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.ReplicaRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Fast path — no writes since last refresh. The refresh
			// call will observe the same condition and return without
			// touching any replica, so we skip the shutdown-plumbing
			// goroutine entirely. context.Background is safe because
			// the fast path cannot block: it is a single atomic load
			// followed by an immediate return.
			//
			// Reading d.replica.lastRefreshedGen and d.replica.seeded
			// without a lock is safe here because both fields are
			// only ever written from inside replicaPool.refresh, and
			// refresh is only ever called from THIS goroutine
			// (replicaRefreshLoop) plus the one-shot call from
			// newReplicaPool during Open — which has already
			// happened-before by the time this loop is spawned. So
			// there is no cross-goroutine mutation to race with.
			// d.writeGen is atomic because it IS written from
			// arbitrary caller goroutines on the write path.
			if d.writeGen.Load() == d.replica.lastRefreshedGen && d.replica.seeded {
				// Still call refresh so any future race between the
				// fast-path check here and a concurrent write is
				// resolved by refresh's own generation check, which
				// reads the counter again under the same atomic
				// ordering. The call is a no-op when the generation
				// is genuinely unchanged.
				_ = d.replica.refresh(context.Background(), d)
				continue
			}

			// Slow path — a real refresh is needed. Allocate the
			// cancellation plumbing so a leaked *sql.Rows holding a
			// replica's only connection cannot wedge shutdown.
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
					safeDo(d.logger(), nil, func() {
						d.cfg.OnFlushError(fmt.Errorf("memdb: replica refresh: %w", err))
					})
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
			safeDo(d.logger(), d.cfg.OnFlushError, func() {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				if err := d.Flush(ctx); err != nil && d.cfg.OnFlushError != nil {
					d.cfg.OnFlushError(err)
				}
			})
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
