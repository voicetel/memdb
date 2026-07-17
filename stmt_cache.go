package memdb

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
)

// stmtCache caches *sql.Stmt instances keyed by SQL text so repeated calls
// against the same statement skip the per-call Prepare → Exec → Close cycle.
// Without the cache every db.Exec / db.Query crosses the cgo boundary three
// times instead of one — pprof of the contended-writer scenario showed
// runtime.cgocall at 40% of CPU, dominated by re-preparing the same INSERT
// over and over.
//
// The cache is concurrency-safe: many goroutines can call ExecContext /
// QueryContext / QueryRowContext on the same cache simultaneously. A miss
// is resolved without holding any lock — multiple goroutines may race to
// PrepareContext the same query, but only one entry is retained
// (LoadOrStore) and the loser is closed.
//
// # Lifetime
//
// Only queries executed WITH bind arguments are cached. Zero-argument
// SQL is passed through uncached: it is either one-shot (DDL) or has its
// values inlined into the string — PostgreSQL wire-protocol clients
// speaking the simple-query protocol (OpenSIPS msilo, psql) synthesise a
// distinct string per call, so caching those both misses forever and
// grows without bound. Round-3 profiling measured the alternative
// (cache + epoch flush for everything): the flush's hundreds of
// statement finalizes contend for the single connection and collapsed
// the inlined-SELECT wire workload to a third of its baseline
// throughput. The zero-args passthrough removes that entire class.
//
// For the parameterised statements that are cached, the cache converges
// to the application's fixed statement set and never flushes. The epoch
// flush below remains as a backstop for pathological callers that
// synthesise unbounded distinct parameterised strings: entries are
// counted and the whole cache is flushed when the count exceeds
// maxCachedStmts; hot statements re-prime with one prepare each.
//
// # Flush safety
//
// A flush must never close a statement between its handout from the map
// and the Exec/Query call that starts the query (database/sql refcounts
// uses only once the call is in flight). Each entry therefore carries its
// own handout refcount: get() acquires a ref before returning the
// statement and the caller releases it after the statement call returns.
// A flush marks the entry evicted and closes it only when the refcount
// is zero; the last release closes an evicted entry otherwise. A get()
// that acquires a ref and then observes the entry already evicted backs
// out and re-prepares. No mutex is held across a statement call, so the
// cache cannot deadlock against the single-connection pool the way a
// reader-writer lock spanning the call would.
//
// # Schema invalidation
//
// SQLite's prepared statements are re-prepared automatically by
// database/sql when the underlying connection is invalidated. The writer
// pins MaxOpenConns=1, so the connection is stable for the life of the
// process; cached statements stay valid across DDL on the same
// connection because SQLite re-plans them transparently. Replica caches
// additionally survive refresh's sqlite3_deserialize — see
// TestReplicaPreparedStmtSurvivesRefresh.
type stmtCache struct {
	db    *sql.DB
	cache sync.Map // map[string]*cachedStmt

	// count tracks the number of live entries in cache. It is advisory:
	// races between concurrent inserts can transiently over- or
	// under-shoot by the number of racing goroutines, which only shifts
	// the flush boundary by a handful of entries — never unbounded
	// growth.
	count atomic.Int64

	// flushMu serialises epoch flushes so concurrent threshold-crossers
	// do not each re-flush the cache the others just re-primed. It is
	// never held across a statement call.
	flushMu sync.Mutex
}

// maxCachedStmts bounds the number of distinct query strings a cache
// retains before an epoch flush. 512 comfortably covers any realistic
// fixed statement set while capping worst-case memory for
// literal-inlining wire clients.
const maxCachedStmts = 512

// cachedStmt is one cache entry: a prepared statement plus the handout
// refcount that lets an epoch flush close it without racing a goroutine
// that has taken it out of the map but not yet started its query.
type cachedStmt struct {
	stmt *sql.Stmt

	// refs counts handouts that have not yet released. Incremented by
	// get() before the entry is returned; decremented via release()
	// after the statement call returns.
	refs atomic.Int32

	// evicted is set (under flushMu) when a flush removes the entry from
	// the map. Once evicted, whoever observes refs at zero closes the
	// statement — the flusher if nothing is handed out, otherwise the
	// last release.
	evicted atomic.Bool

	// closed guards double-close between the flusher and a racing last
	// release.
	closed atomic.Bool
}

// closeIfIdle closes the underlying statement when the entry is evicted,
// no handouts remain, and nobody else closed it first.
func (e *cachedStmt) closeIfIdle() {
	if e.evicted.Load() && e.refs.Load() == 0 && e.closed.CompareAndSwap(false, true) {
		_ = e.stmt.Close()
	}
}

// release drops one handout ref and finishes an eviction when this was
// the last handout of an evicted entry.
func (e *cachedStmt) release() {
	if e.refs.Add(-1) == 0 {
		e.closeIfIdle()
	}
}

func newStmtCache(db *sql.DB) *stmtCache {
	return &stmtCache{db: db}
}

// get returns a cache entry for query with one handout ref acquired,
// preparing the statement on first use. The caller must call release()
// on the returned entry after the statement's Exec/Query call returns.
// Concurrent callers preparing the same query race; LoadOrStore keeps
// the first winner and the losers close their duplicate.
func (c *stmtCache) get(ctx context.Context, query string) (*cachedStmt, error) {
	for {
		if v, ok := c.cache.Load(query); ok {
			e := v.(*cachedStmt)
			e.refs.Add(1)
			// The ref pins the statement open only if it was acquired
			// before a flush evicted the entry. If the entry is already
			// evicted, back out and retry: the map no longer holds it,
			// so the next iteration prepares a fresh statement.
			if e.evicted.Load() {
				e.release()
				continue
			}
			return e, nil
		}
		stmt, err := c.db.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		e := &cachedStmt{stmt: stmt}
		e.refs.Add(1)
		actual, loaded := c.cache.LoadOrStore(query, e)
		if loaded {
			_ = stmt.Close()
			a := actual.(*cachedStmt)
			a.refs.Add(1)
			if a.evicted.Load() {
				a.release()
				continue
			}
			return a, nil
		}
		c.count.Add(1)
		return e, nil
	}
}

// maybeEpochFlush evicts every cached statement once the entry count
// exceeds maxCachedStmts — the symptom of a client synthesising distinct
// query strings rather than parameterising. Entries with no outstanding
// handout close immediately; handed-out entries close on their last
// release.
func (c *stmtCache) maybeEpochFlush() {
	if c.count.Load() <= maxCachedStmts {
		return
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	// Re-check under the lock — a concurrent flusher may have already
	// emptied the cache while this goroutine waited.
	if c.count.Load() <= maxCachedStmts {
		return
	}
	var dropped int64
	c.cache.Range(func(k, v any) bool {
		e := v.(*cachedStmt)
		c.cache.Delete(k)
		e.evicted.Store(true)
		e.closeIfIdle()
		dropped++
		return true
	})
	c.count.Add(-dropped)
}

// ExecContext executes query against the cached statement, preparing it
// on first use. Multi-statement SQL bypasses the cache because *sql.Stmt
// only executes the first statement of a multi-statement string —
// mattn/go-sqlite3's raw db.ExecContext iterates correctly so we route
// through it for those.
func (c *stmtCache) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if isMultiStatement(query) || len(args) == 0 {
		return c.db.ExecContext(ctx, query, args...)
	}
	e, err := c.get(ctx, query)
	if err != nil {
		return nil, err
	}
	res, err := e.stmt.ExecContext(ctx, args...)
	e.release()
	c.maybeEpochFlush()
	return res, err
}

// QueryContext executes query against the cached statement, preparing it
// on first use. See ExecContext for the multi-statement passthrough rule.
func (c *stmtCache) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if isMultiStatement(query) || len(args) == 0 {
		return c.db.QueryContext(ctx, query, args...)
	}
	e, err := c.get(ctx, query)
	if err != nil {
		return nil, err
	}
	// Releasing after QueryContext returns is safe even though the
	// caller still iterates the *sql.Rows: the query is in flight, and
	// database/sql defers the driver-level finalize of a closed
	// statement until its open rows complete.
	rows, err := e.stmt.QueryContext(ctx, args...)
	e.release()
	c.maybeEpochFlush()
	return rows, err
}

// QueryRowContext executes query against the cached statement, preparing
// it on first use. A Prepare error cannot be returned from QueryRow
// directly (it returns *sql.Row, not (*sql.Row, error)) — on Prepare
// failure we fall back to the raw db.QueryRowContext so the caller still
// observes the error via Scan, matching database/sql semantics.
func (c *stmtCache) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if isMultiStatement(query) || len(args) == 0 {
		return c.db.QueryRowContext(ctx, query, args...)
	}
	e, err := c.get(ctx, query)
	if err != nil {
		return c.db.QueryRowContext(ctx, query, args...)
	}
	row := e.stmt.QueryRowContext(ctx, args...)
	e.release()
	c.maybeEpochFlush()
	return row
}

// isMultiStatement reports whether query contains more than one SQL
// statement. Detection is intentionally simple: trim trailing whitespace
// and a trailing terminator semicolon, then look for any remaining ';'.
// False positives for ';' inside string literals or comments only cause
// the query to skip the prepared-statement cache (slower, still correct),
// so the heuristic does not need to be a full SQL parser.
func isMultiStatement(query string) bool {
	s := strings.TrimRight(query, " \t\r\n;")
	return strings.IndexByte(s, ';') >= 0
}

// Close releases every cached statement. Subsequent use is undefined.
// Returns the first non-nil close error observed; later errors are
// dropped because there is no useful place to report them.
func (c *stmtCache) Close() error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	var firstErr error
	var dropped int64
	c.cache.Range(func(k, v any) bool {
		e := v.(*cachedStmt)
		c.cache.Delete(k)
		e.evicted.Store(true)
		if e.refs.Load() == 0 && e.closed.CompareAndSwap(false, true) {
			if err := e.stmt.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		dropped++
		return true
	})
	c.count.Add(-dropped)
	return firstErr
}
