package memdb

import (
	"context"
	"database/sql"
	"strings"
	"sync"
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
// PrepareContext the same query, but only one *sql.Stmt is retained
// (LoadOrStore) and the loser is closed.
//
// # Lifetime
//
// The cache holds a *sql.Stmt for every distinct query string ever seen
// until Close is called. Application workloads typically issue a small
// fixed set of queries, so this bounds the cache to that set. Workloads
// that synthesise unbounded distinct query strings (e.g. inlining values
// into the SQL instead of using parameters) will grow the cache without
// bound — those workloads should not use the cache, but they also should
// not be written that way in the first place.
//
// # Schema invalidation
//
// SQLite's prepared statements are re-prepared automatically by
// database/sql when the underlying connection is invalidated. The writer
// pins MaxOpenConns=1, so the connection is stable for the life of the
// process; cached statements stay valid across DDL on the same
// connection because SQLite re-plans them transparently.
type stmtCache struct {
	db    *sql.DB
	cache sync.Map // map[string]*sql.Stmt
}

func newStmtCache(db *sql.DB) *stmtCache {
	return &stmtCache{db: db}
}

// get returns a cached *sql.Stmt for query, preparing it on first use.
// Concurrent callers preparing the same query race; LoadOrStore keeps the
// first winner and the losers close their duplicate.
func (c *stmtCache) get(ctx context.Context, query string) (*sql.Stmt, error) {
	if v, ok := c.cache.Load(query); ok {
		return v.(*sql.Stmt), nil
	}
	stmt, err := c.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	actual, loaded := c.cache.LoadOrStore(query, stmt)
	if loaded {
		_ = stmt.Close()
	}
	return actual.(*sql.Stmt), nil
}

// ExecContext executes query against the cached *sql.Stmt, preparing it
// on first use. Multi-statement SQL bypasses the cache because *sql.Stmt
// only executes the first statement of a multi-statement string —
// mattn/go-sqlite3's raw db.ExecContext iterates correctly so we route
// through it for those.
func (c *stmtCache) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if isMultiStatement(query) {
		return c.db.ExecContext(ctx, query, args...)
	}
	stmt, err := c.get(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

// QueryContext executes query against the cached *sql.Stmt, preparing it
// on first use. See ExecContext for the multi-statement passthrough rule.
func (c *stmtCache) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if isMultiStatement(query) {
		return c.db.QueryContext(ctx, query, args...)
	}
	stmt, err := c.get(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

// QueryRowContext executes query against the cached *sql.Stmt, preparing
// it on first use. A Prepare error cannot be returned from QueryRow
// directly (it returns *sql.Row, not (*sql.Row, error)) — on Prepare
// failure we fall back to the raw db.QueryRowContext so the caller still
// observes the error via Scan, matching database/sql semantics.
func (c *stmtCache) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if isMultiStatement(query) {
		return c.db.QueryRowContext(ctx, query, args...)
	}
	stmt, err := c.get(ctx, query)
	if err != nil {
		return c.db.QueryRowContext(ctx, query, args...)
	}
	return stmt.QueryRowContext(ctx, args...)
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

// Close releases every cached *sql.Stmt. Subsequent use is undefined.
// Returns the first non-nil close error observed; later errors are
// dropped because there is no useful place to report them.
func (c *stmtCache) Close() error {
	var firstErr error
	c.cache.Range(func(k, v any) bool {
		if err := v.(*sql.Stmt).Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.cache.Delete(k)
		return true
	})
	return firstErr
}
