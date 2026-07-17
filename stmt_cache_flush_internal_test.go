package memdb

// Tests for the stmtCache epoch flush — the bound that stops
// literal-inlining wire clients (simple-query SQL with values baked into
// the string, e.g. OpenSIPS msilo) from growing the cache without limit.

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func newFlushTestCache(t *testing.T) *stmtCache {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	return newStmtCache(db)
}

// TestStmtCache_EpochFlush_BoundsDistinctQueries feeds the cache more
// distinct query strings than maxCachedStmts and verifies the entry
// count stays bounded rather than growing monotonically.
func TestStmtCache_EpochFlush_BoundsDistinctQueries(t *testing.T) {
	c := newFlushTestCache(t)
	defer func() { _ = c.Close() }()

	// Distinct PARAMETERISED strings — zero-arg SQL bypasses the cache
	// entirely (see the zero-args passthrough), so the flush backstop is
	// only reachable through queries that carry bind arguments.
	const distinct = maxCachedStmts*2 + 100
	for i := 0; i < distinct; i++ {
		q := fmt.Sprintf(`SELECT value FROM kv WHERE key = ? /* variant %d */`, i)
		rows, err := c.QueryContext(context.Background(), q, "k")
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		_ = rows.Close()
	}

	if n := c.count.Load(); n > maxCachedStmts {
		t.Errorf("cache count after %d distinct queries = %d, want <= %d", distinct, n, maxCachedStmts)
	}

	// Zero-argument SQL must not populate the cache at all.
	before := c.count.Load()
	for i := 0; i < 50; i++ {
		rows, err := c.QueryContext(context.Background(),
			fmt.Sprintf(`SELECT value FROM kv WHERE key = 'inline-%d'`, i))
		if err != nil {
			t.Fatalf("inline query %d: %v", i, err)
		}
		_ = rows.Close()
	}
	if n := c.count.Load(); n != before {
		t.Errorf("zero-arg queries changed cache count: %d -> %d", before, n)
	}

	// The cache must still work after flushes.
	var v string
	err := c.QueryRowContext(context.Background(), `SELECT value FROM kv WHERE key = ?`, "nope").Scan(&v)
	if err != sql.ErrNoRows {
		t.Errorf("post-flush query error = %v, want sql.ErrNoRows", err)
	}
}

// TestStmtCache_EpochFlush_ConcurrentUseSafe hammers the cache from many
// goroutines with distinct strings (forcing flushes) interleaved with a
// hot parameterised query, asserting no error surfaces — this is the
// close-while-in-use safety property.
func TestStmtCache_EpochFlush_ConcurrentUseSafe(t *testing.T) {
	c := newFlushTestCache(t)
	defer func() { _ = c.Close() }()

	const goroutines = 8
	const perG = maxCachedStmts / 2
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// Distinct parameterised string — drives the count
				// toward the epoch-flush backstop.
				q := fmt.Sprintf(`SELECT value FROM kv WHERE key = ? /* g%d-%d */`, g, i)
				rows, err := c.QueryContext(context.Background(), q, "k")
				if err != nil {
					errs <- fmt.Errorf("distinct query: %w", err)
					return
				}
				_ = rows.Close()
				// Hot parameterised statement — may be mid-flight when
				// another goroutine's insert triggers epochFlush.
				var v string
				err = c.QueryRowContext(context.Background(), `SELECT value FROM kv WHERE key = ?`, "hot").Scan(&v)
				if err != nil && err != sql.ErrNoRows {
					errs <- fmt.Errorf("hot query: %w", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
