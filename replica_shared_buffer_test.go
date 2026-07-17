package memdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestReplica_ReadOnlyEnforced confirms that SQLITE_DESERIALIZE_READONLY
// (which auto-enables PRAGMA query_only=ON) rejects writes attempted
// directly against a replica connection. Application code never
// reaches a replica this way — Exec always goes to the writer — but
// the pin is here so a future refactor can't accidentally lose the
// guarantee.
func TestReplica_ReadOnlyEnforced(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{
		FilePath:               filepath.Join(t.TempDir(), "ro.db"),
		FlushInterval:          -1,
		ReadPoolSize:           2,
		ReplicaRefreshInterval: 20 * time.Millisecond,
	})
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Wait until a replica reflects the row, then check out the replica
	// directly and confirm a write fails with SQLITE_READONLY.
	waitForReplicaCount_t(t, db, "SELECT COUNT(*) FROM t", 1, 2*time.Second)

	// checkout legitimately returns nil while a refresh tick is in
	// progress (the 20 ms interval above makes that window easy to hit
	// under -race scheduling jitter) — retry briefly rather than treating
	// one unlucky attempt as a failure.
	var r *sql.DB
	var releaser replicaReleaser
	deadline := time.Now().Add(2 * time.Second)
	for {
		r, releaser = db.replica.checkout()
		if r != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("could not check out a replica within 2s (refresh never yielded)")
		}
		time.Sleep(time.Millisecond)
	}
	defer releaser.Release()

	// Direct write to the replica should be rejected by SQLite's
	// query_only pragma — set automatically by READONLY deserialize.
	_, err := r.Exec(`INSERT INTO t VALUES (2, 'b')`)
	if err == nil {
		t.Fatal("expected SQLITE_READONLY on direct replica write; got no error")
	}
	low := strings.ToLower(err.Error())
	if !strings.Contains(low, "readonly") && !strings.Contains(low, "read-only") &&
		!strings.Contains(low, "query_only") && !strings.Contains(low, "attempt to write") {
		t.Fatalf("expected readonly-flavoured error, got: %v", err)
	}
}

// TestReplica_AllReplicasSeeSameState verifies that after a refresh,
// every replica returns identical state — proves the shared buffer is
// installed on each replica with the same bytes (no per-replica
// staleness from a partial migration).
func TestReplica_AllReplicasSeeSameState(t *testing.T) {
	t.Parallel()
	const replicas = 4
	db := openTestDB(t, Config{
		FilePath:               filepath.Join(t.TempDir(), "same.db"),
		FlushInterval:          -1,
		ReadPoolSize:           replicas,
		ReplicaRefreshInterval: 10 * time.Millisecond,
	})
	if _, err := db.Exec(`CREATE TABLE n (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 1; i <= 500; i++ {
		if _, err := db.Exec(`INSERT INTO n VALUES (?)`, i); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Wait for one replica to converge, then poll all replicas and
	// confirm they agree.
	waitForReplicaCount_t(t, db, "SELECT COUNT(*) FROM n", 500, 2*time.Second)

	// Check out replicas one at a time and verify each sees the same
	// count + the same sum-of-ids. Releaser returns the replica;
	// next checkout pulls the next from the channel, so all N get
	// exercised across the loop.
	seen := make(map[string]bool)
	for i := 0; i < replicas; i++ {
		r, releaser := db.replica.checkout()
		if r == nil {
			t.Fatalf("checkout %d returned nil", i)
		}
		var count, sum int64
		if err := r.QueryRow(`SELECT COUNT(*), COALESCE(SUM(id), 0) FROM n`).Scan(&count, &sum); err != nil {
			releaser.Release()
			t.Fatalf("query on replica %d: %v", i, err)
		}
		releaser.Release()
		if count != 500 {
			t.Errorf("replica %d count = %d, want 500", i, count)
		}
		// 1+2+…+500 = 125250
		if sum != 125250 {
			t.Errorf("replica %d sum = %d, want 125250", i, sum)
		}
		// Identify which replica this was (each *sql.DB has a unique
		// address) — used only to confirm we did exercise N distinct
		// replicas, not the same one N times.
		seen[fmt.Sprintf("%p", r)] = true
	}
	if len(seen) < replicas {
		// Not a hard fail — checkout/release may have returned the
		// same replica multiple times if there's any concurrent
		// activity — but worth a heads-up because a value < N hints
		// at a checkout bug.
		t.Logf("only saw %d distinct replicas across %d checkouts (channel ordering)", len(seen), replicas)
	}
}

// TestReplica_RefreshCycleNoLeak runs many refresh cycles back-to-back
// to exercise the shared-buffer alloc/free pattern. There's no direct
// way to detect a C-side leak from Go, but if we tracked stale buffers
// or freed the wrong pointer, we'd see either a panic from
// sqlite3_free or growing host memory across the loop. The test passes
// if the pool stays usable and Close() returns cleanly.
func TestReplica_RefreshCycleNoLeak(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{
		FilePath:               filepath.Join(t.TempDir(), "cycle.db"),
		FlushInterval:          -1,
		ReadPoolSize:           3,
		ReplicaRefreshInterval: 5 * time.Millisecond,
	})
	if _, err := db.Exec(`CREATE TABLE c (k INTEGER PRIMARY KEY, v BLOB)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Force ~50 refresh cycles by writing varying amounts of data
	// (the size differences force the shared buffer to be freed and
	// re-allocated each tick — same-size reuse would still alloc
	// since we don't reuse buffers across refreshes, but varying
	// size exercises the alloc/copy/free path with more diversity).
	for cycle := 0; cycle < 50; cycle++ {
		payload := make([]byte, (cycle%10+1)*128)
		if _, err := db.Exec(`INSERT INTO c (k, v) VALUES (?, ?)`, cycle, payload); err != nil {
			t.Fatalf("cycle %d insert: %v", cycle, err)
		}
		// Sleep long enough for at least one refresh tick.
		time.Sleep(15 * time.Millisecond)
		// Force a read through a replica to confirm it's still serving.
		var got int
		if err := db.QueryRow(`SELECT COUNT(*) FROM c`).Scan(&got); err != nil {
			t.Fatalf("cycle %d read: %v", cycle, err)
		}
	}

	// Final state: confirm we can still read everything. The
	// connection will close cleanly via t.Cleanup; close() exercises
	// the buffer free path under load.
	var final int
	if err := db.QueryRow(`SELECT COUNT(*) FROM c`).Scan(&final); err != nil {
		t.Fatalf("final read: %v", err)
	}
	if final != 50 {
		t.Fatalf("final row count = %d, want 50", final)
	}
}

// TestReplica_ConcurrentReadsDuringRefresh exercises the inUse.Wait
// quiescence guarantee: while refresh holds all replicas locally and
// is mid-migration, no checkout returns a replica (refreshing flag is
// set). Concurrent readers must transparently fall back to the writer
// without seeing stale or freed data.
func TestReplica_ConcurrentReadsDuringRefresh(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{
		FilePath:               filepath.Join(t.TempDir(), "concur.db"),
		FlushInterval:          -1,
		ReadPoolSize:           2,
		ReplicaRefreshInterval: 5 * time.Millisecond,
	})
	if _, err := db.Exec(`CREATE TABLE z (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Wait for at least one replica to see the table before starting
	// readers — otherwise the very first reader query races the
	// initial seed and surfaces "no such table" as a transient.
	waitForReplicaCount_t(t, db, "SELECT COUNT(*) FROM z", 0, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	// 8 reader goroutines hammering reads while a writer feeds new rows.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				var n int
				if err := db.QueryRow(`SELECT COUNT(*) FROM z`).Scan(&n); err != nil {
					// "no such table" can transiently surface immediately
					// after CREATE TABLE if a replica refresh races; not a
					// shared-buffer correctness failure. We pre-warm above
					// so this should be vanishingly rare here, but keep
					// the guard as a belt against test-environment noise.
					if strings.Contains(err.Error(), "no such table") {
						continue
					}
					// Use-after-free on a stale buffer would surface as
					// a SQLite error or a panic — both of which the t.Errorf
					// path catches before the goroutine exits.
					t.Errorf("reader: %v", err)
					return
				}
				_ = n
			}
		}()
	}

	// Writer goroutine inserts rows continuously.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 1
		for ctx.Err() == nil {
			if _, err := db.Exec(`INSERT INTO z VALUES (?)`, i); err != nil {
				if !errors.Is(err, ctx.Err()) {
					t.Errorf("writer: %v", err)
				}
				return
			}
			i++
		}
	}()

	wg.Wait()
}

// waitForReplicaCount_t is a small variant of waitForReplicaCount in
// restore_resizable_test.go that takes the SQL string so each test
// can name its own table. The "_t" suffix avoids collision with the
// existing helper.
func waitForReplicaCount_t(t *testing.T, db *DB, query string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastSeen int
	var lastErr error
	for time.Now().Before(deadline) {
		var got int
		err := db.QueryRow(query).Scan(&got)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				lastErr = err
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatalf("query: %v", err)
		}
		if got == want {
			return
		}
		lastSeen = got
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("count never converged to %d within %s (last seen %d, lastErr %v)", want, timeout, lastSeen, lastErr)
}
