package memdb

import (
	"path/filepath"
	"testing"
	"time"
)

// TestReplicaRefreshDivergenceCount_NoPool returns 0 when no replica
// pool is configured. Pins the documented contract for callers that
// scrape the counter unconditionally without inspecting ReadPoolSize.
func TestReplicaRefreshDivergenceCount_NoPool(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{
		FilePath:      filepath.Join(t.TempDir(), "nopool.db"),
		FlushInterval: -1,
	})
	if got := db.ReplicaRefreshDivergenceCount(); got != 0 {
		t.Fatalf("ReplicaRefreshDivergenceCount with no pool: want 0, got %d", got)
	}
}

// TestReplicaRefreshDivergenceCount_HappyPath verifies the counter
// stays at zero across a normal write/refresh cycle — divergence is
// reserved for actual partial-migration failures, not routine refresh
// activity.
func TestReplicaRefreshDivergenceCount_HappyPath(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{
		FilePath:               filepath.Join(t.TempDir(), "happy.db"),
		FlushInterval:          -1,
		ReadPoolSize:           3,
		ReplicaRefreshInterval: 5 * time.Millisecond,
	})
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Drive ~20 refresh cycles with continuous writes so refresh
	// actually runs (writeGen short-circuit only skips when idle).
	for i := 0; i < 100; i++ {
		if _, err := db.Exec(`INSERT INTO t VALUES (?)`, i); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	if got := db.ReplicaRefreshDivergenceCount(); got != 0 {
		t.Fatalf("happy-path refresh should not register divergence, got %d", got)
	}
}

// TestReplicaRefreshDivergenceCount_Wiring exercises the getter by
// bumping the internal counter directly. Triggering an actual
// divergence event requires sqlite3_deserialize to fail, which on a
// clean in-memory DB essentially only happens under OOM — too
// brittle to simulate reliably in a unit test. This guards the
// observability plumbing: the value DB.ReplicaRefreshDivergenceCount
// returns matches what refresh's partial-fail path writes to the
// atomic counter.
func TestReplicaRefreshDivergenceCount_Wiring(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, Config{
		FilePath:               filepath.Join(t.TempDir(), "wiring.db"),
		FlushInterval:          -1,
		ReadPoolSize:           2,
		ReplicaRefreshInterval: 50 * time.Millisecond,
	})

	if got := db.ReplicaRefreshDivergenceCount(); got != 0 {
		t.Fatalf("initial: want 0, got %d", got)
	}

	// Bump the same atomic that the partial-fail path bumps.
	db.replica.refreshDivergenceCount.Add(1)
	db.replica.refreshDivergenceCount.Add(2)

	if got := db.ReplicaRefreshDivergenceCount(); got != 3 {
		t.Fatalf("after Add(1)+Add(2): want 3, got %d", got)
	}
}
