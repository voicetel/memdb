package memdb_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/profiling"
)

// This file contains pprof-capturing benchmark helpers that exercise memdb
// under representative workloads and write CPU, heap, mutex, and block
// profiles to a caller-controlled directory. It intentionally uses *testing.T
// (rather than *testing.B) so the work is reproducible across runs without
// the variable iteration count that -bench imposes — the output files are
// intended for hand-driven analysis with `go tool pprof`.
//
// The tests are gated behind the MEMDB_PPROF environment variable so the
// default `go test ./...` run does not write profile artefacts. To run them:
//
//	MEMDB_PPROF=1 MEMDB_PPROF_DIR=/tmp/memdb-prof go test -run TestPProf_ -v .
//
// Then analyse with:
//
//	go tool pprof -http=: /tmp/memdb-prof/pprof_writes.cpu.prof
//	go tool pprof -http=: /tmp/memdb-prof/pprof_writes.heap.prof
//	go tool pprof -http=: /tmp/memdb-prof/pprof_reads.mutex.prof
//	go tool pprof -http=: /tmp/memdb-prof/pprof_reads.block.prof
//
// The scenarios are deliberately sized to run in well under a minute each so
// that they can be invoked routinely in CI when a performance regression is
// suspected.

// pprofOutputDir returns the directory where profiles should be written, or
// skips the test when pprof capture is not enabled for this run.
func pprofOutputDir(t *testing.T) string {
	t.Helper()
	if os.Getenv("MEMDB_PPROF") == "" {
		t.Skip("set MEMDB_PPROF=1 to enable pprof capture tests")
	}
	dir := os.Getenv("MEMDB_PPROF_DIR")
	if dir == "" {
		dir = t.TempDir()
		t.Logf("MEMDB_PPROF_DIR not set — writing to %s", dir)
	} else {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	return dir
}

// pprofConfig opens a fresh memdb with a silent logger and the standard kv
// schema. Background flushing is disabled so the profile reflects only the
// workload being measured, not periodic I/O.
func pprofConfig(t *testing.T) memdb.Config {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "memdb-pprof-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	os.Remove(f.Name())

	return memdb.Config{
		FilePath:      f.Name(),
		FlushInterval: -1,
		Logger:        silentLogger(),
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`
				CREATE TABLE IF NOT EXISTS kv (
					key   TEXT PRIMARY KEY,
					value TEXT NOT NULL
				)
			`)
			return err
		},
	}
}

// ---------------------------------------------------------------------------
// TestPProf_Writes
// ---------------------------------------------------------------------------

// TestPProf_Writes captures CPU and heap profiles of a pure-write workload.
// A single goroutine issues INSERT OR REPLACE statements as fast as it can
// for a fixed wall-clock duration so the profile reflects the per-write cost
// rather than open/close overhead.
func TestPProf_Writes(t *testing.T) {
	dir := pprofOutputDir(t)

	db, err := memdb.Open(pprofConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cpuPath := filepath.Join(dir, "pprof_writes.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_writes.heap.prof")

	const workDuration = 3 * time.Second
	var writes atomic.Int64

	start := time.Now()
	err = profiling.CaptureCPUProfile(cpuPath, func() error {
		deadline := time.Now().Add(workDuration)
		i := 0
		for time.Now().Before(deadline) {
			if _, err := db.Exec(
				`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
				fmt.Sprintf("k-%d", i), "v",
			); err != nil {
				return err
			}
			i++
			writes.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}

	// Heap profile after the run so live-heap samples include any caches
	// that grew during the workload.
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	n := writes.Load()
	t.Logf("writes: %d in %s (%.0f ops/s)  cpu=%s  heap=%s",
		n, time.Since(start), float64(n)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}

// ---------------------------------------------------------------------------
// TestPProf_Writes_WAL
// ---------------------------------------------------------------------------

// TestPProf_Writes_WAL is TestPProf_Writes with DurabilityWAL enabled so the
// profile reveals WAL-specific overhead (gob encoding, fsync).
func TestPProf_Writes_WAL(t *testing.T) {
	dir := pprofOutputDir(t)

	cfg := pprofConfig(t)
	cfg.Durability = memdb.DurabilityWAL

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cpuPath := filepath.Join(dir, "pprof_writes_wal.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_writes_wal.heap.prof")

	const workDuration = 3 * time.Second
	var writes atomic.Int64

	start := time.Now()
	err = profiling.CaptureCPUProfile(cpuPath, func() error {
		deadline := time.Now().Add(workDuration)
		i := 0
		for time.Now().Before(deadline) {
			if _, err := db.Exec(
				`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
				fmt.Sprintf("k-%d", i), "v",
			); err != nil {
				return err
			}
			i++
			writes.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	n := writes.Load()
	t.Logf("WAL writes: %d in %s (%.0f ops/s)  cpu=%s  heap=%s",
		n, time.Since(start), float64(n)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}

// ---------------------------------------------------------------------------
// TestPProf_Reads_Replicas
// ---------------------------------------------------------------------------

// TestPProf_Reads_Replicas captures CPU, mutex, and block profiles of a
// concurrent read-heavy workload against a read replica pool. This is the
// scenario most sensitive to contention regressions — the mutex and block
// profiles highlight any place where readers serialise on a shared lock or
// channel.
func TestPProf_Reads_Replicas(t *testing.T) {
	dir := pprofOutputDir(t)

	// Enable mutex and block sampling before the workload starts. Sampling
	// every event (fraction=1 / rate=1) is noisy but appropriate for a short
	// diagnostic run.
	profiling.EnableMutexProfiling(1)
	profiling.EnableBlockProfiling(1)
	defer profiling.EnableMutexProfiling(0)
	defer profiling.EnableBlockProfiling(0)

	cfg := pprofConfig(t)
	cfg.ReadPoolSize = runtime.GOMAXPROCS(0)
	cfg.ReplicaRefreshInterval = 2 * time.Millisecond

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Seed rows so the readers always hit data.
	const seed = 1000
	for i := 0; i < seed; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("seed-%d", i), fmt.Sprintf("val-%d", i),
		); err != nil {
			t.Fatal(err)
		}
	}
	// Give the replica pool a moment to pick up the seed.
	time.Sleep(20 * time.Millisecond)

	cpuPath := filepath.Join(dir, "pprof_reads.cpu.prof")
	mutexPath := filepath.Join(dir, "pprof_reads.mutex.prof")
	blockPath := filepath.Join(dir, "pprof_reads.block.prof")

	const (
		workDuration = 3 * time.Second
		readers      = 16
	)
	var reads atomic.Int64

	start := time.Now()
	err = profiling.CaptureCPUProfile(cpuPath, func() error {
		var wg sync.WaitGroup
		done := make(chan struct{})
		for g := 0; g < readers; g++ {
			g := g
			wg.Add(1)
			go func() {
				defer wg.Done()
				var val string
				i := g
				for {
					select {
					case <-done:
						return
					default:
					}
					_ = db.QueryRow(
						`SELECT value FROM kv WHERE key = ?`,
						fmt.Sprintf("seed-%d", i%seed),
					).Scan(&val)
					i++
					reads.Add(1)
				}
			}()
		}
		time.Sleep(workDuration)
		close(done)
		wg.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}

	// Snapshot the mutex/block profiles AFTER the workload has finished
	// running — samples accumulate until the rate is reset to zero.
	if err := profiling.CaptureNamedProfile("mutex", mutexPath); err != nil {
		t.Fatalf("capture mutex: %v", err)
	}
	if err := profiling.CaptureNamedProfile("block", blockPath); err != nil {
		t.Fatalf("capture block: %v", err)
	}

	n := reads.Load()
	t.Logf("reads: %d across %d goroutines in %s (%.0f ops/s)  cpu=%s  mutex=%s  block=%s",
		n, readers, time.Since(start),
		float64(n)/time.Since(start).Seconds(),
		cpuPath, mutexPath, blockPath)
}

// ---------------------------------------------------------------------------
// TestPProf_MixedReadWrite
// ---------------------------------------------------------------------------

// TestPProf_MixedReadWrite exercises a representative application workload:
// a single writer goroutine and several reader goroutines running against
// the replica pool at the same time. CPU and heap profiles are captured.
func TestPProf_MixedReadWrite(t *testing.T) {
	dir := pprofOutputDir(t)

	cfg := pprofConfig(t)
	cfg.ReadPoolSize = runtime.GOMAXPROCS(0)
	cfg.ReplicaRefreshInterval = 2 * time.Millisecond

	runMixedPProf(t, cfg, dir, "pprof_mixed")
}

// TestPProf_MixedReadWrite_DefaultRefresh is the apples-to-apples comparison
// of the mixed workload using the package's production ReplicaRefreshInterval
// default (currently 50 ms) instead of the aggressive 2 ms value that
// TestPProf_MixedReadWrite uses.
//
// Motivation: the first round of pprof analysis attributed ~43% of CPU in
// the 2 ms mixed profile to replicaRefreshLoop. This variant demonstrates
// the cost of the default — which BenchmarkReplicaRefreshInterval showed
// recovers nearly all of the writer throughput — so operators can compare
// the two profiles side by side and understand the refresh-interval knob.
func TestPProf_MixedReadWrite_DefaultRefresh(t *testing.T) {
	dir := pprofOutputDir(t)

	cfg := pprofConfig(t)
	cfg.ReadPoolSize = runtime.GOMAXPROCS(0)
	// Leave ReplicaRefreshInterval at zero so applyDefaults fills in the
	// production default. This is what a real caller who sets ReadPoolSize
	// without tuning the refresh interval will see in pprof.

	runMixedPProf(t, cfg, dir, "pprof_mixed_default")
}

// runMixedPProf factors the common mixed-workload body used by both
// TestPProf_MixedReadWrite and TestPProf_MixedReadWrite_DefaultRefresh so
// the two variants produce strictly comparable profiles.
func runMixedPProf(t *testing.T, cfg memdb.Config, dir, namePrefix string) {
	t.Helper()

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Seed rows so the readers always hit data.
	for i := 0; i < 500; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("seed-%d", i), fmt.Sprintf("val-%d", i),
		); err != nil {
			t.Fatal(err)
		}
	}
	// Sleep long enough for the configured refresh interval to fire at
	// least once so readers see the seeded rows when the timer starts.
	// max(50ms, 2×interval) handles both the aggressive 2 ms variant and
	// the 50 ms default.
	warmup := 50 * time.Millisecond
	if cfg.ReplicaRefreshInterval > 0 && 2*cfg.ReplicaRefreshInterval > warmup {
		warmup = 2 * cfg.ReplicaRefreshInterval
	}
	time.Sleep(warmup)

	cpuPath := filepath.Join(dir, namePrefix+".cpu.prof")
	heapPath := filepath.Join(dir, namePrefix+".heap.prof")

	const (
		workDuration = 3 * time.Second
		readers      = 8
	)
	var writes, reads atomic.Int64

	start := time.Now()
	err = profiling.CaptureCPUProfile(cpuPath, func() error {
		var wg sync.WaitGroup
		done := make(chan struct{})

		// Writer goroutine.
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := db.Exec(
					`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
					fmt.Sprintf("mixed-%d", i), "v",
				); err != nil {
					return
				}
				i++
				writes.Add(1)
			}
		}()

		// Reader goroutines.
		for g := 0; g < readers; g++ {
			g := g
			wg.Add(1)
			go func() {
				defer wg.Done()
				var val string
				i := g
				for {
					select {
					case <-done:
						return
					default:
					}
					_ = db.QueryRow(
						`SELECT value FROM kv WHERE key = ?`,
						fmt.Sprintf("seed-%d", i%500),
					).Scan(&val)
					i++
					reads.Add(1)
				}
			}()
		}

		time.Sleep(workDuration)
		close(done)
		wg.Wait()
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	w, r := writes.Load(), reads.Load()
	t.Logf("%s (refresh=%s): %d writes, %d reads in %s (%.0f write/s, %.0f read/s)  cpu=%s  heap=%s",
		namePrefix, cfg.ReplicaRefreshInterval,
		w, r, time.Since(start),
		float64(w)/time.Since(start).Seconds(),
		float64(r)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}

// ---------------------------------------------------------------------------
// TestPProf_Flush
// ---------------------------------------------------------------------------

// TestPProf_Flush captures profiles of the flush path at a non-trivial
// table size. The SQLite backup API is the most CPU-intensive code path in
// memdb; this profile is useful for sanity-checking changes to the backup
// stepping logic.
func TestPProf_Flush(t *testing.T) {
	dir := pprofOutputDir(t)

	db, err := memdb.Open(pprofConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Seed enough rows that the flush has real work to do.
	const rows = 50_000
	for i := 0; i < rows; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i),
		); err != nil {
			t.Fatal(err)
		}
	}

	cpuPath := filepath.Join(dir, "pprof_flush.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_flush.heap.prof")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	const flushes = 5
	err = profiling.CaptureCPUProfile(cpuPath, func() error {
		for i := 0; i < flushes; i++ {
			if err := db.Flush(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	t.Logf("flushed %d×%d rows in %s (%.0f ms/flush)  cpu=%s  heap=%s",
		flushes, rows, time.Since(start),
		float64(time.Since(start).Milliseconds())/float64(flushes),
		cpuPath, heapPath)
}
