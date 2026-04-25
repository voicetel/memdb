package memdb_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/backends"
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
// profile reveals WAL-specific overhead — the binary-format encoder plus
// the per-write write(2)+fsync(2) syscall pair. Before v1.4 this path was
// dominated by encoding/gob reflection (~25% of total CPU); the v1.4
// binary format eliminated gob from the profile entirely, so the bulk of
// CPU here is now the cgo boundary into SQLite and the fsync syscall.
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

// ---------------------------------------------------------------------------
// TestPProf_WAL_Replay
// ---------------------------------------------------------------------------

// TestPProf_WAL_Replay captures CPU and heap profiles of the cold-start path
// where Open() replays a non-trivial WAL into an empty :memory: database.
// The replay is the only place where the binary v1 decoder runs at scale —
// every entry is decoded, type-converted to []any, and re-Exec'd against
// SQLite — so this profile points directly at any regression in wal.Replay
// or in the Args round-trip.
//
// We bypass DurabilityWAL during the seeding phase because Open() also
// flushes-on-close, which would truncate the WAL before the replay round.
// Instead we (1) Open + Close once with InitSchema so the snapshot on disk
// contains the kv table, then (2) OpenWAL directly, append entries, close,
// then (3) re-Open with DurabilityWAL — Open() loads the snapshot first,
// then runs WAL replay against the now-existing schema. This is
// indistinguishable from a process that crashed between flushes.
func TestPProf_WAL_Replay(t *testing.T) {
	dir := pprofOutputDir(t)

	cfg := pprofConfig(t)
	cfg.Durability = memdb.DurabilityWAL

	// Step 1: bake the schema into a snapshot on disk so the eventual
	// replay has somewhere to INSERT. Close() flushes and truncates the WAL.
	seed, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}

	// Step 2: write the WAL out-of-band so it survives until replay.
	walPath := cfg.FilePath + ".wal"
	wal, err := memdb.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	const entries = 50_000
	for i := 0; i < entries; i++ {
		if err := wal.Append(memdb.WALEntry{
			SQL:  `INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
			Args: []any{fmt.Sprintf("wal-%d", i), fmt.Sprintf("v-%d", i)},
		}); err != nil {
			t.Fatalf("WAL Append: %v", err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("WAL Close: %v", err)
	}

	cpuPath := filepath.Join(dir, "pprof_wal_replay.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_wal_replay.heap.prof")

	var db *memdb.DB
	start := time.Now()
	err = profiling.CaptureCPUProfile(cpuPath, func() error {
		var openErr error
		db, openErr = memdb.Open(cfg)
		return openErr
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	defer db.Close()

	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	t.Logf("WAL replay: %d entries in %s (%.0f entries/s)  cpu=%s  heap=%s",
		entries, time.Since(start),
		float64(entries)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}

// ---------------------------------------------------------------------------
// TestPProf_Flush_Compressed / TestPProf_Flush_Encrypted
// ---------------------------------------------------------------------------

// TestPProf_Flush_Compressed captures profiles of the flush path with the
// snapshot stream wrapped through backends.CompressedBackend (zstd). zstd
// at default settings is the most CPU-intensive part of the production
// flush path; this profile makes the encoder cost vs. the SQLite backup
// API cost directly comparable to TestPProf_Flush.
func TestPProf_Flush_Compressed(t *testing.T) {
	flushBackendPProf(t, "compressed", func(local *backends.LocalBackend) memdb.Backend {
		return memdb.WrapBackend(&backends.CompressedBackend{Inner: local})
	})
}

// TestPProf_Flush_Encrypted captures profiles of the flush path with the
// snapshot stream wrapped through backends.EncryptedBackend (AES-256-GCM).
// AES-NI on modern CPUs makes this much cheaper than zstd, but the
// io.ReadAll buffering inside EncryptedBackend.Write means heap usage is
// non-trivial — the heap profile here will surface that.
func TestPProf_Flush_Encrypted(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	flushBackendPProf(t, "encrypted", func(local *backends.LocalBackend) memdb.Backend {
		return memdb.WrapBackend(&backends.EncryptedBackend{Inner: local, Key: key})
	})
}

// flushBackendPProf is the shared body for the wrapped-backend flush profiles.
// It seeds a fixed-size table, then runs N flushes through the wrapped
// backend so the captured CPU profile is dominated by the wrapping cost
// (compression / encryption / both) rather than SQLite's backup stepping.
func flushBackendPProf(t *testing.T, name string, wrap func(*backends.LocalBackend) memdb.Backend) {
	t.Helper()
	dir := pprofOutputDir(t)

	cfg := pprofConfig(t)
	cfg.Backend = wrap(&backends.LocalBackend{Path: cfg.FilePath})

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const rows = 50_000
	for i := 0; i < rows; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i),
		); err != nil {
			t.Fatal(err)
		}
	}

	cpuPath := filepath.Join(dir, "pprof_flush_"+name+".cpu.prof")
	heapPath := filepath.Join(dir, "pprof_flush_"+name+".heap.prof")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const flushes = 5
	start := time.Now()
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

	t.Logf("flush %s: %d×%d rows in %s (%.0f ms/flush)  cpu=%s  heap=%s",
		name, flushes, rows, time.Since(start),
		float64(time.Since(start).Milliseconds())/float64(flushes),
		cpuPath, heapPath)
}

// ---------------------------------------------------------------------------
// TestPProf_Writes_Contention
// ---------------------------------------------------------------------------

// TestPProf_Writes_Contention captures mutex and block profiles of a
// write-heavy workload run by many goroutines simultaneously. Because the
// writer is pinned to a single connection (MaxOpenConns=1) every concurrent
// caller serialises on database/sql's connection wait queue — the mutex /
// block profiles surface that wait directly so the tail-latency cost of
// sharing the writer is visible in pprof, not just in benchmark numbers.
func TestPProf_Writes_Contention(t *testing.T) {
	dir := pprofOutputDir(t)

	profiling.EnableMutexProfiling(1)
	profiling.EnableBlockProfiling(1)
	defer profiling.EnableMutexProfiling(0)
	defer profiling.EnableBlockProfiling(0)

	db, err := memdb.Open(pprofConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cpuPath := filepath.Join(dir, "pprof_writes_contention.cpu.prof")
	mutexPath := filepath.Join(dir, "pprof_writes_contention.mutex.prof")
	blockPath := filepath.Join(dir, "pprof_writes_contention.block.prof")

	const (
		workDuration = 3 * time.Second
		writers      = 16
	)
	var writes atomic.Int64

	start := time.Now()
	err = profiling.CaptureCPUProfile(cpuPath, func() error {
		var wg sync.WaitGroup
		done := make(chan struct{})
		for g := 0; g < writers; g++ {
			g := g
			wg.Add(1)
			go func() {
				defer wg.Done()
				i := g
				for {
					select {
					case <-done:
						return
					default:
					}
					if _, err := db.Exec(
						`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
						fmt.Sprintf("c-%d-%d", g, i), "v",
					); err != nil {
						return
					}
					i += writers
					writes.Add(1)
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

	if err := profiling.CaptureNamedProfile("mutex", mutexPath); err != nil {
		t.Fatalf("capture mutex: %v", err)
	}
	if err := profiling.CaptureNamedProfile("block", blockPath); err != nil {
		t.Fatalf("capture block: %v", err)
	}

	n := writes.Load()
	t.Logf("contended writes: %d across %d goroutines in %s (%.0f ops/s)  cpu=%s  mutex=%s  block=%s",
		n, writers, time.Since(start),
		float64(n)/time.Since(start).Seconds(),
		cpuPath, mutexPath, blockPath)
}
