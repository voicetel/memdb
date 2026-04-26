package memdb_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voicetel/memdb"
)

// discardLogger returns a *slog.Logger that silently drops all output.
// Used in benchmarks to prevent slog flush/restore events from printing
// to stdout and contaminating benchmark output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// ── helpers ───────────────────────────────────────────────────────────────────

func benchConfig(b *testing.B) memdb.Config {
	b.Helper()
	f, err := os.CreateTemp(b.TempDir(), "memdb-bench-*.db")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()
	os.Remove(f.Name())

	return memdb.Config{
		FilePath:      f.Name(),
		FlushInterval: -1,
		InitSchema:    schemaKV,
		Logger:        discardLogger(),
	}
}

func benchConfigWAL(b *testing.B) memdb.Config {
	b.Helper()
	cfg := benchConfig(b)
	cfg.Durability = memdb.DurabilityWAL
	return cfg
}

func schemaKV(db *memdb.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS kv (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`)
	return err
}

// openBench opens a DB, seeds n rows, resets the benchmark timer, and returns
// the DB. A Cleanup is registered to close it automatically.
func openBench(b *testing.B, cfg memdb.Config, rows int) *memdb.DB {
	b.Helper()
	db, err := memdb.Open(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	for i := 0; i < rows; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("seed-key-%d", i),
			fmt.Sprintf("seed-val-%d", i),
		); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	return db
}

// ── single-row writes ─────────────────────────────────────────────────────────

// BenchmarkExec_Insert measures the cost of a single parameterised INSERT.
func BenchmarkExec_Insert(b *testing.B) {
	db := openBench(b, benchConfig(b), 0)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := db.Exec(
				`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
				fmt.Sprintf("k-%d", i), "v",
			); err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// BenchmarkExec_Insert_WAL measures INSERT throughput with WAL durability enabled.
// This reveals the per-write fsync overhead added by DurabilityWAL.
func BenchmarkExec_Insert_WAL(b *testing.B) {
	db := openBench(b, benchConfigWAL(b), 0)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := db.Exec(
				`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
				fmt.Sprintf("k-%d", i), "v",
			); err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// BenchmarkExec_Update measures a point UPDATE against a pre-populated table.
func BenchmarkExec_Update(b *testing.B) {
	db := openBench(b, benchConfig(b), 1000)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := db.Exec(
				`UPDATE kv SET value = ? WHERE key = ?`,
				"updated", fmt.Sprintf("seed-key-%d", i%1000),
			); err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// BenchmarkExec_Delete measures a point DELETE by primary key.
// Rows are pre-inserted before the timer starts so each iteration has a row to delete.
func BenchmarkExec_Delete(b *testing.B) {
	db := openBench(b, benchConfig(b), 0)

	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("del-key-%d", i), "v",
		); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`DELETE FROM kv WHERE key = ?`, fmt.Sprintf("del-key-%d", i),
		); err != nil {
			b.Error(err)
		}
	}
}

// ── single-row reads ──────────────────────────────────────────────────────────

// BenchmarkQueryRow measures a point-lookup via QueryRow + Scan.
func BenchmarkQueryRow(b *testing.B) {
	db := openBench(b, benchConfig(b), 1000)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		var val string
		for pb.Next() {
			if err := db.QueryRow(
				`SELECT value FROM kv WHERE key = ?`,
				fmt.Sprintf("seed-key-%d", i%1000),
			).Scan(&val); err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// BenchmarkQuery_RangeScan measures a range scan that returns 100 rows per call.
func BenchmarkQuery_RangeScan(b *testing.B) {
	db := openBench(b, benchConfig(b), 1000)
	b.RunParallel(func(pb *testing.PB) {
		var k, v string
		for pb.Next() {
			rows, err := db.Query(`SELECT key, value FROM kv LIMIT 100`)
			if err != nil {
				b.Error(err)
				continue
			}
			for rows.Next() {
				if err := rows.Scan(&k, &v); err != nil {
					b.Error(err)
				}
			}
			rows.Close()
		}
	})
}

// ── transactions ──────────────────────────────────────────────────────────────

// BenchmarkWithTx_SingleInsert measures the overhead of WithTx wrapping one INSERT.
func BenchmarkWithTx_SingleInsert(b *testing.B) {
	db := openBench(b, benchConfig(b), 0)
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("tx-k-%d", i)
			if err := memdb.WithTx(ctx, db, func(tx *sql.Tx) error {
				_, err := tx.Exec(
					`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`, key, "v",
				)
				return err
			}); err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// BenchmarkWithTx_BatchInsert measures committing 50 INSERTs inside a single transaction.
// Compare with BenchmarkExec_Insert to see the amortised benefit of batching.
func BenchmarkWithTx_BatchInsert(b *testing.B) {
	const batchSize = 50
	db := openBench(b, benchConfig(b), 0)
	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			base := i * batchSize
			if err := memdb.WithTx(ctx, db, func(tx *sql.Tx) error {
				for j := 0; j < batchSize; j++ {
					if _, err := tx.Exec(
						`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
						fmt.Sprintf("batch-k-%d", base+j), "v",
					); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// BenchmarkWithTx_ReadOnly measures a read-only transaction (BEGIN + SELECT + COMMIT).
func BenchmarkWithTx_ReadOnly(b *testing.B) {
	db := openBench(b, benchConfig(b), 500)
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := memdb.WithTx(ctx, db, func(tx *sql.Tx) error {
				var v string
				return tx.QueryRow(
					`SELECT value FROM kv WHERE key = ?`,
					fmt.Sprintf("seed-key-%d", i%500),
				).Scan(&v)
			}); err != nil {
				b.Error(err)
			}
			i++
		}
	})
}

// ── flush ─────────────────────────────────────────────────────────────────────

// BenchmarkFlush measures the cost of a full snapshot flush at increasing table sizes.
// The sub-benchmark names make it easy to spot how flush time scales with data volume.
func BenchmarkFlush(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		n := n
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			db := openBench(b, benchConfig(b), n)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Flush(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ── mixed read/write concurrency ──────────────────────────────────────────────

// BenchmarkConcurrentReadWrite models realistic mixed workloads at three
// read/write ratios. Use -benchtime=5s and -cpu=1,4,8 to see contention curves.
func BenchmarkConcurrentReadWrite(b *testing.B) {
	cases := []struct {
		name         string
		writePercent int // out of 100 operations, how many are writes
	}{
		{"writes=10%", 10},
		{"writes=50%", 50},
		{"writes=90%", 90},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			db := openBench(b, benchConfig(b), 1000)
			var counter atomic.Int64

			b.RunParallel(func(pb *testing.PB) {
				var val string
				for pb.Next() {
					n := counter.Add(1)
					if int(n%100) < tc.writePercent {
						if _, err := db.Exec(
							`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
							fmt.Sprintf("w-%d", n), "v",
						); err != nil {
							b.Error(err)
						}
					} else {
						_ = db.QueryRow(
							`SELECT value FROM kv WHERE key = ?`,
							fmt.Sprintf("seed-key-%d", n%1000),
						).Scan(&val)
					}
				}
			})
		})
	}
}

// ── open / restore lifecycle ──────────────────────────────────────────────────

// BenchmarkReplicaRefreshInterval sweeps ReplicaRefreshInterval to quantify
// the CPU cost of the background replica refresh goroutine on a mixed
// read/write workload.
//
// Motivation (from pprof analysis):
//
//	Under a mixed workload with ReplicaRefreshInterval=2ms and a 500-row
//	dataset, pprof attributed ~43% of total CPU to replicaRefreshLoop —
//	driven by sqlite3_deserialize memmoves into every replica. The
//	documented default of 1ms would be even worse.
//
// This benchmark reports write throughput (ns/op) while concurrent readers
// hammer the replica pool, across a range of refresh intervals. The
// sub-benchmark names make it easy to spot the knee in the curve and pick
// a safe default for a given deployment's dataset size.
//
// Representative run (1000-row dataset, 8 concurrent readers; v1.4.0
// reference numbers in BENCHMARKS.md):
//
//	refresh=250µs   — ~90 µs/write; readers starved, refresh dominates CPU
//	refresh=1ms     — ~84 µs/write; previous default (raised to 50ms in v1.4)
//	refresh=5ms     — ~79 µs/write; floor below which Open emits a warning
//	refresh=25ms    — ~53 µs/write; knee of the curve on small DBs
//	refresh=100ms   — ~11 µs/write; writes at full speed, reads ≤100ms stale
//
// The current package default is 50ms — between the 25ms and 100ms rows
// above — chosen as the balance between read staleness and writer CPU
// cost; see Config.ReplicaRefreshInterval and BENCHMARKS.md for the full
// sweep and pprof-derived justification.
//
// Use with:
//
//	go test -bench=BenchmarkReplicaRefreshInterval -benchmem -benchtime=3s .
func BenchmarkReplicaRefreshInterval(b *testing.B) {
	intervals := []struct {
		name     string
		interval time.Duration
	}{
		{"refresh=250µs", 250 * time.Microsecond},
		{"refresh=1ms", 1 * time.Millisecond},
		{"refresh=5ms", 5 * time.Millisecond},
		{"refresh=25ms", 25 * time.Millisecond},
		{"refresh=100ms", 100 * time.Millisecond},
	}

	for _, tc := range intervals {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			cfg := benchConfig(b)
			cfg.ReadPoolSize = 8
			cfg.ReplicaRefreshInterval = tc.interval

			db := openBench(b, cfg, 1000)

			// Spin up background readers to exercise the replica pool so the
			// refresh goroutine is doing real contended work. The readers
			// are bounded by the b.N writer loop via the done channel.
			const readers = 8
			done := make(chan struct{})
			var wg sync.WaitGroup
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
							fmt.Sprintf("seed-key-%d", i%1000),
						).Scan(&val)
						i++
					}
				}()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Exec(
					`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
					fmt.Sprintf("rw-%d", i), "v",
				); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			close(done)
			wg.Wait()
		})
	}
}

// BenchmarkOpen measures the cost of opening a fresh DB with schema initialisation.
func BenchmarkOpen(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f, err := os.CreateTemp(b.TempDir(), "memdb-open-*.db")
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
		os.Remove(f.Name())
		cfg := memdb.Config{
			FilePath:      f.Name(),
			FlushInterval: -1,
			InitSchema:    schemaKV,
			Logger:        discardLogger(),
		}
		b.StartTimer()

		db, err := memdb.Open(cfg)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		db.Close()
		b.StartTimer()
	}
}

// BenchmarkOpenRestore measures open + snapshot restore with a 1 000-row DB.
// A single snapshot is written once; each iteration restores it into a fresh
// in-memory DB, exercising the full SQLite backup-API restore path.
func BenchmarkOpenRestore(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "memdb-restore-*.db")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()
	os.Remove(f.Name())

	cfg := memdb.Config{
		FilePath:      f.Name(),
		FlushInterval: -1,
		InitSchema:    schemaKV,
		Logger:        discardLogger(),
	}

	// Seed and flush once.
	seed, err := memdb.Open(cfg)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := seed.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i),
		); err != nil {
			b.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := seed.Flush(ctx); err != nil {
		b.Fatal(err)
	}
	seed.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := memdb.Open(cfg)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		db.Close()
		b.StartTimer()
	}
}

// ── WAL primitives ────────────────────────────────────────────────────────────

// BenchmarkWAL_Append measures the raw cost of a WAL append including fsync.
func BenchmarkWAL_Append(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "memdb-wal-*.wal")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()

	wal, err := memdb.OpenWAL(f.Name())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { wal.Close() })

	entry := memdb.WALEntry{
		Timestamp: time.Now().UnixNano(),
		SQL:       `INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
		Args:      []any{"bench-key", "bench-val"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Seq = wal.NextSeq()
		if err := wal.Append(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWAL_Append_Parallel measures concurrent Append calls hitting
// the WAL directly (bypassing the SQLite writer-connection mutex that
// would otherwise serialise upstream of the WAL in a full Exec path).
//
// Group commit shows here as amortised fsync cost: N writers in flight
// fan in to ~1 fsync per batch window, so per-op latency drops as
// concurrency rises while raw fsync per call would stay flat.
func BenchmarkWAL_Append_Parallel(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "memdb-wal-parallel-*.wal")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()

	wal, err := memdb.OpenWAL(f.Name())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { wal.Close() })

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		entry := memdb.WALEntry{
			Timestamp: time.Now().UnixNano(),
			SQL:       `INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
			Args:      []any{"bench-key", "bench-val"},
		}
		for pb.Next() {
			entry.Seq = wal.NextSeq()
			if err := wal.Append(entry); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkWAL_Replay measures the cost of reading and decoding a pre-filled WAL.
func BenchmarkWAL_Replay(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		n := n
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			f, err := os.CreateTemp(b.TempDir(), "memdb-wal-replay-*.wal")
			if err != nil {
				b.Fatal(err)
			}
			f.Close()

			wal, err := memdb.OpenWAL(f.Name())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { wal.Close() })

			for i := 0; i < n; i++ {
				e := memdb.WALEntry{
					Seq:       wal.NextSeq(),
					Timestamp: time.Now().UnixNano(),
					SQL:       `INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
					Args:      []any{fmt.Sprintf("k-%d", i), "v"},
				}
				if err := wal.Append(e); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := wal.Replay(func(memdb.WALEntry) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
