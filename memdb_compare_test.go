package memdb_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/voicetel/memdb"
)

// ── plain SQLite helper ───────────────────────────────────────────────────────

// fileDB opens a plain file-backed SQLite database with the given synchronous
// pragma and seeds it with the kv schema. It is closed automatically via
// b.Cleanup. Three durability levels are exercised:
//
//   - "off"    — PRAGMA synchronous=OFF  (no fsync; data loss on OS crash)
//   - "normal" — PRAGMA synchronous=NORMAL (fsync at checkpoint; WAL default)
//   - "full"   — PRAGMA synchronous=FULL (fsync on every commit; safest)
func fileDB(b *testing.B, synchronous string) *sql.DB {
	b.Helper()

	f, err := os.CreateTemp(b.TempDir(), "sqlite-file-*.db")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()

	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA cache_size=-64000",
		fmt.Sprintf("PRAGMA synchronous=%s", synchronous),
		"PRAGMA busy_timeout=5000",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			b.Fatalf("pragma %q: %v", p, err)
		}
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS kv (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`); err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { db.Close() })
	return db
}

// seedFileDB inserts n rows into a plain *sql.DB before the timer starts.
func seedFileDB(b *testing.B, db *sql.DB, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		if _, err := db.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("seed-key-%d", i),
			fmt.Sprintf("seed-val-%d", i),
		); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
}

// ── INSERT ────────────────────────────────────────────────────────────────────

// BenchmarkCompare_Insert runs an INSERT benchmark for every combination of
// backend (memdb, file/off, file/normal, file/full), making the relative cost
// of each durability level directly visible in the output.
func BenchmarkCompare_Insert(b *testing.B) {
	b.Run("memdb", func(b *testing.B) {
		db := openBench(b, benchConfig(b), 0)
		i := 0
		for b.Loop() {
			if _, err := db.Exec(
				`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
				fmt.Sprintf("k-%d", i), "v",
			); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	b.Run("memdb/wal", func(b *testing.B) {
		db := openBench(b, benchConfigWAL(b), 0)
		i := 0
		for b.Loop() {
			if _, err := db.Exec(
				`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
				fmt.Sprintf("k-%d", i), "v",
			); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	for _, sync := range []string{"off", "normal", "full"} {
		sync := sync
		b.Run("file/sync="+sync, func(b *testing.B) {
			db := fileDB(b, sync)
			i := 0
			for b.Loop() {
				if _, err := db.Exec(
					`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
					fmt.Sprintf("k-%d", i), "v",
				); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

// ── UPDATE ────────────────────────────────────────────────────────────────────

func BenchmarkCompare_Update(b *testing.B) {
	const seedRows = 1000

	b.Run("memdb", func(b *testing.B) {
		db := openBench(b, benchConfig(b), seedRows)
		i := 0
		for b.Loop() {
			if _, err := db.Exec(
				`UPDATE kv SET value = ? WHERE key = ?`,
				"updated", fmt.Sprintf("seed-key-%d", i%seedRows),
			); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	for _, sync := range []string{"off", "normal", "full"} {
		sync := sync
		b.Run("file/sync="+sync, func(b *testing.B) {
			db := fileDB(b, sync)
			seedFileDB(b, db, seedRows)
			i := 0
			for b.Loop() {
				if _, err := db.Exec(
					`UPDATE kv SET value = ? WHERE key = ?`,
					"updated", fmt.Sprintf("seed-key-%d", i%seedRows),
				); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

// ── QueryRow (point lookup) ───────────────────────────────────────────────────

func BenchmarkCompare_QueryRow(b *testing.B) {
	const seedRows = 1000

	b.Run("memdb", func(b *testing.B) {
		db := openBench(b, benchConfig(b), seedRows)
		var val string
		i := 0
		for b.Loop() {
			if err := db.QueryRow(
				`SELECT value FROM kv WHERE key = ?`,
				fmt.Sprintf("seed-key-%d", i%seedRows),
			).Scan(&val); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	for _, sync := range []string{"off", "normal", "full"} {
		sync := sync
		b.Run("file/sync="+sync, func(b *testing.B) {
			db := fileDB(b, sync)
			seedFileDB(b, db, seedRows)
			var val string
			i := 0
			for b.Loop() {
				if err := db.QueryRow(
					`SELECT value FROM kv WHERE key = ?`,
					fmt.Sprintf("seed-key-%d", i%seedRows),
				).Scan(&val); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

// ── range scan (100 rows) ─────────────────────────────────────────────────────

func BenchmarkCompare_RangeScan(b *testing.B) {
	const seedRows = 1000

	b.Run("memdb", func(b *testing.B) {
		db := openBench(b, benchConfig(b), seedRows)
		var k, v string
		for b.Loop() {
			rows, err := db.Query(`SELECT key, value FROM kv LIMIT 100`)
			if err != nil {
				b.Fatal(err)
			}
			for rows.Next() {
				if err := rows.Scan(&k, &v); err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})

	for _, sync := range []string{"off", "normal", "full"} {
		sync := sync
		b.Run("file/sync="+sync, func(b *testing.B) {
			db := fileDB(b, sync)
			seedFileDB(b, db, seedRows)
			var k, v string
			for b.Loop() {
				rows, err := db.Query(`SELECT key, value FROM kv LIMIT 100`)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					if err := rows.Scan(&k, &v); err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}

// ── batch INSERT (50 rows in one transaction) ─────────────────────────────────

func BenchmarkCompare_BatchInsert(b *testing.B) {
	const batchSize = 50

	b.Run("memdb", func(b *testing.B) {
		db := openBench(b, benchConfig(b), 0)
		ctx := b.Context()
		i := 0
		for b.Loop() {
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
				b.Fatal(err)
			}
			i++
		}
	})

	for _, sync := range []string{"off", "normal", "full"} {
		sync := sync
		b.Run("file/sync="+sync, func(b *testing.B) {
			db := fileDB(b, sync)
			i := 0
			for b.Loop() {
				base := i * batchSize
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				for j := 0; j < batchSize; j++ {
					if _, err := tx.Exec(
						`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
						fmt.Sprintf("batch-k-%d", base+j), "v",
					); err != nil {
						_ = tx.Rollback()
						b.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

// ── concurrent reads (4 goroutines) ──────────────────────────────────────────

// BenchmarkCompare_ConcurrentRead exercises parallel point-lookups. Because
// SQLite in WAL mode allows concurrent readers on a file DB (unlike journal
// mode), this exposes whether the single-connection constraint in memdb is a
// meaningful bottleneck vs a file DB that can open a read connection pool.
func BenchmarkCompare_ConcurrentRead(b *testing.B) {
	const seedRows = 1000

	b.Run("memdb", func(b *testing.B) {
		db := openBench(b, benchConfig(b), seedRows)
		b.SetParallelism(4)
		b.RunParallel(func(pb *testing.PB) {
			var val string
			i := 0
			for pb.Next() {
				if err := db.QueryRow(
					`SELECT value FROM kv WHERE key = ?`,
					fmt.Sprintf("seed-key-%d", i%seedRows),
				).Scan(&val); err != nil {
					b.Error(err)
				}
				i++
			}
		})
	})

	b.Run("memdb/pool", func(b *testing.B) {
		cfg := benchConfig(b)
		cfg.ReadPoolSize = 4
		cfg.ReplicaRefreshInterval = time.Millisecond
		db := openBench(b, cfg, seedRows)
		// openBench calls b.ResetTimer after seeding, but the background
		// refresh goroutine may not have fired yet. Sleep for a few refresh
		// cycles so every replica holds the seeded rows before we measure.
		b.StopTimer()
		time.Sleep(20 * time.Millisecond)
		b.StartTimer()
		b.SetParallelism(4)
		b.RunParallel(func(pb *testing.PB) {
			var val string
			i := 0
			for pb.Next() {
				if err := db.QueryRow(
					`SELECT value FROM kv WHERE key = ?`,
					fmt.Sprintf("seed-key-%d", i%seedRows),
				).Scan(&val); err != nil {
					b.Error(err)
				}
				i++
			}
		})
	})

	for _, sync := range []string{"off", "normal", "full"} {
		sync := sync
		b.Run("file/sync="+sync, func(b *testing.B) {
			// File DB with a small read-connection pool to exploit WAL
			// concurrent-reader capability.
			f, err := os.CreateTemp(b.TempDir(), "sqlite-conc-*.db")
			if err != nil {
				b.Fatal(err)
			}
			f.Close()

			db, err := sql.Open("sqlite3", f.Name())
			if err != nil {
				b.Fatal(err)
			}
			// Allow up to 4 concurrent reader connections.
			db.SetMaxOpenConns(4)
			b.Cleanup(func() { db.Close() })

			pragmas := []string{
				"PRAGMA journal_mode=WAL",
				"PRAGMA temp_store=MEMORY",
				"PRAGMA cache_size=-64000",
				fmt.Sprintf("PRAGMA synchronous=%s", sync),
				"PRAGMA busy_timeout=5000",
			}
			for _, p := range pragmas {
				if _, err := db.Exec(p); err != nil {
					b.Fatalf("pragma %q: %v", p, err)
				}
			}
			if _, err := db.Exec(`
				CREATE TABLE IF NOT EXISTS kv (
					key   TEXT PRIMARY KEY,
					value TEXT NOT NULL
				)`); err != nil {
				b.Fatal(err)
			}
			seedFileDB(b, db, seedRows)

			b.SetParallelism(4)
			b.RunParallel(func(pb *testing.PB) {
				var val string
				i := 0
				for pb.Next() {
					if err := db.QueryRow(
						`SELECT value FROM kv WHERE key = ?`,
						fmt.Sprintf("seed-key-%d", i%seedRows),
					).Scan(&val); err != nil {
						b.Error(err)
					}
					i++
				}
			})
		})
	}
}
