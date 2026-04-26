package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/voicetel/memdb"
)

func init() {
	// Silence the noisy "memdb: flushed to backend" INFO lines emitted
	// by every fixture build. They clobber the benchmark output table.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// fixturePath builds (once per test binary run) a wrapped snapshot
// file populated with `rows` rows in a "users" table. The returned
// path is ready to feed into openSnapshot.
//
// The snapshot is produced by spinning up an actual *memdb.DB,
// inserting via Exec, then Flush — so the on-disk file matches what
// memdb-cli would see in production (full MDBK envelope, real SQLite
// payload).
func fixturePath(tb testing.TB, rows int) string {
	tb.Helper()

	dir := tb.TempDir()
	path := filepath.Join(dir, fmt.Sprintf("fixture-%d.db", rows))

	db, err := memdb.Open(memdb.Config{
		FilePath:      path,
		FlushInterval: -1, // manual flush only
	})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		created INTEGER,
		score REAL
	)`); err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec(
			"INSERT INTO users(name, email, created, score) VALUES(?, ?, ?, ?)",
			fmt.Sprintf("user-%07d", i),
			fmt.Sprintf("user-%07d@example.test", i),
			time.Now().UnixNano(),
			float64(i)*0.5,
		); err != nil {
			tb.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		tb.Fatal(err)
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	return path
}

// BenchmarkOpenSnapshot measures end-to-end startup cost: unwrap the
// MDBK envelope (verifyAndStreamPayload + temp-file write) then open
// the unwrapped payload via the sqlite3 driver. Dominates CLI
// latency for `-c` one-shot usage.
func BenchmarkOpenSnapshot(b *testing.B) {
	path := fixturePath(b, 10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db, cleanup, err := openSnapshot(path)
		if err != nil {
			b.Fatal(err)
		}
		db.Close()
		cleanup()
	}
}

// BenchmarkRunStatement_Narrow runs a small SELECT through the
// printer in column mode — the typical interactive path where a user
// inspects a few rows.
func BenchmarkRunStatement_Narrow(b *testing.B) {
	benchRunStatement(b, 10000, "SELECT id, name FROM users LIMIT 50", "column")
}

// BenchmarkRunStatement_Wide pulls every column of every row through
// the printer in column mode — the worst case for the buffer-then-
// size approach (column mode reads all rows into memory before
// printing so widths can be sized to the widest cell).
func BenchmarkRunStatement_Wide(b *testing.B) {
	benchRunStatement(b, 10000, "SELECT * FROM users", "column")
}

// BenchmarkRunStatement_List streams rows in list mode (no
// buffering) — measures the cost of the streaming path versus the
// column path.
func BenchmarkRunStatement_List(b *testing.B) {
	benchRunStatement(b, 10000, "SELECT * FROM users", "list")
}

// BenchmarkRunStatement_CSV is identical to _List but encodes
// through encoding/csv — measures the CSV writer overhead.
func BenchmarkRunStatement_CSV(b *testing.B) {
	benchRunStatement(b, 10000, "SELECT * FROM users", "csv")
}

func benchRunStatement(b *testing.B, rows int, query, mode string) {
	b.Helper()
	path := fixturePath(b, rows)
	db, cleanup, err := openSnapshot(path)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	defer db.Close()

	out := newPrinter(io.Discard, true, mode, "|")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := runStatement(db, query, out); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUnwrapOnly isolates the MDBK unwrap from the sqlite3
// open — useful for telling whether snapshot-open cost is dominated
// by the integrity check (SHA-256 + io.Copy) or by SQLite open.
func BenchmarkUnwrapOnly(b *testing.B) {
	path := fixturePath(b, 10000)
	in, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer in.Close()
	stat, _ := in.Stat()
	b.SetBytes(stat.Size())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := in.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		if _, err := memdb.UnwrapSnapshot(in, io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

// Compile-time assertion that the printer actually consumes a *sql.Rows
// — guards against signature drift if printRows is refactored.
var _ = func(p *printer, cols []string, rows *sql.Rows) { p.printRows(cols, rows) }
