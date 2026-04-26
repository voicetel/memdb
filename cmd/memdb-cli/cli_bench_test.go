package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/server"
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

// wireFixture starts an in-process memdb server populated with
// `rows` rows in a "users" table and returns a *sql.DB connected to
// it via the lib/pq driver — exactly the same path memdb-cli's
// wire mode uses, so benches measure the real client cost.
//
// Pre-binds 127.0.0.1:0 to grab a free port, closes the listener,
// then hands the addr to the server. Tiny race (port could be
// stolen between Close and re-Listen) but tolerable for benches.
//
// Returns a cleanup that tears down both the client and the
// in-process server.
func wireFixture(tb testing.TB, rows int) (*sql.DB, func()) {
	tb.Helper()

	dir := tb.TempDir()
	snapPath := filepath.Join(dir, "snap.db")
	mdb, err := memdb.Open(memdb.Config{
		FilePath:      snapPath,
		FlushInterval: -1,
	})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := mdb.Exec(`CREATE TABLE users(
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		created INTEGER,
		score REAL
	)`); err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if _, err := mdb.Exec(
			"INSERT INTO users(name,email,created,score) VALUES(?,?,?,?)",
			fmt.Sprintf("user-%07d", i),
			fmt.Sprintf("user-%07d@example.test", i),
			time.Now().UnixNano(),
			float64(i)*0.5,
		); err != nil {
			tb.Fatal(err)
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := server.New(mdb, server.Config{ListenAddr: addr})
	go srv.ListenAndServe() //nolint:errcheck // server returns nil on Stop

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=%s port=%s sslmode=disable connect_timeout=5",
			strings.Split(addr, ":")[0], strings.Split(addr, ":")[1]))
	if err != nil {
		srv.Stop()
		mdb.Close()
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)

	// Wait for the server to start accepting connections.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		srv.Stop()
		mdb.Close()
		tb.Fatalf("server never accepted connections: %v", err)
	}

	cleanup := func() {
		db.Close()
		srv.Stop()
		mdb.Close()
	}
	return db, cleanup
}

// BenchmarkWire_Query_Narrow is the wire-mode counterpart of
// RunStatement_Narrow — small SELECT through the PG protocol.
// Lets us see the per-query overhead the wire layer adds vs the
// direct in-process snapshot path.
func BenchmarkWire_Query_Narrow(b *testing.B) {
	benchWireQuery(b, 10000, "SELECT id, name FROM users LIMIT 50", "column")
}

// BenchmarkWire_Query_Wide pulls every column of every row through
// the wire — measures the cost of marshalling a substantial result
// set across the protocol vs the same workload on a direct conn.
func BenchmarkWire_Query_Wide(b *testing.B) {
	benchWireQuery(b, 10000, "SELECT * FROM users", "column")
}

// BenchmarkWire_Insert measures write-path latency over the wire —
// the workload that's only available in wire mode.
func BenchmarkWire_Insert(b *testing.B) {
	db, cleanup := wireFixture(b, 0)
	defer cleanup()
	if _, err := db.Exec("CREATE TABLE bench(id INTEGER PRIMARY KEY, payload TEXT)"); err != nil {
		b.Fatal(err)
	}
	out := newPrinter(io.Discard, true, "column", "|")
	stmt := "INSERT INTO bench(payload) VALUES('the quick brown fox jumps over the lazy dog')"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := runStatement(db, stmt, out); err != nil {
			b.Fatal(err)
		}
	}
}

func benchWireQuery(b *testing.B, rows int, query, mode string) {
	b.Helper()
	db, cleanup := wireFixture(b, rows)
	defer cleanup()
	out := newPrinter(io.Discard, true, mode, "|")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := runStatement(db, query, out); err != nil {
			b.Fatal(err)
		}
	}
}
