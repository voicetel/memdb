package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3" // needed for sql.Open("sqlite3", ...) below
	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/profiling"
	"github.com/voicetel/memdb/server"
)

// This file captures CPU, heap, mutex, and block profiles of the
// PostgreSQL wire-protocol server under a sustained load of concurrent
// clients. The server handler has never been profiled — allocating per
// row in handleSelect / sendDataRow is the primary suspicion, but these
// tests will surface whatever the hottest paths actually are.
//
// Run with:
//
//	MEMDB_PPROF=1 MEMDB_PPROF_DIR=/tmp/memdb-srv-prof \
//	    go test -run TestPProf_Server_ -v ./server/...
//
// Analyse with:
//
//	go tool pprof -http=: /tmp/memdb-srv-prof/pprof_server_select.cpu.prof
//	go tool pprof -http=: /tmp/memdb-srv-prof/pprof_server_select.heap.prof
//	go tool pprof -http=: /tmp/memdb-srv-prof/pprof_server_insert.cpu.prof
//	go tool pprof -http=: /tmp/memdb-srv-prof/pprof_server_mixed.mutex.prof
//	go tool pprof -http=: /tmp/memdb-srv-prof/pprof_server_mixed.block.prof
//
// The tests are gated behind MEMDB_PPROF=1 so `go test ./server/...` runs
// fast in CI and never writes profile artefacts to disk.
//
// Scenarios:
//
//	TestPProf_Server_Select        — concurrent SELECT queries, 10-row
//	                                 result sets (measures per-row
//	                                 encoding on the hot wire path).
//	TestPProf_Server_Select_Wide   — concurrent SELECT returning 500
//	                                 rows per query (amplifies per-row
//	                                 allocations).
//	TestPProf_Server_Insert        — concurrent INSERT DML statements
//	                                 through the simple-query protocol.
//	TestPProf_Server_Mixed         — mixed SELECT/INSERT with mutex and
//	                                 block sampling enabled so any
//	                                 contention inside database/sql or
//	                                 the replica pool shows up.
//	TestPProf_Server_Connect       — rapid connect/disconnect cycle
//	                                 (measures startup handler cost).

// ── helpers ──────────────────────────────────────────────────────────────

// srvPprofOutputDir returns the directory where profiles should be written,
// or skips the test when pprof capture is not enabled for this run. Mirrors
// the gating used by the root-package TestPProf_ suite.
func srvPprofOutputDir(t *testing.T) string {
	t.Helper()
	if os.Getenv("MEMDB_PPROF") == "" {
		t.Skip("set MEMDB_PPROF=1 to enable server pprof capture tests")
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

// silentLoggerSrv returns a slog.Logger that discards everything at INFO and
// below. Keeps the test output clean during a pprof-capture run which would
// otherwise print a flush/restore line per test.
func silentLoggerSrv() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError + 1,
	}))
}

// srvPprofDB opens a memdb suitable for server pprof runs: fresh file,
// kv schema, background flush disabled so the profile reflects only the
// server/handler workload we are measuring. ReadPoolSize is set to
// GOMAXPROCS so SELECT traffic exercises the read replica pool —
// without this, every read serialises through the single writer
// connection (MaxOpenConns=1) and pprof's block profile shows readers
// queued behind one another rather than the realistic parallel-read
// behaviour of a tuned deployment.
func srvPprofDB(t *testing.T) *memdb.DB {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "memdb-srv-pprof-*.db")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	_ = os.Remove(f.Name())

	cfg := memdb.Config{
		FilePath:               f.Name(),
		FlushInterval:          -1,
		ReadPoolSize:           runtime.GOMAXPROCS(0),
		ReplicaRefreshInterval: 50 * time.Millisecond, // package default; named explicitly so changes here don't drift
		Logger:                 silentLoggerSrv(),
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
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// srvPickAddr returns a free loopback TCP address. The listener is closed
// before returning so the server can bind the same port. Small races with
// other parallel tests are possible but extremely unlikely — the kernel
// does not immediately reuse ports released this way.
func srvPickAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// startPprofServer launches a server on addr against db and waits for it
// to be accepting connections. Registered cleanup Stop()s the server at
// end of test — including a short grace period so the pprof callback
// returns before the listener goes away.
//
// Named distinctly from any helper in server_test.go to avoid a
// redeclaration error in the shared server_test package.
func startPprofServer(t *testing.T, db *memdb.DB, addr string) *server.Server {
	t.Helper()
	srv := server.New(db, server.Config{ListenAddr: addr})
	servErr := make(chan error, 1)
	go func() {
		servErr <- srv.ListenAndServe()
	}()

	// Wait until the listener is actually accepting. net.Listen returns
	// before the goroutine above has called Accept, so a connect attempt
	// can race and get ECONNREFUSED.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		srv.Stop()
		select {
		case err := <-servErr:
			if err != nil {
				t.Logf("server.ListenAndServe returned: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Logf("server.ListenAndServe did not return within 2s of Stop")
		}
	})
	return srv
}

// openClient opens a pq-compatible database/sql connection to a running
// memdb server. mattn/go-sqlite3 is not the right driver here — we use
// the lib/pq-style DSN via the pgx or lib/pq driver when available, but
// memdb's wire protocol is simple enough that the mattn/go-sqlite3 driver
// cannot connect to it. Instead we speak the wire protocol directly via
// a tiny in-process helper so the test has no external driver dependency.
//
// srvClient below is that helper.

// srvClient is a minimal PostgreSQL wire-protocol client sufficient to
// drive the handler for pprof workloads: startup, simple query, parse
// RowDescription / DataRow / CommandComplete / ReadyForQuery / Error.
// It is NOT a general-purpose Postgres driver — it elides extended
// protocol, parameter types, and result-row typing because the memdb
// handler itself does.
type srvClient struct {
	conn net.Conn
	buf  []byte // reusable read buffer so steady-state allocations stay low
}

func dialSrv(addr string) (*srvClient, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	sc := &srvClient{conn: c, buf: make([]byte, 0, 4096)}

	// Startup message: length (including self) + protocol 3.0 + "user\0memdb\0\0".
	params := []byte("user\x00memdb\x00\x00")
	length := 8 + len(params)
	pkt := make([]byte, length)
	pkt[0] = byte(length >> 24)
	pkt[1] = byte(length >> 16)
	pkt[2] = byte(length >> 8)
	pkt[3] = byte(length)
	// Protocol major 3, minor 0 → 0x00030000.
	pkt[4], pkt[5], pkt[6], pkt[7] = 0, 3, 0, 0
	copy(pkt[8:], params)
	if _, err := c.Write(pkt); err != nil {
		_ = c.Close()
		return nil, err
	}

	// Consume messages until ReadyForQuery. memdb's handler sends
	// AuthenticationOk (R) then ReadyForQuery (Z) when Auth is nil.
	for {
		msgType, _, err := sc.readMsg()
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		if msgType == 'Z' { // ReadyForQuery
			return sc, nil
		}
		if msgType == 'E' { // Error
			_ = c.Close()
			return nil, fmt.Errorf("server rejected startup")
		}
	}
}

// readMsg reads one message into sc.buf and returns (type, body, err).
// The returned body aliases sc.buf and is only valid until the next call.
// Callers that retain any slice of body must copy.
func (sc *srvClient) readMsg() (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(sc.conn, header[:]); err != nil {
		return 0, nil, err
	}
	length := int(header[1])<<24 | int(header[2])<<16 | int(header[3])<<8 | int(header[4])
	if length < 4 || length > 16*1024*1024 {
		return 0, nil, fmt.Errorf("bad message length %d", length)
	}
	bodyLen := length - 4
	if cap(sc.buf) < bodyLen {
		sc.buf = make([]byte, bodyLen)
	} else {
		sc.buf = sc.buf[:bodyLen]
	}
	if _, err := io.ReadFull(sc.conn, sc.buf); err != nil {
		return 0, nil, err
	}
	return header[0], sc.buf, nil
}

// simpleQuery sends a Q message with the given SQL and consumes the
// response messages until ReadyForQuery. Returns the number of DataRow
// messages received (zero for non-SELECT). An Error message aborts the
// sequence with that error.
func (sc *srvClient) simpleQuery(query string) (int, error) {
	size := 1 + 4 + len(query) + 1
	pkt := make([]byte, size)
	pkt[0] = 'Q'
	pkt[1] = byte((size - 1) >> 24)
	pkt[2] = byte((size - 1) >> 16)
	pkt[3] = byte((size - 1) >> 8)
	pkt[4] = byte(size - 1)
	copy(pkt[5:], query)
	pkt[size-1] = 0
	if _, err := sc.conn.Write(pkt); err != nil {
		return 0, err
	}

	rows := 0
	var queryErr error
	for {
		msgType, body, err := sc.readMsg()
		if err != nil {
			return rows, err
		}
		switch msgType {
		case 'D': // DataRow
			rows++
		case 'T', 'C', 'I': // RowDescription, CommandComplete, EmptyQuery
			// no-op
		case 'E': // Error — remember and keep reading until ReadyForQuery
			queryErr = fmt.Errorf("server error: %q", body)
		case 'Z': // ReadyForQuery
			return rows, queryErr
		default:
			// Unknown message type — ignore.
		}
	}
}

func (sc *srvClient) close() { _ = sc.conn.Close() }

// seedRows writes n rows through the writer connection (not through the
// server) so the SELECT scenarios have deterministic data to read. Using
// db.Exec directly here is intentional — we do not want the pprof
// profile for the SELECT scenario to include the seeding INSERT cost.
func seedRows(t *testing.T, db *memdb.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := db.Exec(
			`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
			fmt.Sprintf("seed-%06d", i),
			fmt.Sprintf("value-%06d", i),
		); err != nil {
			t.Fatal(err)
		}
	}
}

// runClients launches `clients` goroutines, each running fn in a tight
// loop until the workDuration elapses. Returns the total number of
// completed iterations across all clients and the wall time the
// workload ran for.
func runClients(
	t *testing.T,
	addr string,
	clients int,
	workDuration time.Duration,
	fn func(sc *srvClient, iter int) error,
) (int64, time.Duration) {
	t.Helper()
	var wg sync.WaitGroup
	var iters atomic.Int64
	var errCount atomic.Int64
	start := time.Now()
	done := make(chan struct{})
	for g := 0; g < clients; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sc, err := dialSrv(addr)
			if err != nil {
				errCount.Add(1)
				return
			}
			defer sc.close()
			i := 0
			for {
				select {
				case <-done:
					return
				default:
				}
				if err := fn(sc, i); err != nil {
					errCount.Add(1)
					return
				}
				i++
				iters.Add(1)
			}
		}()
	}
	time.Sleep(workDuration)
	close(done)
	wg.Wait()
	if n := errCount.Load(); n > 0 {
		t.Logf("warning: %d client goroutines exited with an error", n)
	}
	return iters.Load(), time.Since(start)
}

// ── TestPProf_Server_Select ─────────────────────────────────────────────

// TestPProf_Server_Select captures CPU and heap profiles of a concurrent
// SELECT workload returning ~10 rows per query. This is the "narrow row"
// baseline — it isolates the per-row encoding overhead from wider
// scenarios below.
func TestPProf_Server_Select(t *testing.T) {
	dir := srvPprofOutputDir(t)

	db := srvPprofDB(t)
	const seed = 1000
	seedRows(t, db, seed)

	addr := srvPickAddr(t)
	startPprofServer(t, db, addr)

	cpuPath := filepath.Join(dir, "pprof_server_select.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_server_select.heap.prof")

	const (
		clients      = 16
		workDuration = 3 * time.Second
	)

	var iters int64
	var elapsed time.Duration
	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		iters, elapsed = runClients(t, addr, clients, workDuration,
			func(sc *srvClient, iter int) error {
				// LIMIT 10 — narrow result so per-row cost dominates per-query.
				q := fmt.Sprintf(
					`SELECT key, value FROM kv WHERE key >= 'seed-%06d' ORDER BY key LIMIT 10`,
					iter%(seed-10))
				_, err := sc.simpleQuery(q)
				return err
			})
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	t.Logf("select narrow: %d queries across %d clients in %s (%.0f q/s)  cpu=%s  heap=%s",
		iters, clients, elapsed, float64(iters)/elapsed.Seconds(), cpuPath, heapPath)
}

// ── TestPProf_Server_Select_Wide ───────────────────────────────────────

// TestPProf_Server_Select_Wide captures CPU and heap profiles of a
// concurrent SELECT workload returning 500 rows per query. This amplifies
// per-row and per-cell allocation costs (handleSelect allocates a
// [][]byte per row and appendCell allocates a []byte per non-null cell),
// so the pprof output will point directly at any regression or
// optimisation opportunity in the row-streaming path.
func TestPProf_Server_Select_Wide(t *testing.T) {
	dir := srvPprofOutputDir(t)

	db := srvPprofDB(t)
	const seed = 2000
	seedRows(t, db, seed)

	addr := srvPickAddr(t)
	startPprofServer(t, db, addr)

	cpuPath := filepath.Join(dir, "pprof_server_select_wide.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_server_select_wide.heap.prof")

	const (
		clients      = 8
		workDuration = 3 * time.Second
		limit        = 500
	)

	var iters int64
	var elapsed time.Duration
	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		iters, elapsed = runClients(t, addr, clients, workDuration,
			func(sc *srvClient, iter int) error {
				q := fmt.Sprintf(
					`SELECT key, value FROM kv WHERE key >= 'seed-%06d' ORDER BY key LIMIT %d`,
					iter%(seed-limit), limit)
				_, err := sc.simpleQuery(q)
				return err
			})
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	t.Logf("select wide: %d queries across %d clients in %s (%.0f q/s, ~%d rows each)  cpu=%s  heap=%s",
		iters, clients, elapsed, float64(iters)/elapsed.Seconds(), limit, cpuPath, heapPath)
}

// ── TestPProf_Server_Insert ────────────────────────────────────────────

// TestPProf_Server_Insert captures profiles of concurrent INSERT DML
// through the simple-query protocol. This exercises the sendCommandComplete
// path and the Exec-returning-RowsAffected path, both of which were never
// profiled before.
func TestPProf_Server_Insert(t *testing.T) {
	dir := srvPprofOutputDir(t)

	db := srvPprofDB(t)
	addr := srvPickAddr(t)
	startPprofServer(t, db, addr)

	cpuPath := filepath.Join(dir, "pprof_server_insert.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_server_insert.heap.prof")

	const (
		clients      = 8
		workDuration = 3 * time.Second
	)

	var iters int64
	var elapsed time.Duration
	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		iters, elapsed = runClients(t, addr, clients, workDuration,
			func(sc *srvClient, iter int) error {
				q := fmt.Sprintf(
					`INSERT OR REPLACE INTO kv (key, value) VALUES ('ins-%d-%d', 'v')`,
					iter, time.Now().UnixNano())
				_, err := sc.simpleQuery(q)
				return err
			})
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	t.Logf("insert: %d queries across %d clients in %s (%.0f q/s)  cpu=%s  heap=%s",
		iters, clients, elapsed, float64(iters)/elapsed.Seconds(), cpuPath, heapPath)
}

// ── TestPProf_Server_Mixed ─────────────────────────────────────────────

// TestPProf_Server_Mixed captures CPU, mutex, and block profiles of a
// mixed SELECT/INSERT workload. Mutex and block sampling are enabled for
// the duration of the run so any contention inside database/sql, the
// replica pool, or the server's own buffered writer becomes visible in
// the profile.
func TestPProf_Server_Mixed(t *testing.T) {
	dir := srvPprofOutputDir(t)

	profiling.EnableMutexProfiling(1)
	profiling.EnableBlockProfiling(1)
	defer profiling.EnableMutexProfiling(0)
	defer profiling.EnableBlockProfiling(0)

	db := srvPprofDB(t)
	seedRows(t, db, 500)

	addr := srvPickAddr(t)
	startPprofServer(t, db, addr)

	cpuPath := filepath.Join(dir, "pprof_server_mixed.cpu.prof")
	mutexPath := filepath.Join(dir, "pprof_server_mixed.mutex.prof")
	blockPath := filepath.Join(dir, "pprof_server_mixed.block.prof")

	clients := runtime.GOMAXPROCS(0)
	const workDuration = 3 * time.Second

	var iters int64
	var elapsed time.Duration
	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		iters, elapsed = runClients(t, addr, clients, workDuration,
			func(sc *srvClient, iter int) error {
				// 1-in-4 writes: a realistic OLTP ratio.
				if iter%4 == 0 {
					q := fmt.Sprintf(
						`INSERT OR REPLACE INTO kv (key, value) VALUES ('m-%d-%d', 'v')`,
						iter, time.Now().UnixNano())
					_, err := sc.simpleQuery(q)
					return err
				}
				q := fmt.Sprintf(
					`SELECT value FROM kv WHERE key = 'seed-%06d'`, iter%500)
				_, err := sc.simpleQuery(q)
				return err
			})
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}

	// Snapshot mutex/block profiles after the workload — samples accumulate
	// into the runtime's profiles, not into a per-run handle.
	if err := profiling.CaptureNamedProfile("mutex", mutexPath); err != nil {
		t.Fatalf("capture mutex: %v", err)
	}
	if err := profiling.CaptureNamedProfile("block", blockPath); err != nil {
		t.Fatalf("capture block: %v", err)
	}

	t.Logf("mixed: %d queries across %d clients in %s (%.0f q/s)  cpu=%s  mutex=%s  block=%s",
		iters, clients, elapsed, float64(iters)/elapsed.Seconds(),
		cpuPath, mutexPath, blockPath)
}

// ── TestPProf_Server_Connect ───────────────────────────────────────────

// TestPProf_Server_Connect captures profiles of a rapid connect →
// one query → disconnect cycle. This is the profile an operator wants
// when investigating short-lived-connection overhead (for example, a
// Lambda-style workload that never reuses connections).
func TestPProf_Server_Connect(t *testing.T) {
	dir := srvPprofOutputDir(t)

	db := srvPprofDB(t)
	addr := srvPickAddr(t)
	startPprofServer(t, db, addr)

	cpuPath := filepath.Join(dir, "pprof_server_connect.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_server_connect.heap.prof")

	const (
		clients      = 8
		workDuration = 3 * time.Second
	)

	var wg sync.WaitGroup
	var iters atomic.Int64
	start := time.Now()
	done := make(chan struct{})

	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		for g := 0; g < clients; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					sc, err := dialSrv(addr)
					if err != nil {
						return
					}
					_, _ = sc.simpleQuery(`SELECT 1`)
					sc.close()
					iters.Add(1)
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

	n := iters.Load()
	t.Logf("connect: %d connect/query/disconnect cycles across %d clients in %s (%.0f cyc/s)  cpu=%s  heap=%s",
		n, clients, time.Since(start), float64(n)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}

// ── TestPProf_Server_Connect_TLS ───────────────────────────────────────

// TestPProf_Server_Connect_TLS is the TLS-wrapped twin of
// TestPProf_Server_Connect. Each iteration is a TLS handshake plus one
// SELECT 1 plus a clean disconnect, so the captured CPU profile is
// dominated by ECDSA / AES-GCM handshake cost — exactly the fixed
// per-connection price of running the server with TLS enabled. Compare
// the two profiles side-by-side to size the gap a connection pool buys.
func TestPProf_Server_Connect_TLS(t *testing.T) {
	dir := srvPprofOutputDir(t)

	tlsCfg, clientCfg := genServerTLS(t)

	db := srvPprofDB(t)
	addr := srvPickAddr(t)
	startPprofServerTLS(t, db, addr, tlsCfg)

	cpuPath := filepath.Join(dir, "pprof_server_connect_tls.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_server_connect_tls.heap.prof")

	const (
		clients      = 8
		workDuration = 3 * time.Second
	)

	var wg sync.WaitGroup
	var iters atomic.Int64
	start := time.Now()
	done := make(chan struct{})

	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		for g := 0; g < clients; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					sc, err := dialSrvTLS(addr, clientCfg)
					if err != nil {
						return
					}
					_, _ = sc.simpleQuery(`SELECT 1`)
					sc.close()
					iters.Add(1)
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

	n := iters.Load()
	t.Logf("connect-tls: %d TLS connect/query/disconnect cycles across %d clients in %s (%.0f cyc/s)  cpu=%s  heap=%s",
		n, clients, time.Since(start), float64(n)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}

// ── TestPProf_Server_Allocs ────────────────────────────────────────────

// TestPProf_Server_Allocs captures the runtime "allocs" profile (every
// allocation since process start) at two points around a SELECT workload
// so the operator can diff them with `go tool pprof -base before after`
// to see exactly which allocations the wire-protocol serving path made.
// The CPU profile alone tells you where time is spent; the diff tells
// you where the GC pressure comes from — usually appendCell, sendDataRow,
// and the per-row [][]byte slice in handleSelect.
func TestPProf_Server_Allocs(t *testing.T) {
	dir := srvPprofOutputDir(t)

	db := srvPprofDB(t)
	const seed = 1000
	seedRows(t, db, seed)

	addr := srvPickAddr(t)
	startPprofServer(t, db, addr)

	beforePath := filepath.Join(dir, "pprof_server_allocs.before.prof")
	afterPath := filepath.Join(dir, "pprof_server_allocs.after.prof")

	// Snapshot allocations BEFORE the workload starts. runtime.GC ensures
	// any pending finalizers have run so the "before" baseline is stable.
	runtime.GC()
	if err := profiling.CaptureNamedProfile("allocs", beforePath); err != nil {
		t.Fatalf("capture before: %v", err)
	}

	const (
		clients      = 8
		workDuration = 3 * time.Second
	)
	iters, elapsed := runClients(t, addr, clients, workDuration,
		func(sc *srvClient, iter int) error {
			q := fmt.Sprintf(
				`SELECT key, value FROM kv WHERE key >= 'seed-%06d' ORDER BY key LIMIT 50`,
				iter%(seed-50))
			_, err := sc.simpleQuery(q)
			return err
		})

	runtime.GC()
	if err := profiling.CaptureNamedProfile("allocs", afterPath); err != nil {
		t.Fatalf("capture after: %v", err)
	}

	t.Logf("allocs: %d queries across %d clients in %s (%.0f q/s)\n"+
		"  diff with: go tool pprof -base %s %s",
		iters, clients, elapsed, float64(iters)/elapsed.Seconds(),
		beforePath, afterPath)
}

// ── TestPProf_Server_Extended_Select ───────────────────────────────────

// TestPProf_Server_Extended_Select captures CPU and heap profiles of the
// extended-query path (Parse / Bind / Describe / Execute / Sync) on a
// parameterised SELECT. This is the path pgx and lib/pq use for any
// query with arguments — a separate scenario from the simple-query
// profile because the per-query work is meaningfully different (more
// messages on the wire, server-side prepared-statement bookkeeping,
// per-cell format-code switching for binary results).
func TestPProf_Server_Extended_Select(t *testing.T) {
	dir := srvPprofOutputDir(t)

	db := srvPprofDB(t)
	const seed = 1000
	seedRows(t, db, seed)

	addr := srvPickAddr(t)
	startPprofServer(t, db, addr)

	cpuPath := filepath.Join(dir, "pprof_server_extended_select.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_server_extended_select.heap.prof")

	const (
		clients      = 16
		workDuration = 3 * time.Second
	)

	var iters int64
	var elapsed time.Duration
	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		iters, elapsed = runClients(t, addr, clients, workDuration,
			func(sc *srvClient, iter int) error {
				key := fmt.Sprintf("seed-%06d", iter%seed)
				return sc.extendedQuery(`SELECT key, value FROM kv WHERE key = $1`, key)
			})
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}
	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	t.Logf("extended select: %d queries across %d clients in %s (%.0f q/s)  cpu=%s  heap=%s",
		iters, clients, elapsed, float64(iters)/elapsed.Seconds(), cpuPath, heapPath)
}

// extendedQuery runs one Parse/Bind/Describe/Execute/Sync cycle against
// the unnamed prepared statement and portal, supplying a single
// text-format string parameter. Returns nil on AuthOk + ReadyForQuery,
// or an error wrapping the server's ErrorResponse text.
func (sc *srvClient) extendedQuery(query string, arg string) error {
	// Parse: name="" + query\0 + nParams=0 (server infers $N count)
	parseBody := []byte{0}
	parseBody = append(parseBody, query...)
	parseBody = append(parseBody, 0, 0, 0)
	if err := sc.send('P', parseBody); err != nil {
		return err
	}

	// Bind: portal="" stmt="" nFmt=0 nParams=1 [len=N value] nResultFormats=0
	bindBody := []byte{0, 0, 0, 0, 0, 1}
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(arg)))
	bindBody = append(bindBody, lb[:]...)
	bindBody = append(bindBody, arg...)
	bindBody = append(bindBody, 0, 0)
	if err := sc.send('B', bindBody); err != nil {
		return err
	}

	// Describe portal "" — exercises the first-row-peek path.
	if err := sc.send('D', []byte{'P', 0}); err != nil {
		return err
	}

	// Execute portal "" with no row limit.
	if err := sc.send('E', []byte{0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}

	// Sync.
	if err := sc.send('S', nil); err != nil {
		return err
	}

	for {
		mt, body, err := sc.readMsg()
		if err != nil {
			return err
		}
		switch mt {
		case '1', '2', 't', 'T', 'D', 'C', 'I', 'n':
			// Parse/Bind/Describe/Data/Complete responses — drain.
		case 'E':
			return fmt.Errorf("server error: %q", body)
		case 'Z':
			return nil
		}
	}
}

func (sc *srvClient) send(msgType byte, body []byte) error {
	totalLen := uint32(4 + len(body))
	pkt := make([]byte, 0, 1+totalLen)
	pkt = append(pkt, msgType)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], totalLen)
	pkt = append(pkt, lb[:]...)
	pkt = append(pkt, body...)
	_, err := sc.conn.Write(pkt)
	return err
}

// ── TLS helpers ────────────────────────────────────────────────────────

// genServerTLS produces a self-signed cert/key valid for 127.0.0.1 and
// returns (server *tls.Config, client *tls.Config). The server config
// presents the cert; the client config trusts it via RootCAs. ECDSA
// P-256 is used so the handshake cost in pprof reflects the most common
// production setup (ECDHE_ECDSA suites).
func genServerTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "memdb-pprof"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)

	srvCfg := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}
	cliCfg := &tls.Config{
		RootCAs:    pool,
		ServerName: "127.0.0.1",
	}
	return srvCfg, cliCfg
}

// startPprofServerTLS is startPprofServer with TLSConfig set on the
// memdb server.Config so the listener wraps incoming TCP connections in
// TLS. Mirrors startPprofServer's connect-readiness wait so the test
// does not race the goroutine that calls ListenAndServe.
func startPprofServerTLS(t *testing.T, db *memdb.DB, addr string, tlsCfg *tls.Config) *server.Server {
	t.Helper()
	srv := server.New(db, server.Config{ListenAddr: addr, TLSConfig: tlsCfg})
	servErr := make(chan error, 1)
	go func() { servErr <- srv.ListenAndServe() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		srv.Stop()
		select {
		case err := <-servErr:
			if err != nil {
				t.Logf("server.ListenAndServe returned: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Logf("server.ListenAndServe did not return within 2s of Stop")
		}
	})
	return srv
}

// dialSrvTLS is dialSrv over TLS. The server listener is already TLS, so
// no Postgres SSLRequest is sent — the Postgres startup packet goes
// straight onto the TLS connection.
func dialSrvTLS(addr string, cliCfg *tls.Config) (*srvClient, error) {
	d := &net.Dialer{Timeout: 2 * time.Second}
	c, err := tls.DialWithDialer(d, "tcp", addr, cliCfg)
	if err != nil {
		return nil, err
	}
	sc := &srvClient{conn: c, buf: make([]byte, 0, 4096)}

	params := []byte("user\x00memdb\x00\x00")
	length := 8 + len(params)
	pkt := make([]byte, length)
	pkt[0] = byte(length >> 24)
	pkt[1] = byte(length >> 16)
	pkt[2] = byte(length >> 8)
	pkt[3] = byte(length)
	pkt[4], pkt[5], pkt[6], pkt[7] = 0, 3, 0, 0
	copy(pkt[8:], params)
	if _, err := c.Write(pkt); err != nil {
		_ = c.Close()
		return nil, err
	}
	for {
		msgType, _, err := sc.readMsg()
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		if msgType == 'Z' {
			return sc, nil
		}
		if msgType == 'E' {
			_ = c.Close()
			return nil, fmt.Errorf("server rejected startup")
		}
	}
}

// ── compile-time anchors ───────────────────────────────────────────────

// These unused references pin imports that would otherwise be pruned when
// a subset of the tests is commented out during ad-hoc profiling runs.
// They exist purely to stabilise the file under local edits; they have
// no runtime effect and compile to nothing in a normal build.
var (
	_ = context.Background
	_ = sql.ErrNoRows
)
