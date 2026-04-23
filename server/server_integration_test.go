package server_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"log/slog"
	"math/big"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/server"
)

// ── TLS helpers ───────────────────────────────────────────────────────────────

// generateServerTLS creates a self-signed certificate pair for testing.
// Returns a server-side tls.Config and a client-side tls.Config that trusts
// the self-signed cert.
func generateServerTLS(t *testing.T) (serverCfg, clientCfg *tls.Config) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "memdb-server-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	tlsCert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}

	serverCfg = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	}
	clientCfg = &tls.Config{
		RootCAs:    pool,
		ServerName: "127.0.0.1",
	}
	return serverCfg, clientCfg
}

// ── integration test DB and server helpers ────────────────────────────────────

// integrationDB opens a fresh memdb with the kv schema.
func integrationDB(t *testing.T) *memdb.DB {
	t.Helper()
	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(t.TempDir(), "integ.db"),
		FlushInterval: -1,
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
	})
	if err != nil {
		t.Fatalf("integrationDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// startIntegServer starts a server with the given config on a free port and
// returns the listening address. The server is stopped via t.Cleanup.
func startIntegServer(t *testing.T, db *memdb.DB, cfg server.Config) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg.ListenAddr = addr
	srv := server.New(db, cfg)

	go func() { _ = srv.ListenAndServe() }()

	// Wait until the listener is actually accepting.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Cleanup(func() { srv.Stop() })
	return addr
}

// ── wire protocol helpers ─────────────────────────────────────────────────────

// sslRequestBytes is the 8-byte PostgreSQL SSLRequest packet:
// length=8 followed by the magic number 80877103.
var sslRequestBytes = func() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], 8)
	binary.BigEndian.PutUint32(b[4:8], 80877103)
	return b
}()

// startupPacket builds a minimal PostgreSQL startup packet with the given
// user parameter. Format: length(4) + protocol(4) + "user\0<user>\0\0".
func startupPacket(user string) []byte {
	params := "user\x00" + user + "\x00\x00"
	length := 4 + 4 + len(params)
	b := make([]byte, length)
	binary.BigEndian.PutUint32(b[0:4], uint32(length))
	binary.BigEndian.PutUint32(b[4:8], 196608) // protocol 3.0
	copy(b[8:], params)
	return b
}

// doStartup performs the no-auth startup handshake on conn and returns nil on
// success. It does NOT send an SSLRequest first — use doStartupWithSSL for that.
func doStartup(t *testing.T, conn net.Conn, user string) {
	t.Helper()
	if _, err := conn.Write(startupPacket(user)); err != nil {
		t.Fatalf("doStartup write: %v", err)
	}
	// Expect AuthenticationOk ('R', length=8, int32=0).
	buf := make([]byte, 9)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("doStartup read auth: %v", err)
	}
	if buf[0] != 'R' {
		t.Fatalf("doStartup: expected 'R', got %q (full: %x)", buf[0], buf)
	}
	// Expect ReadyForQuery ('Z', length=5, 'I').
	rdy := make([]byte, 6)
	if _, err := io.ReadFull(conn, rdy); err != nil {
		t.Fatalf("doStartup read rdy: %v", err)
	}
	if rdy[0] != 'Z' {
		t.Fatalf("doStartup: expected 'Z', got %q", rdy[0])
	}
}

// doStartupWithSSL sends an SSLRequest first, reads the server's single-byte
// response ('N' for decline, 'S' for accept), and then sends the startup packet.
// Returns the single-byte SSL response for the caller to inspect.
func doStartupWithSSL(t *testing.T, conn net.Conn, user string) byte {
	t.Helper()
	if _, err := conn.Write(sslRequestBytes); err != nil {
		t.Fatalf("doStartupWithSSL write SSLRequest: %v", err)
	}
	resp := make([]byte, 1)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("doStartupWithSSL read SSL response: %v", err)
	}
	// Now send the real startup message.
	if _, err := conn.Write(startupPacket(user)); err != nil {
		t.Fatalf("doStartupWithSSL write startup: %v", err)
	}
	return resp[0]
}

// readMsg reads one PostgreSQL backend message from conn.
// Returns the message type byte and body (length field excluded).
func readMsg(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("readMsg header: %v", err)
	}
	length := int(binary.BigEndian.Uint32(header[1:5]))
	body := make([]byte, length-4)
	if len(body) > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			t.Fatalf("readMsg body: %v", err)
		}
	}
	return header[0], body
}

// sendSimpleQuery sends a PostgreSQL Simple Query ('Q') message.
func sendSimpleQuery(t *testing.T, conn net.Conn, sql string) {
	t.Helper()
	body := append([]byte(sql), 0)
	length := uint32(len(body) + 4)
	pkt := make([]byte, 1+4+len(body))
	pkt[0] = 'Q'
	binary.BigEndian.PutUint32(pkt[1:5], length)
	copy(pkt[5:], body)
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("sendSimpleQuery: %v", err)
	}
}

// drainToReady reads messages until ReadyForQuery and returns the count of
// DataRow messages received.
func drainToReady(t *testing.T, conn net.Conn) int {
	t.Helper()
	rows := 0
	for {
		msgType, body := readMsg(t, conn)
		switch msgType {
		case 'D':
			rows++
		case 'E':
			t.Errorf("server error: %s", body)
		case 'Z':
			return rows
		}
	}
}

// ── TLS listener tests ────────────────────────────────────────────────────────

// TestServer_TLS_ClientConnects verifies that a client using the correct CA
// can connect over TLS, complete the startup handshake, and execute a query.
// This is the most fundamental TLS test — it exercises tls.Listen, the
// TLS handshake path inside ListenAndServe, and the full query round-trip.
func TestServer_TLS_ClientConnects(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{TLSConfig: serverTLS})

	conn, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	doStartup(t, conn, "memdb")

	sendSimpleQuery(t, conn, "SELECT 1")
	rows := drainToReady(t, conn)
	if rows != 1 {
		t.Errorf("expected 1 DataRow, got %d", rows)
	}
}

// TestServer_TLS_RejectsUntrustedClient verifies that a client without the
// correct CA certificate cannot complete the TLS handshake. The connection
// must be rejected at the TLS layer before any PostgreSQL protocol bytes
// are exchanged.
func TestServer_TLS_RejectsUntrustedClient(t *testing.T) {
	serverTLS, _ := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{TLSConfig: serverTLS})

	// Dial with the system root pool — our self-signed cert is not in it.
	_, err := tls.Dial("tcp", addr, &tls.Config{ServerName: "127.0.0.1"})
	if err == nil {
		t.Fatal("expected TLS handshake to fail for untrusted cert, got nil")
	}
	// The error must be a certificate verification failure, not a
	// network error. Both tls.VerificationError and "unknown authority"
	// strings are acceptable depending on Go version.
	if !strings.Contains(err.Error(), "certificate") &&
		!strings.Contains(err.Error(), "unknown authority") &&
		!strings.Contains(err.Error(), "verification") {
		t.Errorf("unexpected error type (wanted cert error): %v", err)
	}
}

// TestServer_TLS_MultipleClients verifies that multiple clients can connect
// over TLS concurrently and each complete an independent query round-trip.
func TestServer_TLS_MultipleClients(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{TLSConfig: serverTLS})

	const clients = 8
	var wg sync.WaitGroup
	errc := make(chan error, clients)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := tls.Dial("tcp", addr, clientTLS)
			if err != nil {
				errc <- err
				return
			}
			defer conn.Close()

			doStartup(t, conn, "memdb")
			sendSimpleQuery(t, conn, "SELECT 1")
			drainToReady(t, conn)
		}()
	}

	wg.Wait()
	close(errc)
	for err := range errc {
		t.Errorf("TLS client error: %v", err)
	}
}

// TestServer_TLS_INSERT_and_SELECT verifies that a TLS-connected client
// can write data via INSERT and read it back via SELECT — confirming the
// TLS path is a transparent transport overlay on the full handler logic,
// not just the startup handshake.
func TestServer_TLS_INSERT_and_SELECT(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{TLSConfig: serverTLS})

	conn, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	doStartup(t, conn, "memdb")

	sendSimpleQuery(t, conn, `INSERT INTO kv (key, value) VALUES ('tls-key', 'tls-val')`)
	drainToReady(t, conn)

	sendSimpleQuery(t, conn, `SELECT value FROM kv WHERE key = 'tls-key'`)

	var gotRow bool
	for {
		msgType, body := readMsg(t, conn)
		switch msgType {
		case 'T': // RowDescription
		case 'D': // DataRow — verify the value
			gotRow = true
			// DataRow body: [2-byte field count][per-field: 4-byte length + bytes]
			if len(body) < 2 {
				t.Fatalf("DataRow too short: %x", body)
			}
			// fieldCount := binary.BigEndian.Uint16(body[0:2])
			off := 2
			if off+4 > len(body) {
				t.Fatalf("DataRow missing field length")
			}
			flen := int(int32(binary.BigEndian.Uint32(body[off : off+4])))
			off += 4
			if flen < 0 || off+flen > len(body) {
				t.Fatalf("DataRow field length %d out of range", flen)
			}
			val := string(body[off : off+flen])
			if val != "tls-val" {
				t.Errorf("got value %q, want %q", val, "tls-val")
			}
		case 'C': // CommandComplete
		case 'Z': // ReadyForQuery
			goto done
		case 'E':
			t.Fatalf("server error: %s", body)
		}
	}
done:
	if !gotRow {
		t.Error("expected one DataRow, got none")
	}
}

// TestServer_TLS_AuthSuccess verifies that BasicAuth over TLS succeeds when
// the correct credentials are provided.
func TestServer_TLS_AuthSuccess(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{
		TLSConfig: serverTLS,
		Auth:      server.BasicAuth{Username: "alice", Password: "s3cr3t"},
	})

	conn, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	// Send startup.
	if _, err := conn.Write(startupPacket("alice")); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	// Expect AuthenticationCleartextPassword ('R', length=8, int32=3).
	buf := make([]byte, 9)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read auth challenge: %v", err)
	}
	if buf[0] != 'R' {
		t.Fatalf("expected auth challenge 'R', got %q", buf[0])
	}
	// Send password message ('p', length=len+4, password\0).
	pw := append([]byte("s3cr3t"), 0)
	plen := uint32(len(pw) + 4)
	pmsg := make([]byte, 1+4+len(pw))
	pmsg[0] = 'p'
	binary.BigEndian.PutUint32(pmsg[1:5], plen)
	copy(pmsg[5:], pw)
	if _, err := conn.Write(pmsg); err != nil {
		t.Fatalf("write password: %v", err)
	}
	// Expect AuthenticationOk then ReadyForQuery.
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read auth ok: %v", err)
	}
	if buf[0] != 'R' {
		t.Fatalf("expected AuthenticationOk 'R', got %q", buf[0])
	}
	rdy := make([]byte, 6)
	if _, err := io.ReadFull(conn, rdy); err != nil {
		t.Fatalf("read rdy: %v", err)
	}
	if rdy[0] != 'Z' {
		t.Fatalf("expected 'Z', got %q", rdy[0])
	}
}

// TestServer_TLS_AuthFailure verifies that wrong credentials over TLS cause
// the server to send an ErrorResponse and close the connection.
func TestServer_TLS_AuthFailure(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{
		TLSConfig: serverTLS,
		Auth:      server.BasicAuth{Username: "alice", Password: "s3cr3t"},
	})

	conn, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	// Send startup.
	if _, err := conn.Write(startupPacket("alice")); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	// Read auth challenge.
	buf := make([]byte, 9)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read auth challenge: %v", err)
	}
	// Send wrong password.
	pw := append([]byte("wrong"), 0)
	plen := uint32(len(pw) + 4)
	pmsg := make([]byte, 1+4+len(pw))
	pmsg[0] = 'p'
	binary.BigEndian.PutUint32(pmsg[1:5], plen)
	copy(pmsg[5:], pw)
	if _, err := conn.Write(pmsg); err != nil {
		t.Fatalf("write bad password: %v", err)
	}
	// Expect an ErrorResponse ('E').
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("read error header: %v", err)
	}
	if header[0] != 'E' {
		t.Errorf("expected ErrorResponse 'E', got %q", header[0])
	}
}

// ── SSLRequest negotiation tests ──────────────────────────────────────────────

// TestServer_SSLRequest_Declined verifies that when the server is configured
// WITHOUT TLSConfig, it responds to an SSLRequest with 'N' (decline) and
// continues to serve the subsequent plaintext startup packet correctly.
// This is the code path psql and pgx always exercise on first connect.
func TestServer_SSLRequest_Declined(t *testing.T) {
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	resp := doStartupWithSSL(t, conn, "memdb")
	if resp != 'N' {
		t.Errorf("expected SSL decline 'N', got %q (%d)", resp, resp)
	}

	// After the decline + startup, the server should send AuthOk + ReadyForQuery.
	buf := make([]byte, 9)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read AuthOk: %v", err)
	}
	if buf[0] != 'R' {
		t.Fatalf("expected 'R' after SSLRequest decline, got %q", buf[0])
	}
	rdy := make([]byte, 6)
	if _, err := io.ReadFull(conn, rdy); err != nil {
		t.Fatalf("read ReadyForQuery: %v", err)
	}
	if rdy[0] != 'Z' {
		t.Fatalf("expected 'Z' after SSLRequest decline, got %q", rdy[0])
	}
}

// TestServer_SSLRequest_Declined_QueryWorks verifies that a client that
// received 'N' (SSL decline) and then completed the startup handshake can
// execute a query successfully over the plaintext connection.
func TestServer_SSLRequest_Declined_QueryWorks(t *testing.T) {
	db := integrationDB(t)
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('ssl-test', 'ok')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	addr := startIntegServer(t, db, server.Config{})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	resp := doStartupWithSSL(t, conn, "memdb")
	if resp != 'N' {
		t.Fatalf("expected 'N', got %q", resp)
	}

	// Complete auth + ready.
	buf := make([]byte, 15) // 9 (AuthOk) + 6 (ReadyForQuery)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read startup response: %v", err)
	}

	sendSimpleQuery(t, conn, `SELECT value FROM kv WHERE key = 'ssl-test'`)
	rows := drainToReady(t, conn)
	if rows != 1 {
		t.Errorf("expected 1 DataRow after SSLRequest decline, got %d", rows)
	}
}

// TestServer_SSLRequest_MultipleTimes verifies that an SSLRequest followed
// by the startup packet is idempotent — a second connection doing the same
// also works. Exercises that the SSLRequest path does not corrupt handler
// state.
func TestServer_SSLRequest_MultipleTimes(t *testing.T) {
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{})

	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("iteration %d: Dial: %v", i, err)
		}

		resp := doStartupWithSSL(t, conn, "memdb")
		if resp != 'N' {
			conn.Close()
			t.Fatalf("iteration %d: expected 'N', got %q", i, resp)
		}
		// Drain AuthOk + ReadyForQuery.
		buf := make([]byte, 15)
		if _, err := io.ReadFull(conn, buf); err != nil {
			conn.Close()
			t.Fatalf("iteration %d: read startup response: %v", i, err)
		}
		conn.Close()
	}
}

// ── Concurrent connection tests ───────────────────────────────────────────────

// TestServer_ConcurrentConnections opens many connections simultaneously and
// verifies each can independently query the server without interfering with
// the others. This exercises the per-goroutine handler model and the
// sync.WaitGroup tracking in ListenAndServe.
func TestServer_ConcurrentConnections(t *testing.T) {
	db := integrationDB(t)
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`,
			"cc-key-"+string(rune('a'+i)), "val"); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	addr := startIntegServer(t, db, server.Config{})

	const goroutines = 32
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errCount.Add(1)
				return
			}
			defer conn.Close()

			doStartup(t, conn, "memdb")
			sendSimpleQuery(t, conn, `SELECT key, value FROM kv LIMIT 5`)
			drainToReady(t, conn)
		}()
	}

	wg.Wait()
	if n := errCount.Load(); n > 0 {
		t.Errorf("%d of %d concurrent goroutines failed to connect", n, goroutines)
	}
}

// TestServer_ConcurrentConnections_TLS is the same as
// TestServer_ConcurrentConnections but with TLS, verifying that the TLS
// accept path is safe under concurrent load.
func TestServer_ConcurrentConnections_TLS(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{TLSConfig: serverTLS})

	const goroutines = 16
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := tls.Dial("tcp", addr, clientTLS)
			if err != nil {
				errCount.Add(1)
				return
			}
			defer conn.Close()

			doStartup(t, conn, "memdb")
			sendSimpleQuery(t, conn, `SELECT 1`)
			drainToReady(t, conn)
		}()
	}

	wg.Wait()
	if n := errCount.Load(); n > 0 {
		t.Errorf("%d of %d TLS goroutines failed", n, goroutines)
	}
}

// TestServer_ConnectionsAfterStop verifies that after Stop() is called, new
// connection attempts are rejected and in-flight requests complete cleanly.
func TestServer_Stop_RejectsNewConnections(t *testing.T) {
	db := integrationDB(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := server.New(db, server.Config{ListenAddr: addr})
	go func() { _ = srv.ListenAndServe() }()

	// Wait for the server to be accepting.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Stop the server.
	srv.Stop()

	// New connections must fail.
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatal("expected connection refused after Stop(), got successful dial")
	}
}

// TestServer_Stop_WaitsForInFlight verifies that Stop() does not return
// until all in-flight handler goroutines have finished. We open a connection,
// start a slow operation (by sending a query and holding the connection open),
// call Stop() in a separate goroutine, and verify Stop() does not return
// until we close the connection.
func TestServer_Stop_WaitsForInFlight(t *testing.T) {
	db := integrationDB(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := server.New(db, server.Config{ListenAddr: addr})
	go func() { _ = srv.ListenAndServe() }()

	// Wait for the server to be accepting.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Open a connection and complete the handshake (handler goroutine starts).
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	doStartup(t, conn, "memdb")

	// Call Stop() in a goroutine; it should block until conn is closed.
	stopped := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopped)
	}()

	// Stop should not have returned yet — the handler is still running.
	select {
	case <-stopped:
		t.Error("Stop() returned before in-flight connection was closed")
	case <-time.After(100 * time.Millisecond):
		// Expected: Stop is still waiting.
	}

	// Close the client connection — the handler should exit.
	conn.Close()

	// Now Stop() must complete within a generous timeout.
	select {
	case <-stopped:
		// Pass.
	case <-time.After(3 * time.Second):
		t.Error("Stop() did not return within 3s after client connection closed")
	}
}

// ── Panic recovery tests ──────────────────────────────────────────────────────

// TestServer_HandlerPanic_OtherConnectionsUnaffected verifies that a panic
// inside one connection's handler goroutine does not crash the server and
// that other connections continue to work normally.
//
// We cannot easily inject a panic into handleConn from outside the package,
// so we simulate the effect by verifying that the server continues to accept
// and serve new connections after the *existing* connection is abruptly
// closed (which causes handleConn to receive an unexpected EOF — the
// equivalent of a partial failure). The real panic-recovery path is unit-
// tested in the root memdb package (TestPanic_* in memdb_extra_test.go);
// this test validates the server-level isolation contract.
func TestServer_AbruptClientClose_OtherConnectionsUnaffected(t *testing.T) {
	db := integrationDB(t)
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('probe', 'ok')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	addr := startIntegServer(t, db, server.Config{})

	// Connection 1: start the handshake then abruptly close (simulates a crash).
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("conn1 Dial: %v", err)
	}
	doStartup(t, conn1, "memdb")
	conn1.Close() // abrupt close mid-session

	// Give the server a moment to notice the EOF.
	time.Sleep(20 * time.Millisecond)

	// Connection 2: must work normally after conn1's abrupt close.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("conn2 Dial after abrupt close: %v", err)
	}
	defer conn2.Close()

	doStartup(t, conn2, "memdb")
	sendSimpleQuery(t, conn2, `SELECT value FROM kv WHERE key = 'probe'`)
	rows := drainToReady(t, conn2)
	if rows != 1 {
		t.Errorf("conn2: expected 1 row, got %d", rows)
	}
}

// ── Message size / protocol edge cases ───────────────────────────────────────

// TestServer_OversizeQuery verifies that a query message exceeding
// maxMessageLen causes the server to close the connection rather than
// allocating gigabytes of memory or crashing.
func TestServer_OversizeQuery(t *testing.T) {
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	doStartup(t, conn, "memdb")

	// Craft a 'Q' message claiming length = 32 MB (well over maxMessageLen=16MB).
	const claimedLen = 32 * 1024 * 1024
	oversized := make([]byte, 5)
	oversized[0] = 'Q'
	binary.BigEndian.PutUint32(oversized[1:5], claimedLen)
	if _, err := conn.Write(oversized); err != nil {
		// The server may close before we finish writing — that's fine.
		return
	}

	// The server should close the connection after reading the invalid length.
	// We expect either an EOF or an ErrorResponse.
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Logf("SetReadDeadline: %v (non-fatal)", err)
	}
	buf := make([]byte, 5)
	_, readErr := io.ReadFull(conn, buf)
	if readErr == nil {
		// If we got a response, it must be an ErrorResponse.
		if buf[0] != 'E' {
			t.Errorf("expected ErrorResponse or EOF, got message type %q", buf[0])
		}
	}
	// EOF / connection closed is also acceptable — the server rejected the message.
}

// TestServer_EmptyQuery verifies that an empty query string is handled
// gracefully: the server must return EmptyQueryResponse + ReadyForQuery
// rather than panicking or hanging.
func TestServer_EmptyQuery(t *testing.T) {
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	doStartup(t, conn, "memdb")
	sendSimpleQuery(t, conn, "") // empty query string

	// Must receive either EmptyQueryResponse ('I') or CommandComplete ('C'),
	// followed by ReadyForQuery. Must NOT hang or error.
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Logf("SetReadDeadline: %v (non-fatal)", err)
	}
	gotReady := false
	for !gotReady {
		msgType, _ := readMsg(t, conn)
		switch msgType {
		case 'I', 'C': // EmptyQueryResponse or CommandComplete
		case 'Z':
			gotReady = true
		case 'E':
			// Some servers return an error for empty queries — also acceptable.
			gotReady = true
		default:
			t.Logf("unexpected message type %q on empty query", msgType)
		}
	}
}

// TestServer_Terminate_TLS verifies that the Terminate ('X') message causes
// the server to cleanly close a TLS connection.
func TestServer_Terminate_TLS(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{TLSConfig: serverTLS})

	conn, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	doStartup(t, conn, "memdb")

	// Send Terminate ('X', length=4).
	if _, err := conn.Write([]byte{'X', 0, 0, 0, 4}); err != nil {
		t.Fatalf("write Terminate: %v", err)
	}

	// Server must close the connection: next read must return EOF or error.
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Logf("SetReadDeadline: %v (non-fatal)", err)
	}
	buf := make([]byte, 1)
	_, readErr := conn.Read(buf)
	if readErr == nil {
		t.Error("expected EOF after Terminate on TLS connection, got data")
	}
}

// ── Logger tests ──────────────────────────────────────────────────────────────

// TestServer_Config_Logger verifies that a non-nil Logger in server.Config
// is used by the server rather than slog.Default(). We verify this
// indirectly by confirming the server still operates correctly when a
// discard logger is provided — the Logger field must not panic or break
// any normal code path.
func TestServer_Config_Logger_DoesNotBreakNormalOperation(t *testing.T) {
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{
		// Use a no-op writer to ensure Logger is exercised without polluting
		// test output. Any panic from Logger use would surface as a test failure.
		Logger: newDiscardLogger(),
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	doStartup(t, conn, "memdb")
	sendSimpleQuery(t, conn, `SELECT 1`)
	rows := drainToReady(t, conn)
	if rows != 1 {
		t.Errorf("expected 1 row, got %d", rows)
	}
}

// TestServer_Config_Logger_TLS verifies that a Logger is also exercised on
// the TLS path without breaking normal operation.
func TestServer_Config_Logger_TLS(t *testing.T) {
	serverTLS, clientTLS := generateServerTLS(t)
	db := integrationDB(t)
	addr := startIntegServer(t, db, server.Config{
		TLSConfig: serverTLS,
		Logger:    newDiscardLogger(),
	})

	conn, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()

	doStartup(t, conn, "memdb")
	sendSimpleQuery(t, conn, `SELECT 1`)
	rows := drainToReady(t, conn)
	if rows != 1 {
		t.Errorf("expected 1 row, got %d", rows)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// newDiscardLogger returns a *slog.Logger that silently drops all output.
// Named with a "new" prefix to avoid a redeclaration collision with the
// silentLogger() helper defined in server_test.go (same package).
func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError + 1,
	}))
}
