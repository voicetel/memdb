package server_test

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/server"
)

// ── wire helpers ──────────────────────────────────────────────────────────────

// dialAndStartup dials addr, completes the startup handshake (no auth), and
// returns the open connection. The connection is closed via t.Cleanup.
func dialAndStartup(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	// Send startup message: length=8, protocol version 3.0 (196608)
	startup := []byte{0, 0, 0, 8, 0, 3, 0, 0}
	if _, err := conn.Write(startup); err != nil {
		t.Fatal(err)
	}

	// Read AuthenticationOk: 'R' + length=8 + int32=0  (9 bytes total)
	buf := make([]byte, 9)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("reading AuthenticationOk: %v", err)
	}
	if buf[0] != 'R' {
		t.Fatalf("expected AuthenticationOk ('R'), got %q", buf[0])
	}

	// Read ReadyForQuery: 'Z' + length=5 + 'I'  (6 bytes total)
	rdy := make([]byte, 6)
	if _, err := io.ReadFull(conn, rdy); err != nil {
		t.Fatalf("reading ReadyForQuery: %v", err)
	}
	if rdy[0] != 'Z' {
		t.Fatalf("expected ReadyForQuery ('Z'), got %q", rdy[0])
	}

	return conn
}

// sendQuery writes a Simple Query ('Q') message to conn.
func sendQuery(t *testing.T, conn net.Conn, query string) {
	t.Helper()
	body := append([]byte(query), 0) // null-terminate
	length := int32(len(body) + 4)
	msg := []byte{
		'Q',
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}
	msg = append(msg, body...)
	if _, err := conn.Write(msg); err != nil {
		t.Fatal(err)
	}
}

// readMessage reads one server message and returns its type and body.
// The 4-byte length field is NOT included in the returned body slice.
func readMessage(t *testing.T, conn net.Conn) (msgType byte, body []byte) {
	t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatal(err)
	}
	msgType = header[0]
	length := int(header[1])<<24 | int(header[2])<<16 | int(header[3])<<8 | int(header[4])
	body = make([]byte, length-4)
	if len(body) > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			t.Fatal(err)
		}
	}
	return
}

// ── server helper ─────────────────────────────────────────────────────────────

// startServer opens a memdb.DB with a kv schema and starts a server on a
// random port. It returns the listening address and the DB.
func startServer(t *testing.T) (string, *memdb.DB) {
	t.Helper()
	return startServerWithConfig(t, server.Config{})
}

// startServerWithConfig is like startServer but allows injecting server.Config
// fields (e.g. Auth). ListenAddr is always overridden with a free port.
func startServerWithConfig(t *testing.T, cfg server.Config) (string, *memdb.DB) {
	t.Helper()

	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(t.TempDir(), "test.db"),
		FlushInterval: -1,
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// Grab a free port by binding then immediately releasing it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg.ListenAddr = addr
	srv := server.New(db, cfg)

	started := make(chan struct{})
	go func() {
		close(started)
		_ = srv.ListenAndServe()
	}()
	<-started
	// Give the goroutine a moment to reach l.Accept().
	time.Sleep(20 * time.Millisecond)

	t.Cleanup(func() { srv.Stop() })
	return addr, db
}

// ── auth helpers ──────────────────────────────────────────────────────────────

// doAuthHandshake performs the cleartext-password auth exchange.
// Returns true if the server accepted the password (AuthOk + ReadyForQuery).
func doAuthHandshake(t *testing.T, conn net.Conn, password string) bool {
	t.Helper()

	// Send startup
	if _, err := conn.Write([]byte{0, 0, 0, 8, 0, 3, 0, 0}); err != nil {
		t.Fatal(err)
	}

	// Read first 'R' message — expect AuthenticationCleartextPassword (body=3)
	msgType, body := readMessage(t, conn)
	if msgType != 'R' {
		t.Fatalf("expected 'R' (auth request), got %q", msgType)
	}
	if len(body) < 4 || body[3] != 3 {
		t.Fatalf("expected cleartext password request (int32=3), got body %v", body)
	}

	// Send PasswordMessage: 'p' + length(4 + len(pwd) + 1) + password + NUL
	pwd := append([]byte(password), 0)
	plen := int32(4 + len(pwd))
	msg := []byte{
		'p',
		byte(plen >> 24),
		byte(plen >> 16),
		byte(plen >> 8),
		byte(plen),
	}
	msg = append(msg, pwd...)
	if _, err := conn.Write(msg); err != nil {
		t.Fatal(err)
	}

	// Read next message
	msgType, body = readMessage(t, conn)
	if msgType == 'E' {
		// Error — authentication failed
		return false
	}
	if msgType != 'R' || len(body) < 4 || body[3] != 0 {
		t.Fatalf("expected AuthenticationOk ('R' body=0), got type=%q body=%v", msgType, body)
	}

	// Read ReadyForQuery
	msgType, _ = readMessage(t, conn)
	if msgType != 'Z' {
		t.Fatalf("expected ReadyForQuery ('Z'), got %q", msgType)
	}
	return true
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestServer_BasicAuth_Success(t *testing.T) {
	addr, _ := startServerWithConfig(t, server.Config{
		Auth: server.BasicAuth{Username: "memdb", Password: "secret"},
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ok := doAuthHandshake(t, conn, "secret")
	if !ok {
		t.Fatal("expected authentication to succeed")
	}
}

func TestServer_BasicAuth_Failure(t *testing.T) {
	addr, _ := startServerWithConfig(t, server.Config{
		Auth: server.BasicAuth{Username: "memdb", Password: "secret"},
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ok := doAuthHandshake(t, conn, "wrongpassword")
	if ok {
		t.Fatal("expected authentication to fail")
	}
}

func TestServer_SimpleQuery_SELECT(t *testing.T) {
	addr, db := startServer(t)

	// Seed a row directly via the DB.
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('hello', 'world')`); err != nil {
		t.Fatal(err)
	}

	conn := dialAndStartup(t, addr)
	sendQuery(t, conn, `SELECT value FROM kv WHERE key = 'hello'`)

	var gotRowDesc, gotDataRow, gotCommandComplete, gotReadyForQuery bool
	for {
		msgType, body := readMessage(t, conn)
		switch msgType {
		case 'T': // RowDescription
			gotRowDesc = true
		case 'D': // DataRow
			gotDataRow = true
			// Parse: int16(colCount) + per-column data
			if len(body) < 2 {
				t.Fatal("DataRow too short")
			}
			colCount := int(body[0])<<8 | int(body[1])
			if colCount != 1 {
				t.Fatalf("expected 1 column, got %d", colCount)
			}
			// Column value: int32(len) + bytes
			if len(body) < 6 {
				t.Fatal("DataRow body too short for column data")
			}
			valLen := int(body[2])<<24 | int(body[3])<<16 | int(body[4])<<8 | int(body[5])
			val := string(body[6 : 6+valLen])
			if val != "world" {
				t.Errorf("expected value 'world', got %q", val)
			}
		case 'C': // CommandComplete
			gotCommandComplete = true
			tag := strings.TrimRight(string(body), "\x00")
			if !strings.HasPrefix(tag, "SELECT") {
				t.Errorf("expected CommandComplete tag to start with 'SELECT', got %q", tag)
			}
		case 'Z': // ReadyForQuery
			gotReadyForQuery = true
		case 'E': // ErrorResponse
			t.Fatalf("unexpected ErrorResponse: %s", body)
		}
		if gotReadyForQuery {
			break
		}
	}

	if !gotRowDesc {
		t.Error("did not receive RowDescription")
	}
	if !gotDataRow {
		t.Error("did not receive DataRow")
	}
	if !gotCommandComplete {
		t.Error("did not receive CommandComplete")
	}
}

func TestServer_SimpleQuery_INSERT(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	sendQuery(t, conn, `INSERT INTO kv (key, value) VALUES ('k1', 'v1')`)

	var gotCommandComplete bool
	for {
		msgType, body := readMessage(t, conn)
		switch msgType {
		case 'C':
			gotCommandComplete = true
			tag := strings.TrimRight(string(body), "\x00")
			if tag != "INSERT 0 1" {
				t.Errorf("expected tag 'INSERT 0 1', got %q", tag)
			}
		case 'Z':
			goto done
		case 'E':
			t.Fatalf("unexpected ErrorResponse: %s", body)
		}
	}
done:
	if !gotCommandComplete {
		t.Error("did not receive CommandComplete")
	}
}

func TestServer_SimpleQuery_UPDATE(t *testing.T) {
	addr, db := startServer(t)

	// Seed a row.
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('k1', 'v1')`); err != nil {
		t.Fatal(err)
	}

	conn := dialAndStartup(t, addr)
	sendQuery(t, conn, `UPDATE kv SET value = 'v2' WHERE key = 'k1'`)

	var gotCommandComplete bool
	for {
		msgType, body := readMessage(t, conn)
		switch msgType {
		case 'C':
			gotCommandComplete = true
			tag := strings.TrimRight(string(body), "\x00")
			if tag != "UPDATE 1" {
				t.Errorf("expected tag 'UPDATE 1', got %q", tag)
			}
		case 'Z':
			goto done
		case 'E':
			t.Fatalf("unexpected ErrorResponse: %s", body)
		}
	}
done:
	if !gotCommandComplete {
		t.Error("did not receive CommandComplete")
	}
}

func TestServer_SimpleQuery_DELETE(t *testing.T) {
	addr, db := startServer(t)

	// Seed a row.
	if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES ('k1', 'v1')`); err != nil {
		t.Fatal(err)
	}

	conn := dialAndStartup(t, addr)
	sendQuery(t, conn, `DELETE FROM kv WHERE key = 'k1'`)

	var gotCommandComplete bool
	for {
		msgType, body := readMessage(t, conn)
		switch msgType {
		case 'C':
			gotCommandComplete = true
			tag := strings.TrimRight(string(body), "\x00")
			if tag != "DELETE 1" {
				t.Errorf("expected tag 'DELETE 1', got %q", tag)
			}
		case 'Z':
			goto done
		case 'E':
			t.Fatalf("unexpected ErrorResponse: %s", body)
		}
	}
done:
	if !gotCommandComplete {
		t.Error("did not receive CommandComplete")
	}
}

func TestServer_SimpleQuery_Error(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	sendQuery(t, conn, `SELECT * FROM nonexistent_table`)

	var gotError bool
	for {
		msgType, _ := readMessage(t, conn)
		switch msgType {
		case 'E':
			gotError = true
		case 'Z':
			goto done
		}
	}
done:
	if !gotError {
		t.Error("expected an ErrorResponse for query against nonexistent table")
	}
}

func TestServer_Terminate(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	// Send Terminate message: 'X' + length=4
	terminate := []byte{'X', 0, 0, 0, 4}
	if _, err := conn.Write(terminate); err != nil {
		t.Fatal(err)
	}

	// Give the server a moment to close the connection.
	time.Sleep(20 * time.Millisecond)

	// Subsequent reads should return EOF or an error.
	buf := make([]byte, 1)
	if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	_, err := conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed after Terminate, but Read returned nil error")
	}
	// Accept either io.EOF or a network error — both indicate the server closed the conn.
}

func TestServer_UnixSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix domain sockets not available on Windows")
	}

	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(t.TempDir(), "test.db"),
		FlushInterval: -1,
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	sockPath := filepath.Join(t.TempDir(), "memdb-test.sock")
	srv := server.New(db, server.Config{
		ListenAddr: "unix://" + sockPath,
	})

	started := make(chan struct{})
	go func() {
		close(started)
		_ = srv.ListenAndServe()
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	t.Cleanup(func() { srv.Stop() })

	// Verify the socket file exists.
	if _, err := os.Stat(sockPath); err != nil {
		t.Fatalf("unix socket not created: %v", err)
	}

	// Dial via unix socket.
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("Dial unix: %v", err)
	}
	defer conn.Close()

	// Startup handshake.
	if _, err := conn.Write([]byte{0, 0, 0, 8, 0, 3, 0, 0}); err != nil {
		t.Fatal(err)
	}

	// Read AuthenticationOk (9 bytes).
	authBuf := make([]byte, 9)
	if _, err := io.ReadFull(conn, authBuf); err != nil {
		t.Fatalf("reading AuthOk: %v", err)
	}
	if authBuf[0] != 'R' {
		t.Fatalf("expected 'R', got %q", authBuf[0])
	}

	// Read ReadyForQuery (6 bytes).
	rdyBuf := make([]byte, 6)
	if _, err := io.ReadFull(conn, rdyBuf); err != nil {
		t.Fatalf("reading ReadyForQuery: %v", err)
	}
	if rdyBuf[0] != 'Z' {
		t.Fatalf("expected 'Z', got %q", rdyBuf[0])
	}

	// Send a query and verify we get a response.
	body := append([]byte("SELECT 1"), 0)
	plen := int32(len(body) + 4)
	qmsg := []byte{'Q', byte(plen >> 24), byte(plen >> 16), byte(plen >> 8), byte(plen)}
	qmsg = append(qmsg, body...)
	if _, err := conn.Write(qmsg); err != nil {
		t.Fatal(err)
	}

	// Drain messages until ReadyForQuery — just verify no error.
	for {
		hdr := make([]byte, 5)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			t.Fatalf("reading message header: %v", err)
		}
		msgLen := int(hdr[1])<<24 | int(hdr[2])<<16 | int(hdr[3])<<8 | int(hdr[4])
		if msgLen > 4 {
			rest := make([]byte, msgLen-4)
			if _, err := io.ReadFull(conn, rest); err != nil {
				t.Fatalf("reading message body: %v", err)
			}
		}
		if hdr[0] == 'E' {
			t.Fatal("unexpected ErrorResponse on unix socket")
		}
		if hdr[0] == 'Z' {
			break
		}
	}
}

// ── unit test for BasicAuth ───────────────────────────────────────────────────

func TestBasicAuth_Authenticate(t *testing.T) {
	auth := server.BasicAuth{Username: "alice", Password: "letmein"}

	tests := []struct {
		name     string
		username string
		password string
		want     bool
	}{
		{"correct credentials", "alice", "letmein", true},
		{"wrong password", "alice", "wrong", false},
		{"wrong username", "bob", "letmein", false},
		{"both wrong", "bob", "wrong", false},
		{"empty credentials", "", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := auth.Authenticate(tc.username, tc.password)
			if got != tc.want {
				t.Errorf("Authenticate(%q, %q) = %v, want %v",
					tc.username, tc.password, got, tc.want)
			}
		})
	}
}

// TestServer_MultipleRows verifies that multiple DataRow messages are sent
// when a SELECT returns more than one result.
func TestServer_MultipleRows(t *testing.T) {
	addr, db := startServer(t)

	rows := []struct{ key, val string }{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, r.key, r.val); err != nil {
			t.Fatal(err)
		}
	}

	conn := dialAndStartup(t, addr)
	sendQuery(t, conn, `SELECT key, value FROM kv ORDER BY key`)

	dataRowCount := 0
	for {
		msgType, body := readMessage(t, conn)
		switch msgType {
		case 'T': // RowDescription — OK
		case 'D': // DataRow
			dataRowCount++
		case 'C': // CommandComplete
			tag := strings.TrimRight(string(body), "\x00")
			if !strings.HasPrefix(tag, "SELECT 3") {
				t.Errorf("expected 'SELECT 3', got %q", tag)
			}
		case 'Z':
			goto done
		case 'E':
			t.Fatalf("unexpected ErrorResponse: %s", body)
		}
	}
done:
	if dataRowCount != 3 {
		t.Errorf("expected 3 DataRow messages, got %d", dataRowCount)
	}
}

// TestServer_EmptySELECT verifies that a SELECT returning no rows still sends
// RowDescription + CommandComplete + ReadyForQuery.
func TestServer_EmptySELECT(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	sendQuery(t, conn, `SELECT value FROM kv WHERE key = 'nosuchkey'`)

	var gotRowDesc, gotCommandComplete bool
	for {
		msgType, body := readMessage(t, conn)
		switch msgType {
		case 'T':
			gotRowDesc = true
		case 'D':
			t.Error("unexpected DataRow for empty result set")
		case 'C':
			gotCommandComplete = true
			tag := strings.TrimRight(string(body), "\x00")
			if tag != "SELECT 0" {
				t.Errorf("expected 'SELECT 0', got %q", tag)
			}
		case 'Z':
			goto done
		case 'E':
			t.Fatalf("unexpected ErrorResponse: %s", body)
		}
	}
done:
	if !gotRowDesc {
		t.Error("expected RowDescription for empty result set")
	}
	if !gotCommandComplete {
		t.Error("expected CommandComplete for empty result set")
	}
}
