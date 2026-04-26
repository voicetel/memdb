package server_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
)

// TestServer_Extended_ParameterizedSelect drives the Parse / Bind /
// Describe / Execute / Sync flow against a real server and confirms it
// returns the expected RowDescription + DataRows + CommandComplete.
//
// We deliberately do NOT use pgx or lib/pq here: those drivers add
// their own connection-startup chatter (LISTEN/UNLISTEN, charset
// query, schema introspection) that obscures wire-protocol bugs in
// our own implementation. A hand-rolled client makes a wire-level
// mistake on either side immediately visible.
func TestServer_Extended_ParameterizedSelect(t *testing.T) {
	addr, db := startServer(t)
	if _, err := db.Exec(`INSERT INTO kv(key, value) VALUES ('a','1'), ('b','2'), ('c','3')`); err != nil {
		t.Fatal(err)
	}

	conn := dialAndStartup(t, addr)

	// Parse: name="", SELECT * FROM kv WHERE key = $1, 1 param oid=0 (unspecified)
	parseBody := []byte{0} // statement name = ""
	parseBody = append(parseBody, "SELECT key, value FROM kv WHERE key = $1"...)
	parseBody = append(parseBody, 0) // SQL terminator
	parseBody = append(parseBody, 0, 1, 0, 0, 0, 0)
	// nParams=1, then OID=0 (text/unknown)
	send(t, conn, 'P', parseBody)

	// Bind: portal="", stmt="", paramFormats=[], paramCount=1, param="a"
	bindBody := []byte{0, 0} // portal name "" + stmt name ""
	bindBody = append(bindBody, 0, 0)
	// nFmt=0
	bindBody = append(bindBody, 0, 1)
	// nParams=1
	bindBody = append(bindBody, 0, 0, 0, 1, 'a')
	// paramLen=1, value="a"
	bindBody = append(bindBody, 0, 0)
	// nResultFormats=0
	send(t, conn, 'B', bindBody)

	// Describe portal "" — expects RowDescription
	descBody := append([]byte{'P'}, 0)
	send(t, conn, 'D', descBody)

	// Execute portal "" with no row limit
	execBody := []byte{0, 0, 0, 0, 0, 0}
	send(t, conn, 'E', execBody)

	// Sync
	send(t, conn, 'S', nil)

	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Logf("CloseWrite: %v", err) // optional — server keeps reading until EOF
	}

	// Read responses until ReadyForQuery.
	got := drainUntilReady(t, conn)

	wantTypes := []byte{'1', '2', 'T', 'D', 'C', 'Z'}
	if string(got.types) != string(wantTypes) {
		t.Fatalf("response types %q, want %q", got.types, wantTypes)
	}
	// The single DataRow should encode the row {"a", "1"}.
	dataRow := got.bodies[3]
	cells := decodeDataRow(t, dataRow)
	if len(cells) != 2 || string(cells[0]) != "a" || string(cells[1]) != "1" {
		t.Fatalf("data row = %q, want [\"a\", \"1\"]", cells)
	}
}

// TestServer_Extended_ParameterizedInsert confirms DML round-trips
// through Parse/Bind/Execute and returns "INSERT 0 1" — the canonical
// PG CommandComplete tag.
func TestServer_Extended_ParameterizedInsert(t *testing.T) {
	addr, db := startServer(t)

	conn := dialAndStartup(t, addr)

	parseBody := []byte{0}
	parseBody = append(parseBody, "INSERT INTO kv(key, value) VALUES ($1, $2)"...)
	parseBody = append(parseBody, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0)
	// nParams=2, two OIDs of 0
	send(t, conn, 'P', parseBody)

	bindBody := []byte{0, 0, 0, 0, 0, 2}
	// portal="" stmt="" nFmt=0 nParams=2
	// param 1: len=4, "key1"
	bindBody = append(bindBody, 0, 0, 0, 4, 'k', 'e', 'y', '1')
	// param 2: len=3, "val"
	bindBody = append(bindBody, 0, 0, 0, 3, 'v', 'a', 'l')
	// nResultFormats=0
	bindBody = append(bindBody, 0, 0)
	send(t, conn, 'B', bindBody)

	send(t, conn, 'E', []byte{0, 0, 0, 0, 0, 0})
	send(t, conn, 'S', nil)

	got := drainUntilReady(t, conn)
	wantTypes := []byte{'1', '2', 'C', 'Z'}
	if string(got.types) != string(wantTypes) {
		t.Fatalf("response types %q, want %q", got.types, wantTypes)
	}
	tag := trimTrailingNULTest(got.bodies[2])
	if tag != "INSERT 0 1" {
		t.Fatalf("CommandComplete = %q, want %q", tag, "INSERT 0 1")
	}
	// And the row should actually be in the database now.
	var v string
	if err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, "key1").Scan(&v); err != nil {
		t.Fatalf("verifying row: %v", err)
	}
	if v != "val" {
		t.Fatalf("row value = %q, want %q", v, "val")
	}
}

// TestServer_Extended_ErrorState_SkipsUntilSync verifies the
// "skip-until-Sync" semantics: a Bind that references a non-existent
// statement triggers an ErrorResponse, after which subsequent
// extended-query messages must be silently ignored until the next Sync.
func TestServer_Extended_ErrorState_SkipsUntilSync(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	// Bind to a statement that was never Parsed → triggers an error.
	bindBody := []byte{0, 0}
	bindBody = append(bindBody, "missing"...)
	bindBody = append(bindBody, 0)
	bindBody = append(bindBody, 0, 0, 0, 0, 0, 0)
	send(t, conn, 'B', bindBody)

	// Server should ignore this Execute (error state).
	send(t, conn, 'E', []byte{0, 0, 0, 0, 0, 0})

	// Sync clears the error state.
	send(t, conn, 'S', nil)

	got := drainUntilReady(t, conn)
	// Expect: ErrorResponse ('E') from the Bind, then ReadyForQuery ('Z')
	// from the Sync — Execute in between produced nothing.
	wantTypes := []byte{'E', 'Z'}
	if string(got.types) != string(wantTypes) {
		t.Fatalf("response types %q, want %q", got.types, wantTypes)
	}
}

// ── wire helpers (extended query) ────────────────────────────────────────────

type collected struct {
	types  []byte
	bodies [][]byte
}

func send(t *testing.T, conn net.Conn, msgType byte, body []byte) {
	t.Helper()
	totalLen := uint32(4 + len(body))
	buf := make([]byte, 0, 1+totalLen)
	buf = append(buf, msgType)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], totalLen)
	buf = append(buf, lb[:]...)
	buf = append(buf, body...)
	if _, err := conn.Write(buf); err != nil {
		t.Fatal(err)
	}
}

func drainUntilReady(t *testing.T, conn net.Conn) collected {
	t.Helper()
	var out collected
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			t.Fatalf("read: %v", err)
		}
		mt := header[0]
		length := int(binary.BigEndian.Uint32(header[1:5]))
		body := make([]byte, length-4)
		if length > 4 {
			if _, err := io.ReadFull(conn, body); err != nil {
				t.Fatalf("read body: %v", err)
			}
		}
		out.types = append(out.types, mt)
		out.bodies = append(out.bodies, body)
		if mt == 'Z' {
			return out
		}
	}
}

func decodeDataRow(t *testing.T, body []byte) [][]byte {
	t.Helper()
	if len(body) < 2 {
		t.Fatalf("DataRow too short")
	}
	n := int(binary.BigEndian.Uint16(body[:2]))
	cells := make([][]byte, n)
	pos := 2
	for i := 0; i < n; i++ {
		if pos+4 > len(body) {
			t.Fatalf("DataRow truncated at cell %d", i)
		}
		l := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if l == -1 {
			cells[i] = nil
			continue
		}
		cells[i] = body[pos : pos+int(l)]
		pos += int(l)
	}
	return cells
}

func trimTrailingNULTest(b []byte) string {
	n := len(b)
	for n > 0 && b[n-1] == 0 {
		n--
	}
	return string(b[:n])
}

var _ = fmt.Sprintf // keep fmt referenced in case future debug Printlns reappear
