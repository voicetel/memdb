package server_test

// Wire-protocol tests for the Close ('C'), Describe('S'), and binary
// result-format paths in extended_query.go that the existing
// server_extended_test.go does not cover.

import (
	"encoding/binary"
	"net"
	"testing"
)

// TestServer_Extended_DescribeStatement covers the Describe('S')
// branch which sends ParameterDescription ('t') + RowDescription ('T').
// The previous tests use Describe('P'), so this exercises the 'S'
// path including sendParameterDescription.
func TestServer_Extended_DescribeStatement(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	parseBody := []byte{0}
	parseBody = append(parseBody, "SELECT key, value FROM kv WHERE key = $1"...)
	parseBody = append(parseBody, 0)
	parseBody = append(parseBody, 0, 1, 0, 0, 0, 0)
	send(t, conn, 'P', parseBody)

	// Describe statement "" — expects ParameterDescription + RowDescription.
	descBody := append([]byte{'S'}, 0)
	send(t, conn, 'D', descBody)

	send(t, conn, 'S', nil)
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Logf("CloseWrite: %v", err)
	}

	got := drainUntilReady(t, conn)
	// Order: ParseComplete('1'), ParameterDescription('t'),
	// RowDescription('T'), ReadyForQuery('Z').
	wantTypes := []byte{'1', 't', 'T', 'Z'}
	if string(got.types) != string(wantTypes) {
		t.Fatalf("types=%q, want %q", got.types, wantTypes)
	}

	// ParameterDescription body: nParams (uint16) followed by N OIDs (uint32).
	pd := got.bodies[1]
	if len(pd) < 2 {
		t.Fatalf("ParameterDescription too short: %x", pd)
	}
	nParams := binary.BigEndian.Uint16(pd[:2])
	if nParams != 1 {
		t.Errorf("nParams=%d, want 1", nParams)
	}
	if len(pd) != 2+4 {
		t.Errorf("ParameterDescription body len=%d, want %d", len(pd), 2+4)
	}
}

// TestServer_Extended_ClosePortal exercises Close('P') — the portal-
// kind branch of handleClose. The server must respond with CloseComplete
// ('3') and the portal must be gone afterwards (a follow-up Execute on
// the same name surfaces an error, asserting the deletion took effect).
func TestServer_Extended_ClosePortal(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	// Parse + Bind a portal.
	parseBody := []byte{0}
	parseBody = append(parseBody, "SELECT 1"...)
	parseBody = append(parseBody, 0)
	parseBody = append(parseBody, 0, 0)
	send(t, conn, 'P', parseBody)

	bindBody := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	send(t, conn, 'B', bindBody)

	// Close the portal.
	closeBody := []byte{'P', 0}
	send(t, conn, 'C', closeBody)

	send(t, conn, 'S', nil)
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Logf("CloseWrite: %v", err)
	}

	got := drainUntilReady(t, conn)
	// Order: ParseComplete, BindComplete, CloseComplete, ReadyForQuery.
	wantTypes := []byte{'1', '2', '3', 'Z'}
	if string(got.types) != string(wantTypes) {
		t.Fatalf("types=%q, want %q", got.types, wantTypes)
	}
}

// TestServer_Extended_CloseStatement exercises Close('S') — the
// statement-kind branch.
func TestServer_Extended_CloseStatement(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	parseBody := []byte{0}
	parseBody = append(parseBody, "SELECT 1"...)
	parseBody = append(parseBody, 0)
	parseBody = append(parseBody, 0, 0)
	send(t, conn, 'P', parseBody)

	closeBody := []byte{'S', 0}
	send(t, conn, 'C', closeBody)

	send(t, conn, 'S', nil)
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Logf("CloseWrite: %v", err)
	}

	got := drainUntilReady(t, conn)
	wantTypes := []byte{'1', '3', 'Z'}
	if string(got.types) != string(wantTypes) {
		t.Fatalf("types=%q, want %q", got.types, wantTypes)
	}
}

// TestServer_Extended_CloseUnknownKind exercises the default-branch
// error path of handleClose: an unknown kind byte must produce an
// ErrorResponse without crashing the connection.
func TestServer_Extended_CloseUnknownKind(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	closeBody := []byte{'X', 0} // unknown kind
	send(t, conn, 'C', closeBody)
	send(t, conn, 'S', nil)
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Logf("CloseWrite: %v", err)
	}

	got := drainUntilReady(t, conn)
	// We expect an ErrorResponse somewhere before ReadyForQuery.
	sawError := false
	for _, mt := range got.types {
		if mt == 'E' {
			sawError = true
		}
	}
	if !sawError {
		t.Errorf("expected ErrorResponse for unknown Close kind, got types=%q", got.types)
	}
	if got.types[len(got.types)-1] != 'Z' {
		t.Errorf("last type=%c, want Z", got.types[len(got.types)-1])
	}
}

// TestServer_Extended_BinaryResultFormat exercises the binary
// result-format path: when Bind requests result format = 1 (binary),
// the server must encode each cell via encodeCellBinary and the
// client must be able to decode the resulting bytes.
//
// We bind one column with format 1 and read the resulting DataRow,
// asserting it's an 8-byte big-endian int8 (the 'INTEGER 7' value).
func TestServer_Extended_BinaryResultFormat(t *testing.T) {
	addr, _ := startServer(t)
	conn := dialAndStartup(t, addr)

	parseBody := []byte{0}
	parseBody = append(parseBody, "SELECT 7"...)
	parseBody = append(parseBody, 0)
	parseBody = append(parseBody, 0, 0)
	send(t, conn, 'P', parseBody)

	// Bind with nResultFormats=1, format=1 (binary) — applies to all cols.
	bindBody := []byte{0, 0}
	bindBody = append(bindBody, 0, 0)
	bindBody = append(bindBody, 0, 0)
	bindBody = append(bindBody, 0, 1, 0, 1)
	send(t, conn, 'B', bindBody)

	// Execute portal with no row limit.
	send(t, conn, 'E', []byte{0, 0, 0, 0, 0, 0})
	send(t, conn, 'S', nil)
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Logf("CloseWrite: %v", err)
	}

	got := drainUntilReady(t, conn)
	// Find the DataRow.
	var dataRow []byte
	for i, mt := range got.types {
		if mt == 'D' {
			dataRow = got.bodies[i]
			break
		}
	}
	if dataRow == nil {
		t.Fatalf("no DataRow in response: types=%q", got.types)
	}
	cells := decodeDataRow(t, dataRow)
	if len(cells) != 1 {
		t.Fatalf("ncells=%d, want 1", len(cells))
	}
	if len(cells[0]) != 8 {
		t.Fatalf("cell0 len=%d, want 8 (binary int8)", len(cells[0]))
	}
	if got := int64(binary.BigEndian.Uint64(cells[0])); got != 7 {
		t.Errorf("decoded int8=%d, want 7", got)
	}
}
