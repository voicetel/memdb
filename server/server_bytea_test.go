package server_test

// Wire-level tests for the PostgreSQL-compat fixes that unblock
// libpq-based clients writing binary payloads (e.g. OpenSIPS msilo over
// db_postgres):
//
//   - startup ParameterStatus advertisement (server_version ≥ 9.0 +
//     standard_conforming_strings=on select modern libpq encodings),
//   - bytea hex output for BLOB result cells in text format,
//   - bytea text-input decoding for OID-17 extended-query parameters.

import (
	"io"
	"net"
	"testing"

	"github.com/voicetel/memdb"
)

// readParameterStatuses dials addr, performs the no-auth startup, and
// returns the ParameterStatus name→value pairs the server sent before
// ReadyForQuery.
func readParameterStatuses(t *testing.T, addr string) map[string]string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if _, err := conn.Write([]byte{0, 0, 0, 8, 0, 3, 0, 0}); err != nil {
		t.Fatal(err)
	}
	// AuthenticationOk.
	buf := make([]byte, 9)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("reading AuthenticationOk: %v", err)
	}
	if buf[0] != 'R' {
		t.Fatalf("expected 'R', got %q", buf[0])
	}

	params := make(map[string]string)
	for {
		msgType, body := readMessage(t, conn)
		switch msgType {
		case 'S':
			// name\0value\0
			var name, value string
			for i, b := range body {
				if b == 0 {
					name = string(body[:i])
					value = trimTrailingNULTest(body[i+1:])
					break
				}
			}
			params[name] = value
		case 'Z':
			return params
		default:
			t.Fatalf("unexpected message %q during startup", msgType)
		}
	}
}

// TestStartup_ParameterStatus verifies the server advertises the
// parameters libpq derives its encodings from. server_version ≥ 9.0
// makes PQescapeByteaConn emit hex-format bytea instead of legacy octal
// escapes; standard_conforming_strings=on stops libpq doubling
// backslashes in string literals (SQLite has no backslash escapes, so
// doubling corrupts stored values).
func TestStartup_ParameterStatus(t *testing.T) {
	addr, _ := startServer(t)
	params := readParameterStatuses(t, addr)

	if v := params["server_version"]; v != "16.0" {
		t.Errorf("server_version = %q, want %q", v, "16.0")
	}
	if v := params["standard_conforming_strings"]; v != "on" {
		t.Errorf("standard_conforming_strings = %q, want %q", v, "on")
	}
	if v := params["client_encoding"]; v != "UTF8" {
		t.Errorf("client_encoding = %q, want %q", v, "UTF8")
	}
	if v := params["server_encoding"]; v != "UTF8" {
		t.Errorf("server_encoding = %q, want %q", v, "UTF8")
	}
}

// blobServer starts a server whose schema has a BLOB column, mirroring
// the msilo silo.body column shape.
func blobServer(t *testing.T) (string, *memdb.DB) {
	t.Helper()
	addr, db := startServer(t)
	if _, err := db.Exec(`CREATE TABLE silo (id INTEGER PRIMARY KEY, body BLOB)`); err != nil {
		t.Fatal(err)
	}
	return addr, db
}

// TestSimpleQuery_BlobOutputIsHex verifies that a genuine BLOB value is
// rendered in PG's bytea hex output format on the text protocol. A libpq
// client passes every bytea result through PQunescapeBytea; raw blob
// bytes on the wire (the old behaviour) would be "unescaped" and any
// payload containing backslashes mangled.
func TestSimpleQuery_BlobOutputIsHex(t *testing.T) {
	addr, db := blobServer(t)
	if _, err := db.Exec(`INSERT INTO silo(id, body) VALUES (1, x'580a59')`); err != nil {
		t.Fatal(err)
	}

	conn := dialAndStartup(t, addr)
	sendQuery(t, conn, "SELECT body FROM silo WHERE id = 1")
	got := drainUntilReady(t, conn)

	wantTypes := "TDCZ"
	if string(got.types) != wantTypes {
		t.Fatalf("response types %q, want %q", got.types, wantTypes)
	}
	cells := decodeDataRow(t, got.bodies[1])
	if len(cells) != 1 || string(cells[0]) != `\x580a59` {
		t.Fatalf("blob cell = %q, want %q", cells[0], `\x580a59`)
	}
}

// TestSimpleQuery_HexLiteralRoundTrip mirrors the post-fix msilo write
// path: libpq (seeing server_version 16.0, standard_conforming_strings
// on) renders a bytea parameter as the string literal '\x…'. SQLite
// stores that text verbatim, and the SELECT must return it verbatim —
// the client's PQunescapeBytea then reproduces the original bytes. The
// storage representation IS the wire representation, so no server-side
// bytea parsing of simple-query literals is needed.
func TestSimpleQuery_HexLiteralRoundTrip(t *testing.T) {
	addr, _ := blobServer(t)

	conn := dialAndStartup(t, addr)
	sendQuery(t, conn, `INSERT INTO silo(id, body) VALUES (2, '\x0d0a5c')`)
	got := drainUntilReady(t, conn)
	if tag := trimTrailingNULTest(got.bodies[0]); tag != "INSERT 0 1" {
		t.Fatalf("CommandComplete = %q, want %q", tag, "INSERT 0 1")
	}

	sendQuery(t, conn, "SELECT body FROM silo WHERE id = 2")
	got = drainUntilReady(t, conn)
	if string(got.types) != "TDCZ" {
		t.Fatalf("response types %q, want %q", got.types, "TDCZ")
	}
	cells := decodeDataRow(t, got.bodies[1])
	if len(cells) != 1 || string(cells[0]) != `\x0d0a5c` {
		t.Fatalf("round-tripped cell = %q, want %q", cells[0], `\x0d0a5c`)
	}
}

// TestExtended_ByteaParamDecoded verifies that a text-format parameter
// declared bytea at Parse time (OID 17) is decoded from PG's hex input
// format to raw bytes before binding, so the stored value is the binary
// payload — not the literal "\x…" text.
func TestExtended_ByteaParamDecoded(t *testing.T) {
	addr, db := blobServer(t)
	conn := dialAndStartup(t, addr)

	// Parse: 1 param with OID 17 (bytea).
	parseBody := []byte{0}
	parseBody = append(parseBody, "INSERT INTO silo(id, body) VALUES (3, $1)"...)
	parseBody = append(parseBody, 0)
	parseBody = append(parseBody, 0, 1) // nParams=1
	parseBody = append(parseBody, 0, 0, 0, 17)
	send(t, conn, 'P', parseBody)

	// Bind: text format, value "\x580a59".
	val := `\x580a59`
	bindBody := []byte{0, 0}          // portal "", stmt ""
	bindBody = append(bindBody, 0, 0) // nFmt=0 (all text)
	bindBody = append(bindBody, 0, 1) // nParams=1
	bindBody = append(bindBody, 0, 0, 0, byte(len(val)))
	bindBody = append(bindBody, val...)
	bindBody = append(bindBody, 0, 0) // nResultFormats=0
	send(t, conn, 'B', bindBody)

	send(t, conn, 'E', []byte{0, 0, 0, 0, 0, 0})
	send(t, conn, 'S', nil)

	got := drainUntilReady(t, conn)
	wantTypes := "12CZ"
	if string(got.types) != wantTypes {
		t.Fatalf("response types %q, want %q", got.types, wantTypes)
	}
	if tag := trimTrailingNULTest(got.bodies[2]); tag != "INSERT 0 1" {
		t.Fatalf("CommandComplete = %q, want %q", tag, "INSERT 0 1")
	}

	// The stored value must be the raw 3 bytes, not the 8-char text.
	var n int
	var hexed string
	if err := db.QueryRow(`SELECT length(body), lower(hex(body)) FROM silo WHERE id = 3`).Scan(&n, &hexed); err != nil {
		t.Fatal(err)
	}
	if n != 3 || hexed != "580a59" {
		t.Fatalf("stored blob = len %d hex %q, want len 3 hex %q", n, hexed, "580a59")
	}
}

// TestExtended_ByteaParamInvalidInputErrors verifies malformed bytea
// input surfaces as an ErrorResponse, mirroring PG's byteain.
func TestExtended_ByteaParamInvalidInputErrors(t *testing.T) {
	addr, _ := blobServer(t)
	conn := dialAndStartup(t, addr)

	parseBody := []byte{0}
	parseBody = append(parseBody, "INSERT INTO silo(id, body) VALUES (4, $1)"...)
	parseBody = append(parseBody, 0)
	parseBody = append(parseBody, 0, 1)
	parseBody = append(parseBody, 0, 0, 0, 17)
	send(t, conn, 'P', parseBody)

	val := `\x5g` // invalid hex digit
	bindBody := []byte{0, 0}
	bindBody = append(bindBody, 0, 0)
	bindBody = append(bindBody, 0, 1)
	bindBody = append(bindBody, 0, 0, 0, byte(len(val)))
	bindBody = append(bindBody, val...)
	bindBody = append(bindBody, 0, 0)
	send(t, conn, 'B', bindBody)

	send(t, conn, 'S', nil)

	got := drainUntilReady(t, conn)
	// ParseComplete, then ErrorResponse from Bind, then ReadyForQuery.
	wantTypes := "1EZ"
	if string(got.types) != wantTypes {
		t.Fatalf("response types %q, want %q", got.types, wantTypes)
	}
}
