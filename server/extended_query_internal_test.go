package server

// White-box unit tests for the package-private helpers in extended_query.go.
// These cover the OID inference and binary-cell encoding tables that the
// integration tests cannot easily probe (every binary OID branch, every
// fallback path, and every nil/unknown-type case). The integration tests
// in server_extended_test.go exercise the wire protocol; this file
// exercises the codecs.

import (
	"bytes"
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/voicetel/memdb"
)

func TestPgOIDFromGoValue_AllBranches(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want uint32
	}{
		{"int64", int64(7), 20},
		{"int", int(7), 20},
		{"int32", int32(7), 20},
		{"float64", 1.5, 701},
		{"float32", float32(1.5), 701},
		{"bool", true, 16},
		{"bytes", []byte{1, 2}, 17},
		{"string", "x", 25},
		{"nil", nil, 25},
		{"unknown", struct{}{}, 25},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pgOIDFromGoValue(tc.v); got != tc.want {
				t.Errorf("pgOIDFromGoValue(%T)=%d, want %d", tc.v, got, tc.want)
			}
		})
	}
}

func TestEncodeCellBinary_Int8(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want int64
	}{
		{"int64", int64(0x1122334455667788), 0x1122334455667788},
		{"int", int(42), 42},
		{"bytes-of-digits", []byte("12345"), 12345},
		{"string-of-digits", "67890", 67890},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := encodeCellBinary(nil, 20, tc.v)
			if len(out) != 8 {
				t.Fatalf("len=%d, want 8", len(out))
			}
			if got := int64(binary.BigEndian.Uint64(out)); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestEncodeCellBinary_Int8_FallbackOnUnparseable(t *testing.T) {
	// Non-numeric string should fall back to text encoding (appendCell).
	got := encodeCellBinary(nil, 20, "not-a-number")
	want := appendCell(nil, "not-a-number")
	if !bytes.Equal(got, want) {
		t.Errorf("fallback differs: got %q, want %q", got, want)
	}
}

func TestEncodeCellBinary_Int4(t *testing.T) {
	out := encodeCellBinary(nil, 23, int64(-1))
	if len(out) != 4 {
		t.Fatalf("len=%d, want 4", len(out))
	}
	if binary.BigEndian.Uint32(out) != 0xffffffff {
		t.Errorf("int4(-1) bits=%x", binary.BigEndian.Uint32(out))
	}
	out = encodeCellBinary(nil, 23, int(99))
	if got := int32(binary.BigEndian.Uint32(out)); got != 99 {
		t.Errorf("int4 int=99: got %d", got)
	}
	// Unknown source type → text fallback.
	out = encodeCellBinary(nil, 23, "x")
	if bytes.Equal(out, []byte{0, 0, 0, 0}) {
		t.Errorf("unexpected zero buffer for fallback: %x", out)
	}
}

func TestEncodeCellBinary_Float8(t *testing.T) {
	out := encodeCellBinary(nil, 701, math.Pi)
	if len(out) != 8 {
		t.Fatalf("len=%d, want 8", len(out))
	}
	got := math.Float64frombits(binary.BigEndian.Uint64(out))
	if got != math.Pi {
		t.Errorf("got %v, want pi", got)
	}
	// int64 → float8 widening branch.
	out = encodeCellBinary(nil, 701, int64(7))
	got = math.Float64frombits(binary.BigEndian.Uint64(out))
	if got != 7.0 {
		t.Errorf("int64→float8: got %v", got)
	}
	// Unknown source → fallback.
	out = encodeCellBinary(nil, 701, "x")
	if len(out) == 8 && bytes.Equal(out, make([]byte, 8)) {
		t.Errorf("expected text fallback for string→float8")
	}
}

func TestEncodeCellBinary_Bool(t *testing.T) {
	out := encodeCellBinary(nil, 16, true)
	if !bytes.Equal(out, []byte{1}) {
		t.Errorf("bool true: %x", out)
	}
	out = encodeCellBinary(nil, 16, false)
	if !bytes.Equal(out, []byte{0}) {
		t.Errorf("bool false: %x", out)
	}
	// SQLite returns int64 for BOOLEAN columns.
	out = encodeCellBinary(nil, 16, int64(1))
	if !bytes.Equal(out, []byte{1}) {
		t.Errorf("int64(1) as bool: %x", out)
	}
	out = encodeCellBinary(nil, 16, int64(0))
	if !bytes.Equal(out, []byte{0}) {
		t.Errorf("int64(0) as bool: %x", out)
	}
}

func TestEncodeCellBinary_Bytea(t *testing.T) {
	in := []byte{0xde, 0xad, 0xbe, 0xef}
	out := encodeCellBinary(nil, 17, in)
	if !bytes.Equal(out, in) {
		t.Errorf("bytea: %x", out)
	}
	// String source — wire-identical for bytea (both are raw).
	out = encodeCellBinary(nil, 17, "hello")
	if string(out) != "hello" {
		t.Errorf("bytea from string: %q", out)
	}
}

func TestEncodeCellBinary_Text(t *testing.T) {
	out := encodeCellBinary(nil, 25, "hello")
	want := appendCell(nil, "hello")
	if !bytes.Equal(out, want) {
		t.Errorf("text branch should equal appendCell output")
	}
}

func TestEncodeCellBinary_UnknownOIDFallsThrough(t *testing.T) {
	out := encodeCellBinary(nil, 9999, int64(42))
	want := appendCell(nil, int64(42))
	if !bytes.Equal(out, want) {
		t.Errorf("unknown OID should fall through to text")
	}
}

func TestEncodeCellBinary_AppendsToExistingDst(t *testing.T) {
	prefix := []byte{0xff, 0xff}
	out := encodeCellBinary(prefix, 20, int64(1))
	if !bytes.Equal(out[:2], prefix) {
		t.Errorf("prefix clobbered: %x", out[:2])
	}
	if len(out) != 10 {
		t.Errorf("len=%d, want 10 (2 prefix + 8 int8)", len(out))
	}
}

func TestCountDollarPlaceholders(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want int
	}{
		{"none", "SELECT 1", 0},
		{"one", "SELECT $1", 1},
		{"two-different", "SELECT $1, $2", 2},
		{"reused-counts-max", "SELECT $1, $1, $2", 2},
		{"out-of-order", "SELECT $5", 5},
		{"in-single-quotes", "SELECT '$1'", 0},
		{"in-double-quotes", `SELECT "$1"`, 0},
		{"in-line-comment", "SELECT 1 -- $1", 0},
		{"after-line-comment", "SELECT 1 -- $1\nFROM x WHERE y=$2", 2},
		{"in-block-comment", "SELECT /* $1 */ 1", 0},
		{"after-block-comment", "SELECT /* $9 */ $1", 1},
		{"escaped-quote", "SELECT '''$1'''", 0},
		{"weird-no-num", "SELECT $abc", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := countDollarPlaceholders(tc.sql); got != tc.want {
				t.Errorf("countDollarPlaceholders(%q)=%d, want %d",
					tc.sql, got, tc.want)
			}
		})
	}
}

// fakeResult is a sql.Result that returns a configurable RowsAffected
// (and an optional error) so commandTagFor's error path can be tested
// without touching a real driver.
type fakeResult struct {
	rows int64
	err  error
}

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) { return r.rows, r.err }

func TestCommandTagFor(t *testing.T) {
	cases := []struct {
		verb    string
		rows    int64
		want    string
		wantErr bool
	}{
		{"INSERT", 3, "INSERT 0 3", false},
		{"INSERT", 0, "INSERT 0 0", false},
		{"UPDATE", 5, "UPDATE 5", false},
		{"DELETE", 1, "DELETE 1", false},
		{"CREATE", 0, "CREATE", false},
		{"DROP", 0, "DROP", false},
		{"VACUUM", 0, "VACUUM", false},
	}
	for _, tc := range cases {
		t.Run(tc.verb, func(t *testing.T) {
			tag, err := commandTagFor(tc.verb, fakeResult{rows: tc.rows})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, tc.wantErr)
			}
			if tag != tc.want {
				t.Errorf("tag=%q, want %q", tag, tc.want)
			}
		})
	}
}

func TestPeekColumns_HappyPath(t *testing.T) {
	// peekColumns runs the SQL with NULL bound to every placeholder and
	// reports the result columns. We use a real in-memory memdb so the
	// queryRunner contract is exercised end-to-end.
	dir := t.TempDir()
	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(dir, "x.db"),
		FlushInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.ExecDirect("CREATE TABLE kv (k TEXT, v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	cols, colTypes, err := peekColumns(db, "SELECT k, v FROM kv WHERE k = $1", 1)
	if err != nil {
		t.Fatalf("peekColumns: %v", err)
	}
	if len(cols) != 2 || cols[0] != "k" || cols[1] != "v" {
		t.Errorf("cols=%v", cols)
	}
	if len(colTypes) != 2 {
		t.Errorf("colTypes len=%d, want 2", len(colTypes))
	}
}

func TestPeekColumns_BadSQLReturnsError(t *testing.T) {
	dir := t.TempDir()
	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(dir, "y.db"),
		FlushInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, _, err := peekColumns(db, "SELECT FROM no_such_table", 0); err == nil {
		t.Error("expected error for invalid SQL, got nil")
	}
}

func TestReadCString(t *testing.T) {
	cases := []struct {
		name    string
		body    []byte
		pos     int
		want    string
		wantPos int
		wantErr bool
	}{
		{"basic", []byte("hello\x00world"), 0, "hello", 6, false},
		{"empty", []byte("\x00rest"), 0, "", 1, false},
		{"midbuffer", []byte("xxxhello\x00rest"), 3, "hello", 9, false},
		{"missing-nul", []byte("noterminator"), 0, "", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotPos, err := readCString(tc.body, tc.pos)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want || gotPos != tc.wantPos {
				t.Errorf("got=%q,%d want=%q,%d", got, gotPos, tc.want, tc.wantPos)
			}
		})
	}
}
