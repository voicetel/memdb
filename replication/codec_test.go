package replication_test

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/voicetel/memdb/replication"
)

// argEqual compares decoded args for equality, special-casing []byte and
// time.Time which are not == comparable / require Equal for proper semantics.
func argEqual(a, b any) bool {
	switch av := a.(type) {
	case []byte:
		bv, ok := b.([]byte)
		if !ok {
			return false
		}
		return bytes.Equal(av, bv)
	case time.Time:
		bv, ok := b.(time.Time)
		if !ok {
			return false
		}
		return av.Equal(bv)
	default:
		return a == b
	}
}

func TestEncodeDecode_RoundTrip_AllArgTypes(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)

	cases := []struct {
		name string
		in   []any
		want []any
	}{
		{"nil", []any{nil}, []any{nil}},
		{"string", []any{"hello"}, []any{"hello"}},
		{"empty-string", []any{""}, []any{""}},
		{"int64", []any{int64(-42)}, []any{int64(-42)}},
		{"int-widens", []any{int(7)}, []any{int64(7)}},
		{"int8-widens", []any{int8(-5)}, []any{int64(-5)}},
		{"int16-widens", []any{int16(-500)}, []any{int64(-500)}},
		{"int32-widens", []any{int32(-50000)}, []any{int64(-50000)}},
		{"uint64", []any{uint64(1 << 63)}, []any{uint64(1 << 63)}},
		{"uint-widens", []any{uint(42)}, []any{uint64(42)}},
		{"uint8-widens", []any{uint8(200)}, []any{uint64(200)}},
		{"uint16-widens", []any{uint16(60000)}, []any{uint64(60000)}},
		{"uint32-widens", []any{uint32(4_000_000_000)}, []any{uint64(4_000_000_000)}},
		{"float64", []any{math.Pi}, []any{math.Pi}},
		{"float32-widens", []any{float32(1.5)}, []any{float64(1.5)}},
		{"bool-true", []any{true}, []any{true}},
		{"bool-false", []any{false}, []any{false}},
		{"bytes", []any{[]byte("binary")}, []any{[]byte("binary")}},
		{"empty-bytes", []any{[]byte{}}, []any{[]byte{}}},
		{"time", []any{now}, []any{now}},
		{
			"mixed",
			[]any{"k", int64(1), 2.0, true, []byte{0xff}, now, nil},
			[]any{"k", int64(1), 2.0, true, []byte{0xff}, now, nil},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := replication.WALEntry{
				Seq:       42,
				Timestamp: now.UnixNano(),
				SQL:       "INSERT INTO t VALUES (?)",
				Args:      tc.in,
			}
			data, err := replication.EncodeEntry(nil, entry)
			if err != nil {
				t.Fatalf("EncodeEntry: %v", err)
			}
			if !replication.HasBinaryMagic(data) {
				t.Errorf("encoded payload missing magic prefix")
			}
			got, err := replication.DecodeEntry(data)
			if err != nil {
				t.Fatalf("DecodeEntry: %v", err)
			}
			if got.Seq != entry.Seq {
				t.Errorf("Seq=%d, want %d", got.Seq, entry.Seq)
			}
			if got.Timestamp != entry.Timestamp {
				t.Errorf("Timestamp=%d, want %d", got.Timestamp, entry.Timestamp)
			}
			if got.SQL != entry.SQL {
				t.Errorf("SQL=%q, want %q", got.SQL, entry.SQL)
			}
			if len(got.Args) != len(tc.want) {
				t.Fatalf("len(Args)=%d, want %d", len(got.Args), len(tc.want))
			}
			for i, w := range tc.want {
				if !argEqual(got.Args[i], w) {
					t.Errorf("Args[%d]=%#v (%T), want %#v (%T)",
						i, got.Args[i], got.Args[i], w, w)
				}
			}
		})
	}
}

func TestEncode_NoArgs(t *testing.T) {
	entry := replication.WALEntry{Seq: 1, Timestamp: 123, SQL: "DELETE FROM t"}
	data, err := replication.EncodeEntry(nil, entry)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	got, err := replication.DecodeEntry(data)
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	if len(got.Args) != 0 {
		t.Errorf("Args=%#v, want empty", got.Args)
	}
	if got.SQL != "DELETE FROM t" {
		t.Errorf("SQL=%q", got.SQL)
	}
}

func TestEncode_AppendsToExistingSlice(t *testing.T) {
	prefix := []byte{0xaa, 0xbb}
	entry := replication.WALEntry{Seq: 1, SQL: "x"}
	out, err := replication.EncodeEntry(prefix, entry)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	if len(out) <= len(prefix) {
		t.Fatalf("output not appended (len=%d)", len(out))
	}
	if !bytes.Equal(out[:2], prefix) {
		t.Errorf("prefix clobbered: %x", out[:2])
	}
	// The encoded entry should start at offset 2 (after prefix).
	if !replication.HasBinaryMagic(out[2:]) {
		t.Errorf("missing magic at offset 2")
	}
}

type unsupportedType struct{ X int }

func TestEncode_UnsupportedArgType(t *testing.T) {
	entry := replication.WALEntry{
		Seq:  1,
		SQL:  "x",
		Args: []any{unsupportedType{X: 7}},
	}
	_, err := replication.EncodeEntry(nil, entry)
	if err == nil {
		t.Fatal("expected error for unsupported arg type, got nil")
	}
	if !errors.Is(err, replication.ErrUnsupportedArgType) {
		t.Errorf("error chain missing ErrUnsupportedArgType: %v", err)
	}
}

func TestHasBinaryMagic(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want bool
	}{
		{"valid", []byte{'M', 'D', 'B', 'W', 1, 2, 3}, true},
		{"too-short", []byte{'M', 'D', 'B'}, false},
		{"empty", nil, false},
		{"wrong-magic", []byte{'X', 'X', 'X', 'X'}, false},
		{"gob-like", []byte{0x05, 0x10, 0x00, 0x00}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := replication.HasBinaryMagic(tc.in); got != tc.want {
				t.Errorf("HasBinaryMagic=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecode_MissingMagic(t *testing.T) {
	// Build a payload that lacks the magic prefix entirely.
	body := []byte{0xde, 0xad, 0xbe, 0xef, 1, 0, 0, 0, 0, 0, 0, 0, 0}
	_, err := replication.DecodeEntry(body)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestDecode_UnknownVersion(t *testing.T) {
	// Magic + version 99 + remaining zero bytes.
	body := []byte{'M', 'D', 'B', 'W', 99}
	_, err := replication.DecodeEntry(body)
	if err == nil {
		t.Fatal("expected error for unknown version, got nil")
	}
}

func TestDecode_TruncatedAtEachOffset(t *testing.T) {
	// Encode a known-good entry, then test truncation at every byte offset.
	good, err := replication.EncodeEntry(nil, replication.WALEntry{
		Seq:       7,
		Timestamp: 99,
		SQL:       "SELECT ?",
		Args:      []any{int64(123)},
	})
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	// Skip offset 0 (empty payload) — already covered by missing-magic test.
	// At every other offset shorter than the full payload, decoding must fail.
	for i := 1; i < len(good); i++ {
		_, err := replication.DecodeEntry(good[:i])
		if err == nil {
			t.Errorf("offset=%d: expected error, got nil", i)
		}
	}
}

func TestDecode_TrailingBytes(t *testing.T) {
	good, err := replication.EncodeEntry(nil, replication.WALEntry{Seq: 1, SQL: "x"})
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	bad := append(good, 0xff, 0xff, 0xff)
	_, err = replication.DecodeEntry(bad)
	if err == nil {
		t.Fatal("expected error for trailing bytes, got nil")
	}
}

func TestDecode_ImplausibleArgsCount(t *testing.T) {
	// Hand-craft a valid header but with a huge nArgs value (> 1<<20).
	// Layout: magic(4) + version(1) + seq(8) + ts(8) + sqlLen(4)=0 + nArgs(4)=0xffffffff
	body := []byte{
		'M', 'D', 'B', 'W',
		1,
		0, 0, 0, 0, 0, 0, 0, 1, // seq
		0, 0, 0, 0, 0, 0, 0, 0, // ts
		0, 0, 0, 0, // sqlLen
		0xff, 0xff, 0xff, 0xff, // nArgs (way too large)
	}
	_, err := replication.DecodeEntry(body)
	if err == nil {
		t.Fatal("expected error for implausible arg count, got nil")
	}
}

func TestDecode_BytesAreCopied(t *testing.T) {
	// The decoder defensively copies []byte args so they survive after the
	// source buffer is mutated. Verify by mutating the source post-decode.
	entry := replication.WALEntry{
		Seq:  1,
		SQL:  "x",
		Args: []any{[]byte("original")},
	}
	data, err := replication.EncodeEntry(nil, entry)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	got, err := replication.DecodeEntry(data)
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	// Wipe the source buffer.
	for i := range data {
		data[i] = 0
	}
	b, ok := got.Args[0].([]byte)
	if !ok {
		t.Fatalf("Args[0] type=%T, want []byte", got.Args[0])
	}
	if string(b) != "original" {
		t.Errorf("decoded bytes were aliased to source buffer: %q", b)
	}
}

func TestEncode_LargeString(t *testing.T) {
	// 2 MB string round-trip — exercises the 32-bit length prefix path
	// without crossing the math.MaxUint32 ceiling.
	big := make([]byte, 2<<20)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	entry := replication.WALEntry{
		Seq:  1,
		SQL:  "x",
		Args: []any{string(big)},
	}
	data, err := replication.EncodeEntry(nil, entry)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	got, err := replication.DecodeEntry(data)
	if err != nil {
		t.Fatalf("DecodeEntry: %v", err)
	}
	s, ok := got.Args[0].(string)
	if !ok {
		t.Fatalf("Args[0] type=%T, want string", got.Args[0])
	}
	if len(s) != len(big) || s[0] != 'a' || s[len(s)-1] != big[len(big)-1] {
		t.Errorf("round-trip failed: len=%d, first=%c, last=%c",
			len(s), s[0], s[len(s)-1])
	}
}

func TestDecode_UnknownArgTag(t *testing.T) {
	// Magic + version + seq + ts + sqlLen=0 + nArgs=1 + tag=99 (unknown).
	body := []byte{
		'M', 'D', 'B', 'W',
		1,
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 1,
		99, // unknown arg tag
	}
	_, err := replication.DecodeEntry(body)
	if err == nil {
		t.Fatal("expected error for unknown arg tag, got nil")
	}
}

func TestEncode_TimePreservesUnixNano(t *testing.T) {
	// Sub-second precision must survive the round-trip.
	want := time.Unix(1700000000, 123456789).UTC()
	entry := replication.WALEntry{Seq: 1, SQL: "x", Args: []any{want}}
	data, err := replication.EncodeEntry(nil, entry)
	if err != nil {
		t.Fatal(err)
	}
	got, err := replication.DecodeEntry(data)
	if err != nil {
		t.Fatal(err)
	}
	tv, ok := got.Args[0].(time.Time)
	if !ok {
		t.Fatalf("Args[0] type=%T, want time.Time", got.Args[0])
	}
	if !tv.Equal(want) {
		t.Errorf("time round-trip: got %v, want %v", tv, want)
	}
	if tv.UnixNano() != want.UnixNano() {
		t.Errorf("UnixNano: got %d, want %d", tv.UnixNano(), want.UnixNano())
	}
}
