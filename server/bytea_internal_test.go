package server

// White-box unit tests for the bytea text codecs: decodeByteaText (the
// Bind-time input decoder mirroring PostgreSQL's byteain) and
// appendCellText (the text-format result encoder that hex-encodes BLOB
// cells). The wire-level behaviour is exercised by the integration tests
// in server_test.go / server_extended_test.go; this file exercises every
// branch of the codecs directly.

import (
	"bytes"
	"testing"
)

func TestDecodeByteaText_HexFormat(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []byte
	}{
		{"empty", `\x`, []byte{}},
		{"lower", `\x580a59`, []byte{'X', 0x0A, 'Y'}},
		{"upper-digits", `\x580A59`, []byte{'X', 0x0A, 'Y'}},
		{"whitespace-between-pairs", `\x58 0a	59`, []byte{'X', 0x0A, 'Y'}},
		{"crlf-bytes", `\x0d0a`, []byte{0x0D, 0x0A}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeByteaText([]byte(tc.in))
			if err != nil {
				t.Fatalf("decodeByteaText(%q): %v", tc.in, err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("decodeByteaText(%q) = %x, want %x", tc.in, got, tc.want)
			}
		})
	}
}

func TestDecodeByteaText_HexFormat_Errors(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"odd-digits", `\x580`},
		{"invalid-digit", `\x5g`},
		{"whitespace-inside-pair", `\x5 8`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeByteaText([]byte(tc.in)); err == nil {
				t.Errorf("decodeByteaText(%q) succeeded, want error", tc.in)
			}
		})
	}
}

func TestDecodeByteaText_EscapeFormat(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []byte
	}{
		{"plain", "hello", []byte("hello")},
		{"empty", "", []byte{}},
		{"octal-lf", `X\012Y`, []byte{'X', 0x0A, 'Y'}},
		{"octal-cr-lf", `\015\012`, []byte{0x0D, 0x0A}},
		{"double-backslash", `a\\b`, []byte(`a\b`)},
		{"octal-max", `\377`, []byte{0xFF}},
		{"octal-zero", `\000`, []byte{0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeByteaText([]byte(tc.in))
			if err != nil {
				t.Fatalf("decodeByteaText(%q): %v", tc.in, err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("decodeByteaText(%q) = %x, want %x", tc.in, got, tc.want)
			}
		})
	}
}

func TestDecodeByteaText_EscapeFormat_Errors(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"trailing-backslash", `abc\`},
		{"short-octal", `\01`},
		{"first-digit-too-big", `\412`},
		{"non-octal-digit", `\098`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeByteaText([]byte(tc.in)); err == nil {
				t.Errorf("decodeByteaText(%q) succeeded, want error", tc.in)
			}
		})
	}
}

func TestAppendCellText_ByteaHex(t *testing.T) {
	got := appendCellText(nil, []byte{0x0D, 0x0A, 0xFF})
	if string(got) != `\x0d0aff` {
		t.Errorf("appendCellText([]byte) = %q, want %q", got, `\x0d0aff`)
	}

	// Appends after existing content without disturbing it.
	got = appendCellText([]byte("pre:"), []byte{0x41})
	if string(got) != `pre:\x41` {
		t.Errorf("appendCellText with prefix = %q, want %q", got, `pre:\x41`)
	}
}

func TestAppendCellText_NonByteaMatchesAppendCell(t *testing.T) {
	for _, v := range []any{"text", int64(42), 3.5, true} {
		want := appendCell(nil, v)
		got := appendCellText(nil, v)
		if !bytes.Equal(got, want) {
			t.Errorf("appendCellText(%T) = %q, want appendCell result %q", v, got, want)
		}
	}
}
