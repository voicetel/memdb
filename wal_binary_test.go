package memdb_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/voicetel/memdb"
)

// This file exercises the binary WAL wire format introduced to replace
// encoding/gob on the hot Append path. The gob encoder was measured at
// ~25% of total CPU under DurabilityWAL pprof traces before the change.
//
// Test coverage:
//
//   - TestWAL_Binary_RoundTrip_AllArgTypes  — every registered arg type
//     survives Append→Replay without loss of value. Types that widen on
//     decode (int8→int64, float32→float64) are checked for the widened
//     value rather than the original Go type.
//
//   - TestWAL_Binary_EmptyArgs              — entries with no bound
//     parameters round-trip as nil/empty Args.
//
//   - TestWAL_Binary_NilArg                 — a nil any in Args round-trips
//     as nil (walArgNil tag).
//
//   - TestWAL_Binary_LargeString            — a multi-MB string fits and
//     round-trips.
//
//   - TestWAL_Binary_LargePayload_Rejected  — an entry that would exceed
//     the 64 MB record cap is rejected with a descriptive error.
//
//   - TestWAL_Binary_UnsupportedArgType     — a Go type the encoder does
//     not recognise surfaces ErrWALUnsupportedArgType so the caller can
//     reject the offending write.
//
//   - TestWAL_Binary_TruncatedTail_StopsCleanly — a record with a valid
//     length prefix but a truncated body aborts Replay at exactly that
//     record; all prior records are still returned.
//
//   - TestWAL_Binary_CorruptVersion         — a record prefixed with the
//     binary magic but an unknown version tag is treated as corrupt
//     (Replay stops); it does not propagate garbage back to the caller.

// helperWAL opens a fresh WAL in a temp dir and registers cleanup.
func helperWAL(t *testing.T) (*memdb.WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.bin")
	wal, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return wal, path
}

// collectReplay replays all entries from wal into a slice. Used to check
// round-trip behaviour on a freshly-reopened file.
func collectReplay(t *testing.T, wal *memdb.WAL) []memdb.WALEntry {
	t.Helper()
	var got []memdb.WALEntry
	if err := wal.Replay(func(e memdb.WALEntry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return got
}

// TestWAL_Binary_RoundTrip_AllArgTypes appends one entry per supported
// arg type and verifies every field survives Replay. Types that widen on
// decode (int8/16/32 → int64, uint8/16/32/uint → uint64, float32 →
// float64) are checked for the widened value rather than the original.
func TestWAL_Binary_RoundTrip_AllArgTypes(t *testing.T) {
	wal, path := helperWAL(t)

	now := time.Now().UTC().Truncate(time.Nanosecond)

	cases := []struct {
		name string
		in   []any
		want []any
	}{
		{"string", []any{"hello"}, []any{"hello"}},
		{"empty-string", []any{""}, []any{""}},
		{"int64", []any{int64(-42)}, []any{int64(-42)}},
		{"int-widens-to-int64", []any{int(7)}, []any{int64(7)}},
		{"int8-widens-to-int64", []any{int8(-5)}, []any{int64(-5)}},
		{"int16-widens-to-int64", []any{int16(-500)}, []any{int64(-500)}},
		{"int32-widens-to-int64", []any{int32(-50000)}, []any{int64(-50000)}},
		{"uint64", []any{uint64(1 << 63)}, []any{uint64(1 << 63)}},
		{"uint-widens-to-uint64", []any{uint(42)}, []any{uint64(42)}},
		{"uint8-widens-to-uint64", []any{uint8(200)}, []any{uint64(200)}},
		{"uint16-widens-to-uint64", []any{uint16(60000)}, []any{uint64(60000)}},
		{"uint32-widens-to-uint64", []any{uint32(4_000_000_000)}, []any{uint64(4_000_000_000)}},
		{"float64", []any{float64(math.Pi)}, []any{float64(math.Pi)}},
		{"float32-widens-to-float64", []any{float32(1.5)}, []any{float64(1.5)}},
		{"bool-true", []any{true}, []any{true}},
		{"bool-false", []any{false}, []any{false}},
		{"bytes", []any{[]byte("binary")}, []any{[]byte("binary")}},
		{"empty-bytes", []any{[]byte{}}, []any{[]byte{}}},
		{"time-utc", []any{now}, []any{now}},
		{"mixed", []any{"key", int64(123), 4.5, true, []byte{0xde, 0xad}, now},
			[]any{"key", int64(123), 4.5, true, []byte{0xde, 0xad}, now}},
	}

	// Append every case first, so one Replay covers all of them.
	seq := uint64(0)
	for _, tc := range cases {
		seq++
		e := memdb.WALEntry{
			Seq:       seq,
			Timestamp: now.UnixNano(),
			SQL:       "INSERT INTO t VALUES (?)",
			Args:      tc.in,
		}
		if err := wal.Append(e); err != nil {
			t.Fatalf("%s: Append: %v", tc.name, err)
		}
	}

	// Close and reopen to guarantee we are really reading off disk, not
	// from any in-memory state the WAL might hold.
	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wal2, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = wal2.Close() })

	got := collectReplay(t, wal2)
	if len(got) != len(cases) {
		t.Fatalf("got %d entries, want %d", len(got), len(cases))
	}

	for i, tc := range cases {
		e := got[i]
		if e.SQL != "INSERT INTO t VALUES (?)" {
			t.Errorf("%s: SQL=%q", tc.name, e.SQL)
		}
		if e.Seq != uint64(i+1) {
			t.Errorf("%s: Seq=%d, want %d", tc.name, e.Seq, i+1)
		}
		if e.Timestamp != now.UnixNano() {
			t.Errorf("%s: Timestamp=%d, want %d", tc.name, e.Timestamp, now.UnixNano())
		}
		if len(e.Args) != len(tc.want) {
			t.Errorf("%s: len(Args)=%d, want %d", tc.name, len(e.Args), len(tc.want))
			continue
		}
		for j, want := range tc.want {
			if !argEqual(e.Args[j], want) {
				t.Errorf("%s: Args[%d] = %#v (%T), want %#v (%T)",
					tc.name, j, e.Args[j], e.Args[j], want, want)
			}
		}
	}
}

// argEqual compares two decoded argument values for equality, special-
// casing []byte (which is not comparable with ==) and time.Time.
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

// TestWAL_Binary_EmptyArgs verifies an entry with no args round-trips
// with a nil/empty Args slice.
func TestWAL_Binary_EmptyArgs(t *testing.T) {
	wal, path := helperWAL(t)

	e := memdb.WALEntry{
		Seq:       1,
		Timestamp: time.Now().UnixNano(),
		SQL:       "DELETE FROM t",
	}
	if err := wal.Append(e); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = wal.Close()

	wal2, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = wal2.Close() })

	got := collectReplay(t, wal2)
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if len(got[0].Args) != 0 {
		t.Errorf("Args=%#v, want empty", got[0].Args)
	}
	if got[0].SQL != "DELETE FROM t" {
		t.Errorf("SQL=%q", got[0].SQL)
	}
}

// TestWAL_Binary_NilArg verifies a nil any in Args round-trips as nil.
func TestWAL_Binary_NilArg(t *testing.T) {
	wal, path := helperWAL(t)

	e := memdb.WALEntry{
		Seq:       1,
		Timestamp: time.Now().UnixNano(),
		SQL:       "INSERT INTO t VALUES (?)",
		Args:      []any{nil},
	}
	if err := wal.Append(e); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = wal.Close()

	wal2, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = wal2.Close() })

	got := collectReplay(t, wal2)
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if len(got[0].Args) != 1 {
		t.Fatalf("len(Args)=%d, want 1", len(got[0].Args))
	}
	if got[0].Args[0] != nil {
		t.Errorf("Args[0]=%#v, want nil", got[0].Args[0])
	}
}

// TestWAL_Binary_LargeString pushes a multi-MB string through the codec
// to prove the length-prefixed string path handles non-trivial payloads.
func TestWAL_Binary_LargeString(t *testing.T) {
	wal, path := helperWAL(t)

	// 2 MB of repeating ASCII so we can verify every byte round-trips.
	big := make([]byte, 2<<20)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	want := string(big)

	e := memdb.WALEntry{
		Seq:       1,
		Timestamp: time.Now().UnixNano(),
		SQL:       "INSERT INTO t VALUES (?)",
		Args:      []any{want},
	}
	if err := wal.Append(e); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = wal.Close()

	wal2, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = wal2.Close() })

	got := collectReplay(t, wal2)
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	s, ok := got[0].Args[0].(string)
	if !ok {
		t.Fatalf("Args[0] type = %T, want string", got[0].Args[0])
	}
	if s != want {
		t.Errorf("round-trip mismatch at len=%d (first diff at %d)",
			len(s), firstDiff(s, want))
	}
}

// firstDiff reports the first byte index at which two strings differ, or
// min(len(a), len(b)) if one is a prefix of the other.
func firstDiff(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// TestWAL_Binary_LargePayload_Rejected verifies a record larger than the
// 64 MB cap is rejected with a descriptive error rather than silently
// truncated or written.
func TestWAL_Binary_LargePayload_Rejected(t *testing.T) {
	wal, _ := helperWAL(t)

	// 65 MB string → record body slightly larger than 64 MB.
	big := make([]byte, 65<<20)
	for i := range big {
		big[i] = 'x'
	}

	e := memdb.WALEntry{
		Seq:       1,
		Timestamp: time.Now().UnixNano(),
		SQL:       "INSERT INTO t VALUES (?)",
		Args:      []any{string(big)},
	}
	err := wal.Append(e)
	if err == nil {
		t.Fatal("expected error for oversized entry, got nil")
	}
	// The error should mention the size so operators can diagnose.
	msg := err.Error()
	if !bytesContainsAny(msg, "too large", "too long") {
		t.Errorf("error message should describe size: %v", err)
	}
}

func bytesContainsAny(haystack string, needles ...string) bool {
	for _, n := range needles {
		if bytesContains(haystack, n) {
			return true
		}
	}
	return false
}

func bytesContains(s, substr string) bool {
	return len(substr) == 0 || len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// unsupportedArgType is a type the binary encoder should not know about.
type unsupportedArgType struct{ X int }

// TestWAL_Binary_UnsupportedArgType verifies Append rejects args of an
// unknown Go type with ErrWALUnsupportedArgType so the caller can reject
// the offending write cleanly.
func TestWAL_Binary_UnsupportedArgType(t *testing.T) {
	wal, _ := helperWAL(t)

	e := memdb.WALEntry{
		Seq:       1,
		Timestamp: time.Now().UnixNano(),
		SQL:       "INSERT INTO t VALUES (?)",
		Args:      []any{unsupportedArgType{X: 42}},
	}
	err := wal.Append(e)
	if err == nil {
		t.Fatal("expected error for unsupported arg type, got nil")
	}
	if !errors.Is(err, memdb.ErrWALUnsupportedArgType) {
		t.Errorf("want ErrWALUnsupportedArgType in chain, got: %v", err)
	}
}

// TestWAL_Binary_TruncatedTail_StopsCleanly appends two valid records,
// then writes a third header whose body is truncated. Replay must return
// the first two records and stop — not error, because a partial tail is
// the normal consequence of an unclean shutdown.
func TestWAL_Binary_TruncatedTail_StopsCleanly(t *testing.T) {
	wal, path := helperWAL(t)

	for i := uint64(1); i <= 2; i++ {
		e := memdb.WALEntry{
			Seq:       i,
			Timestamp: time.Now().UnixNano(),
			SQL:       "INSERT INTO t VALUES (?)",
			Args:      []any{int64(i)},
		}
		if err := wal.Append(e); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a header that claims a 100-byte body but only writes 10.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 100)
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("truncated!")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	wal2, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = wal2.Close() })

	got := collectReplay(t, wal2)
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2 (truncated tail must be skipped)", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Errorf("entry %d Seq=%d", i, e.Seq)
		}
	}
}

// TestWAL_Binary_CorruptVersion writes a record prefixed with the binary
// magic but carrying an unknown version tag. Replay must stop at that
// record and return every prior valid entry — it must not misinterpret
// the unknown-version bytes as a later v1 record.
func TestWAL_Binary_CorruptVersion(t *testing.T) {
	wal, path := helperWAL(t)

	if err := wal.Append(memdb.WALEntry{
		Seq:       1,
		Timestamp: time.Now().UnixNano(),
		SQL:       "INSERT INTO t VALUES (?)",
		Args:      []any{int64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Craft a record with magic "MDBW", version 99, then some garbage.
	body := []byte{'M', 'D', 'B', 'W', 99, 0, 0, 0, 0, 0, 0, 0, 0}
	record := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(record[:4], uint32(len(body)))
	copy(record[4:], body)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(record); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	wal2, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = wal2.Close() })

	got := collectReplay(t, wal2)
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1 (unknown-version record must be skipped)", len(got))
	}
	if got[0].Seq != 1 {
		t.Errorf("Seq=%d, want 1", got[0].Seq)
	}
}

// TestWAL_Binary_NextSeqUpdated verifies that after Replay the WAL's
// internal sequence counter is advanced past the highest Seq seen on
// disk, so subsequent Append calls produce strictly increasing seqs.
func TestWAL_Binary_NextSeqUpdated(t *testing.T) {
	wal, path := helperWAL(t)

	// Append three entries with explicit seqs so we know the expected
	// post-replay value.
	for i := uint64(1); i <= 3; i++ {
		if err := wal.Append(memdb.WALEntry{
			Seq:       i,
			Timestamp: time.Now().UnixNano(),
			SQL:       "x",
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = wal.Close()

	wal2, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal2.Close() })

	if err := wal2.Replay(func(memdb.WALEntry) error { return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// After replay the next Seq must be >= 4.
	next := wal2.NextSeq()
	if next < 4 {
		t.Errorf("NextSeq after replay = %d, want >= 4", next)
	}
}
