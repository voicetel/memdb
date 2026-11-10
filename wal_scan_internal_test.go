package memdb

// White-box tests for scanWAL's handling of read errors. A failing read says
// nothing about what the file holds, so it must surface as an error — never as
// a torn tail (which would truncate the log) or as corruption.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"syscall"
	"testing"

	"github.com/voicetel/memdb/replication"
)

// failAfterReader yields the first n bytes of data, then fails with err.
type failAfterReader struct {
	data []byte
	n    int
	err  error
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, r.err
	}
	if len(p) > r.n {
		p = p[:r.n]
	}
	c := copy(p, r.data)
	r.data, r.n = r.data[c:], r.n-c
	return c, nil
}

func scanTestRecord(t *testing.T, seq uint64) []byte {
	t.Helper()
	body, err := replication.EncodeEntry(nil, WALEntry{Seq: seq, SQL: "nop"})
	if err != nil {
		t.Fatal(err)
	}
	rec := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(rec, uint32(len(body)))
	return append(rec, body...)
}

func TestScanWAL_ReadErrorIsReturnedNotTreatedAsTornTail(t *testing.T) {
	rec := scanTestRecord(t, 1)
	two := append(append([]byte{}, rec...), scanTestRecord(t, 2)...)

	cases := []struct {
		name string
		n    int // bytes delivered before the read fails
	}{
		{"at a length prefix", len(rec)},
		{"inside a length prefix", len(rec) + 2},
		{"inside a record body", len(rec) + 4 + 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scanWAL(&failAfterReader{data: two, n: tc.n, err: syscall.EIO})
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("scanWAL error = %v, want EIO", err)
			}
		})
	}

	// While scanning a zero-filled region for the end of the file.
	zeros := append(append([]byte{}, rec...), make([]byte, 64)...)
	if _, err := scanWAL(&failAfterReader{data: zeros, n: len(rec) + 32, err: syscall.EIO}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("scanWAL error in a zero tail = %v, want EIO", err)
	}
}

// TestScanWAL_Classification pins the torn-tail / corruption split at the
// level the WAL acts on it.
func TestScanWAL_Classification(t *testing.T) {
	rec := scanTestRecord(t, 1)
	bad := scanTestRecord(t, 2)
	bad[len(bad)-1] ^= 0xFF // still a complete record; no longer decodes cleanly
	bad[4] ^= 0xFF

	cases := []struct {
		name    string
		tail    []byte
		corrupt bool
	}{
		{"clean end", nil, false},
		{"partial prefix", []byte{0, 0}, false},
		{"body past EOF", rec[:len(rec)-1], false},
		{"zero tail", make([]byte, 100), false},
		{"zero prefix then data", append([]byte{0, 0, 0, 0}, rec...), true},
		{"zeros then one stray byte", append(make([]byte, 100), 1), true},
		{"oversized length", []byte{0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3}, true},
		{"undecodable record", append(append([]byte{}, bad...), rec...), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scan, err := scanWAL(io.MultiReader(bytes.NewReader(rec), bytes.NewReader(tc.tail)))
			if err != nil {
				t.Fatal(err)
			}
			if len(scan.entries) != 1 || scan.validEnd != int64(len(rec)) {
				t.Errorf("entries=%d validEnd=%d, want 1 and %d", len(scan.entries), scan.validEnd, len(rec))
			}
			if (scan.corrupt != "") != tc.corrupt {
				t.Errorf("corrupt = %q, want corruption=%v", scan.corrupt, tc.corrupt)
			}
		})
	}
}
