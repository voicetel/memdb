package memdb_test

// Replay must tell a torn tail (what a crash mid-append leaves; cut silently)
// from corruption in the middle of the log (never deleted silently: moved to a
// sidecar file and reported).

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/voicetel/memdb"
)

// writeWALFile writes chunks back to back as the WAL file at path.
func writeWALFile(t *testing.T, path string, chunks ...[]byte) {
	t.Helper()
	if err := os.WriteFile(path, bytes.Join(chunks, nil), 0o600); err != nil {
		t.Fatal(err)
	}
}

// walSidecars returns the quarantine sidecar files next to walPath.
func walSidecars(t *testing.T, walPath string) []string {
	t.Helper()
	m, err := filepath.Glob(walPath + ".corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// replayWAL opens the WAL at path, replays it, and returns the sequence
// numbers seen plus the quarantine the replay reported.
func replayWAL(t *testing.T, path string) ([]uint64, *memdb.WALQuarantine) {
	t.Helper()
	w, err := memdb.OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	var seqs []uint64
	if err := w.Replay(func(e memdb.WALEntry) error { seqs = append(seqs, e.Seq); return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return seqs, w.Quarantine()
}

// undecodableWALRecord is a complete record (its length prefix matches its
// body) whose body is not a binary v1 entry.
func undecodableWALRecord(t *testing.T) []byte {
	t.Helper()
	rec := rawWALEntry(t, 4, `INSERT INTO kv VALUES (?, ?)`, 4, "v")
	rec[4] ^= 0xFF // first body byte: the "MDBW" magic
	return rec
}

// TestWAL_MidFileCorruption_Quarantined puts a corrupt record between valid
// ones. The valid prefix is replayed; the corrupt record and everything after
// it are moved, byte for byte, to a sidecar; the WAL is cut back to the prefix
// so later appends are reachable; and the replay reports what it did. Before
// the fix the three records after the corruption were silently deleted.
func TestWAL_MidFileCorruption_Quarantined(t *testing.T) {
	cases := []struct {
		name   string
		bad    func(t *testing.T) []byte
		reason string
	}{
		{"undecodable record", undecodableWALRecord, "does not decode"},
		{"zero length with data after", func(*testing.T) []byte { return []byte{0, 0, 0, 0} }, "zero length"},
		{"length above the record limit", func(*testing.T) []byte { return []byte{0xFF, 0xFF, 0xFF, 0xFF} }, "exceeds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			walPath := filepath.Join(t.TempDir(), "w.wal")
			var prefix, tail []byte
			for i := 1; i <= 3; i++ {
				prefix = append(prefix, rawWALEntry(t, uint64(i), `INSERT INTO kv VALUES (?, ?)`, i, "v")...)
			}
			tail = append(tail, tc.bad(t)...)
			for i := 5; i <= 7; i++ {
				tail = append(tail, rawWALEntry(t, uint64(i), `INSERT INTO kv VALUES (?, ?)`, i, "v")...)
			}
			writeWALFile(t, walPath, prefix, tail)

			seqs, q := replayWAL(t, walPath)
			if len(seqs) != 3 || seqs[0] != 1 || seqs[2] != 3 {
				t.Fatalf("replayed %v, want the valid prefix [1 2 3]", seqs)
			}
			if q == nil {
				t.Fatal("Quarantine() = nil after replaying a WAL with mid-file corruption")
			}
			if q.Offset != int64(len(prefix)) || q.Bytes != int64(len(tail)) {
				t.Errorf("quarantine offset=%d bytes=%d, want offset=%d bytes=%d",
					q.Offset, q.Bytes, len(prefix), len(tail))
			}
			if !strings.Contains(q.Reason, tc.reason) {
				t.Errorf("quarantine reason = %q, want it to mention %q", q.Reason, tc.reason)
			}

			// The sidecar holds exactly the discarded bytes, mode 0600.
			sidecars := walSidecars(t, walPath)
			if len(sidecars) != 1 || sidecars[0] != q.Path {
				t.Fatalf("sidecars = %v, want exactly [%s]", sidecars, q.Path)
			}
			got, err := os.ReadFile(q.Path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tail) {
				t.Errorf("sidecar holds %d bytes that differ from the %d discarded bytes", len(got), len(tail))
			}
			if fi, err := os.Stat(q.Path); err != nil || fi.Mode().Perm() != 0o600 {
				t.Errorf("sidecar mode = %v, %v; want 0600", fi.Mode().Perm(), err)
			}

			// The WAL is cut back to the valid prefix.
			if onDisk, err := os.ReadFile(walPath); err != nil || !bytes.Equal(onDisk, prefix) {
				t.Errorf("WAL after replay is %d bytes, %v; want the %d-byte valid prefix", len(onDisk), err, len(prefix))
			}

			// A later append lands right after the prefix, and the next
			// replay is clean: no new quarantine, no second sidecar.
			w, err := memdb.OpenWAL(walPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Append(memdb.WALEntry{Seq: 8, SQL: "nop"}); err != nil {
				t.Fatal(err)
			}
			_ = w.Close()
			seqs, q = replayWAL(t, walPath)
			if len(seqs) != 4 || seqs[3] != 8 {
				t.Errorf("second replay saw %v, want [1 2 3 8]", seqs)
			}
			if q != nil {
				t.Errorf("second replay reported a quarantine: %+v", q)
			}
			if n := len(walSidecars(t, walPath)); n != 1 {
				t.Errorf("%d sidecars after a clean replay, want 1", n)
			}
		})
	}
}

// TestWAL_TornTail_TruncatedWithoutSidecar covers what a crash mid-append
// leaves behind. Each is cut off silently: no sidecar, no quarantine report.
func TestWAL_TornTail_TruncatedWithoutSidecar(t *testing.T) {
	whole := rawWALEntry(t, 4, `INSERT INTO kv VALUES (?, ?)`, 4, "v")
	cases := []struct {
		name string
		tail []byte
	}{
		{"partial length prefix", whole[:3]},
		{"body runs past EOF", whole[:len(whole)-1]},
		{"length prefix with no body", whole[:4]},
		{"zero-filled tail", make([]byte, 4096)},
		{"zero-filled tail of exactly one prefix", make([]byte, 4)},
		{"zero-filled tail shorter than a prefix", make([]byte, 2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			walPath := filepath.Join(t.TempDir(), "w.wal")
			var prefix []byte
			for i := 1; i <= 3; i++ {
				prefix = append(prefix, rawWALEntry(t, uint64(i), `INSERT INTO kv VALUES (?, ?)`, i, "v")...)
			}
			writeWALFile(t, walPath, prefix, tc.tail)

			seqs, q := replayWAL(t, walPath)
			if len(seqs) != 3 {
				t.Fatalf("replayed %v, want [1 2 3]", seqs)
			}
			if q != nil {
				t.Errorf("a torn tail was reported as corruption: %+v", q)
			}
			if s := walSidecars(t, walPath); len(s) != 0 {
				t.Errorf("a torn tail produced sidecar files: %v", s)
			}
			if onDisk, err := os.ReadFile(walPath); err != nil || !bytes.Equal(onDisk, prefix) {
				t.Errorf("WAL after replay is %d bytes, %v; want the %d-byte valid prefix", len(onDisk), err, len(prefix))
			}
		})
	}
}

// TestWAL_QuarantineFailure_LeavesWALUntouched makes the sidecar impossible to
// create. Replay must fail rather than delete the corrupt region, and the WAL
// must be byte-identical afterwards.
func TestWAL_QuarantineFailure_LeavesWALUntouched(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "w.wal")
	content := bytes.Join([][]byte{
		rawWALEntry(t, 1, "nop"),
		undecodableWALRecord(t),
		rawWALEntry(t, 3, "nop"),
	}, nil)
	writeWALFile(t, walPath, content)

	// The WAL itself stays writable; only creating a new file in dir fails.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	w, err := memdb.OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Replay(func(memdb.WALEntry) error { return nil }); err == nil {
		t.Fatal("Replay succeeded although the corrupt region could not be quarantined")
	}
	if q := w.Quarantine(); q != nil {
		t.Errorf("Quarantine() = %+v after a failed quarantine, want nil", q)
	}
	if onDisk, err := os.ReadFile(walPath); err != nil || !bytes.Equal(onDisk, content) {
		t.Errorf("WAL was modified although the quarantine failed (%d bytes, %v; want %d)", len(onDisk), err, len(content))
	}
}

// TestOpen_WALCorruption_ReplaysPrefixAndLogsQuarantine drives the same case
// through Open: the database comes up with the valid prefix applied, and the
// quarantine is logged at Error level with the sidecar path, offset and size.
func TestOpen_WALCorruption_ReplaysPrefixAndLogsQuarantine(t *testing.T) {
	cfg := walTestConfig(t)
	walPath := cfg.FilePath + ".wal"

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2"} {
		if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, k, "v"); err != nil {
			t.Fatal(err)
		}
	}
	valid, err := os.ReadFile(walPath) // CREATE TABLE, k1, k2
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Crash before any snapshot: only the WAL survives, now with a corrupt
	// record followed by one more valid write.
	if err := os.Remove(cfg.FilePath); err != nil {
		t.Fatal(err)
	}
	tail := append(undecodableWALRecord(t),
		rawWALEntry(t, 99, `INSERT INTO kv (key, value) VALUES (?, ?)`, "k3", "v")...)
	writeWALFile(t, walPath, valid, tail)

	var logs bytes.Buffer
	cfg.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))
	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open with mid-file WAL corruption: %v", err)
	}
	defer func() { _ = db2.Close() }()

	for _, k := range []string{"k1", "k2"} {
		var v string
		if err := db2.QueryRow(`SELECT value FROM kv WHERE key = ?`, k).Scan(&v); err != nil || v != "v" {
			t.Errorf("%s = %q, %v; the valid prefix was not replayed", k, v, err)
		}
	}
	var n int
	if err := db2.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM kv WHERE key = 'k3'`).Scan(&n); err != nil || n != 0 {
		t.Errorf("k3 count = %d, %v; entries after the corruption must not be applied", n, err)
	}

	sidecars := walSidecars(t, walPath)
	if len(sidecars) != 1 {
		t.Fatalf("sidecars = %v, want exactly one", sidecars)
	}
	if got, err := os.ReadFile(sidecars[0]); err != nil || !bytes.Equal(got, tail) {
		t.Errorf("sidecar holds %d bytes, %v; want the %d discarded bytes", len(got), err, len(tail))
	}
	out := logs.String()
	for _, want := range []string{
		`"level":"ERROR"`,
		"WAL corruption quarantined",
		`"sidecar":"` + sidecars[0] + `"`,
		`"offset":` + strconv.Itoa(len(valid)),
		`"bytes":` + strconv.Itoa(len(tail)),
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output lacks %s\n%s", want, out)
		}
	}
}
