package memdb

// Guards the two flush payload paths in copyMemToWriter: the sqlite3_serialize
// fast path taken by small DBs and the streaming backup-API path taken by DBs
// above serializeFlushMaxBytes. Every other small-DB test now exercises only
// the serialize path, so this pins the streaming path and the size gate, and
// asserts both produce restorable images with identical logical content.

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestFlushSerializeAndBackupPathsAgree(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	src, err := Open(Config{
		FilePath:      filepath.Join(dir, "src.db"),
		FlushInterval: -1,
		InitSchema: func(db *DB) error {
			if _, err := db.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)`); err != nil {
				return err
			}
			for i := 0; i < 50; i++ {
				if _, err := db.Exec(`INSERT INTO kv VALUES (?, ?)`,
					fmt.Sprintf("k%02d", i), fmt.Sprintf("value-%d", i)); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()

	// The gate must classify this DB as small enough for the serialize path.
	size, ok := src.approxBytes(ctx)
	if !ok {
		t.Fatal("approxBytes reported !ok for a live DB")
	}
	if size <= 0 {
		t.Fatalf("approxBytes = %d, want > 0", size)
	}
	if size > serializeFlushMaxBytes {
		t.Fatalf("test DB size %d exceeds serializeFlushMaxBytes %d; cannot exercise the serialize gate",
			size, serializeFlushMaxBytes)
	}

	var serBuf, bakBuf bytes.Buffer
	if err := copyMemToWriter(ctx, src, &serBuf, -1); err != nil {
		t.Fatalf("copyMemToWriter (serialize path): %v", err)
	}
	if err := copyMemToWriterViaBackup(ctx, src, &bakBuf, -1); err != nil {
		t.Fatalf("copyMemToWriterViaBackup (streaming path): %v", err)
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"serialize", serBuf.Bytes()},
		{"backup", bakBuf.Bytes()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.HasPrefix(tc.data, sqliteFileHeader) {
				t.Fatalf("payload missing SQLite file header (got %q)", tc.data[:min(16, len(tc.data))])
			}
			dst, err := Open(Config{
				FilePath:      filepath.Join(dir, tc.name+".db"),
				FlushInterval: -1,
			})
			if err != nil {
				t.Fatalf("open dst: %v", err)
			}
			defer func() { _ = dst.Close() }()

			if err := dst.Restore(tc.data); err != nil {
				t.Fatalf("restore: %v", err)
			}
			var count int
			if err := dst.QueryRow(`SELECT count(*) FROM kv`).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 50 {
				t.Fatalf("got %d rows, want 50", count)
			}
			var v string
			if err := dst.QueryRow(`SELECT value FROM kv WHERE key = 'k07'`).Scan(&v); err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if v != "value-7" {
				t.Fatalf("got %q, want %q", v, "value-7")
			}
		})
	}
}
