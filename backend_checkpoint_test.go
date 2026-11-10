package memdb_test

// The WAL checkpoint for authenticated backends (which skip the MDBK wrap),
// and rejection of snapshot format versions this build does not know.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/backends"
)

var sqliteMagic = []byte("SQLite format 3\x00")

func encryptedWALConfig(t *testing.T) (memdb.Config, *backends.EncryptedBackend) {
	t.Helper()
	cfg := walTestConfig(t)
	enc := &backends.EncryptedBackend{
		Inner: &backends.LocalBackend{Path: filepath.Join(filepath.Dir(cfg.FilePath), "snap.enc")},
	}
	for i := range enc.Key {
		enc.Key[i] = byte(i + 1)
	}
	cfg.Backend = memdb.WrapBackend(enc)
	return cfg, enc
}

// TestEncryptedBackend_WAL_CrashBetweenSnapshotAndTruncate reproduces the
// crash window with EncryptedBackend: the snapshot is written, and the WAL
// still holds the entries it contains. Without a checkpoint in the encrypted
// snapshot, reopen replayed them over the snapshot — the INSERTs hit the
// UNIQUE constraint and Open failed (and an UPDATE would have applied twice).
func TestEncryptedBackend_WAL_CrashBetweenSnapshotAndTruncate(t *testing.T) {
	cfg, enc := encryptedWALConfig(t)
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
	// Not idempotent: applying it twice is visible in the value.
	if _, err := db.Exec(`UPDATE kv SET value = value || '+' WHERE key = 'k1'`); err != nil {
		t.Fatal(err)
	}
	walBeforeFlush, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The crash: the snapshot made it, the truncate did not.
	if err := os.WriteFile(walPath, walBeforeFlush, 0o600); err != nil {
		t.Fatal(err)
	}

	db2, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("reopen after a crash between the encrypted snapshot and the WAL truncate: %v", err)
	}
	want := map[string]string{"k1": "v+", "k2": "v"}
	for k, w := range want {
		var v string
		if err := db2.QueryRow(`SELECT value FROM kv WHERE key = ?`, k).Scan(&v); err != nil || v != w {
			t.Errorf("%s = %q, %v; want %q (entries the snapshot holds must not be re-applied)", k, v, err, w)
		}
	}

	// A write after the recovery must get a sequence number above the
	// checkpoint, or the next recovery would skip it. Crash again before that
	// write is snapshotted: same snapshot, WAL now also holding the new entry.
	snapPath := enc.Inner.(*backends.LocalBackend).Path
	snapshot, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db2.Exec(`INSERT INTO kv (key, value) VALUES ('k3', 'v')`); err != nil {
		t.Fatal(err)
	}
	walAfterWrite, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapPath, snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, walAfterWrite, 0o600); err != nil {
		t.Fatal(err)
	}

	db3, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	defer func() { _ = db3.Close() }()
	want["k3"] = "v"
	for k, w := range want {
		var v string
		if err := db3.QueryRow(`SELECT value FROM kv WHERE key = ?`, k).Scan(&v); err != nil || v != w {
			t.Errorf("after the second recovery %s = %q, %v; want %q", k, v, err, w)
		}
	}
}

// TestEncryptedBackend_LegacyHeaderlessSnapshotRestores: an encrypted
// snapshot written before the checkpoint prefix existed is raw SQLite bytes
// inside the ciphertext. It must still load, with checkpoint 0 — so every WAL
// entry is replayed, as it always was for such snapshots.
func TestEncryptedBackend_LegacyHeaderlessSnapshotRestores(t *testing.T) {
	// Raw SQLite bytes holding one row.
	src := mustOpen(t, extraTestConfig(t))
	if _, err := src.Exec(`INSERT INTO kv (key, value) VALUES ('legacy', 'row')`); err != nil {
		t.Fatal(err)
	}
	raw, err := src.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, sqliteMagic) {
		t.Fatal("Serialize did not return raw SQLite bytes")
	}

	cfg, enc := encryptedWALConfig(t)
	if err := enc.Write(context.Background(), bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	// A WAL entry with a low sequence number: checkpoint 0 means it replays.
	w, err := memdb.OpenWAL(cfg.FilePath + ".wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(memdb.WALEntry{Seq: 1, SQL: `INSERT INTO kv (key, value) VALUES (?, ?)`, Args: []any{"wal", "row"}}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("Open on a legacy headerless encrypted snapshot: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, k := range []string{"legacy", "wal"} {
		var v string
		if err := db.QueryRow(`SELECT value FROM kv WHERE key = ?`, k).Scan(&v); err != nil || v != "row" {
			t.Errorf("%s = %q, %v", k, v, err)
		}
	}
}

// authCaptureBackend is an in-memory ExternalBackend that claims to
// authenticate its payload, exposing the plaintext stream memdb hands it.
type authCaptureBackend struct {
	mu   sync.Mutex
	data []byte
}

func (b *authCaptureBackend) Authenticated() bool { return true }

func (b *authCaptureBackend) Exists(context.Context) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data != nil, nil
}

func (b *authCaptureBackend) Write(_ context.Context, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = data
	return nil
}

func (b *authCaptureBackend) Read(context.Context) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

func (b *authCaptureBackend) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...)
}

// TestAuthenticatedBackend_StreamLayout pins the bytes an authenticated
// backend receives: the 16-byte checkpoint prefix, then the raw SQLite
// database, and nothing after it — no SHA-256 footer.
func TestAuthenticatedBackend_StreamLayout(t *testing.T) {
	backend := &authCaptureBackend{}
	cfg := walTestConfig(t)
	cfg.Backend = memdb.WrapBackend(backend)
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	// InitSchema's CREATE TABLE is seq 1; these are 2 and 3.
	for _, k := range []string{"k1", "k2"} {
		if _, err := db.Exec(`INSERT INTO kv (key, value) VALUES (?, ?)`, k, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := backend.bytes()
	if len(got) < 16+100 {
		t.Fatalf("stream is only %d bytes", len(got))
	}
	if string(got[0:4]) != "MDBC" {
		t.Errorf("magic = %q, want MDBC", got[0:4])
	}
	if v := binary.BigEndian.Uint32(got[4:8]); v != 1 {
		t.Errorf("prefix version = %d, want 1", v)
	}
	if seq := binary.BigEndian.Uint64(got[8:16]); seq != 3 {
		t.Errorf("checkpoint = %d, want 3 (CREATE TABLE + 2 INSERTs)", seq)
	}
	payload := got[16:]
	if !bytes.HasPrefix(payload, sqliteMagic) {
		t.Fatalf("payload does not start with the SQLite header: %q", payload[:16])
	}
	// A SQLite file is a whole number of pages; a 40-byte footer would break that.
	pageSize := int(binary.BigEndian.Uint16(payload[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if len(payload)%pageSize != 0 {
		t.Errorf("payload is %d bytes, not a multiple of the %d-byte page: something follows the database", len(payload), pageSize)
	}
}

// TestUnwrapSnapshot_CheckpointPrefix: external tooling handed an
// authenticated backend's plaintext gets the database with the prefix
// stripped, flagged as carrying no MDBK integrity wrap.
func TestUnwrapSnapshot_CheckpointPrefix(t *testing.T) {
	payload := append(append([]byte{}, sqliteMagic...), bytes.Repeat([]byte{0xAB}, 5000)...)
	stream := append([]byte("MDBC\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x2A"), payload...)

	var out bytes.Buffer
	isLegacy, err := memdb.UnwrapSnapshot(bytes.NewReader(stream), &out)
	if err != nil {
		t.Fatal(err)
	}
	if !isLegacy {
		t.Error("isLegacy = false; a checkpoint-prefixed stream has no MDBK integrity wrap")
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Errorf("unwrapped %d bytes, want the %d-byte payload without the prefix", out.Len(), len(payload))
	}

	// Truncated inside the prefix.
	if _, err := memdb.UnwrapSnapshot(bytes.NewReader(stream[:10]), io.Discard); !errors.Is(err, memdb.ErrSnapshotCorrupt) {
		t.Errorf("truncated prefix: err = %v, want ErrSnapshotCorrupt", err)
	}
}

// TestSnapshot_UnknownVersion_Rejected: a snapshot whose envelope names a
// version newer than this build knows must be refused with ErrSnapshotVersion,
// not parsed as if it were the newest known version.
func TestSnapshot_UnknownVersion_Rejected(t *testing.T) {
	t.Run("MDBK envelope via Open", func(t *testing.T) {
		cfg, path := writeSnapshotForCorruption(t)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if v := binary.BigEndian.Uint32(data[4:8]); v != 2 {
			t.Fatalf("current envelope version = %d; update this test for the new format", v)
		}
		binary.BigEndian.PutUint32(data[4:8], 3)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err = memdb.Open(cfg)
		if !errors.Is(err, memdb.ErrSnapshotVersion) {
			t.Fatalf("Open = %v, want ErrSnapshotVersion", err)
		}
		if errors.Is(err, memdb.ErrSnapshotCorrupt) {
			t.Errorf("Open = %v; a newer version is not corruption", err)
		}
	})

	t.Run("MDBK envelope via UnwrapSnapshot", func(t *testing.T) {
		stream := []byte("MDBK\xFF\xFF\xFF\xFF")
		stream = append(stream, make([]byte, 200)...)
		var out bytes.Buffer
		if _, err := memdb.UnwrapSnapshot(bytes.NewReader(stream), &out); !errors.Is(err, memdb.ErrSnapshotVersion) {
			t.Fatalf("UnwrapSnapshot = %v, want ErrSnapshotVersion", err)
		}
		if out.Len() != 0 {
			t.Errorf("%d payload bytes were emitted from a snapshot of unknown version", out.Len())
		}
	})

	t.Run("checkpoint prefix", func(t *testing.T) {
		for _, version := range []uint32{0, 2} {
			stream := []byte("MDBC")
			stream = binary.BigEndian.AppendUint32(stream, version)
			stream = binary.BigEndian.AppendUint64(stream, 7)
			stream = append(stream, sqliteMagic...)
			if _, err := memdb.UnwrapSnapshot(bytes.NewReader(stream), io.Discard); !errors.Is(err, memdb.ErrSnapshotVersion) {
				t.Errorf("version %d: UnwrapSnapshot = %v, want ErrSnapshotVersion", version, err)
			}
		}
	})
}
