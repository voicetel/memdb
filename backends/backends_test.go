package backends_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/voicetel/memdb/backends"
)

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

// failBackend always fails on Write and Read.
type failBackend struct{ err error }

func (f *failBackend) Exists(_ context.Context) (bool, error)        { return false, nil }
func (f *failBackend) Write(_ context.Context, _ io.Reader) error    { return f.err }
func (f *failBackend) Read(_ context.Context) (io.ReadCloser, error) { return nil, f.err }

// captureBackend stores every set of bytes passed to Write so callers can
// compare successive writes.
type captureBackend struct {
	data [][]byte
}

func (c *captureBackend) Exists(_ context.Context) (bool, error) {
	return len(c.data) > 0, nil
}

func (c *captureBackend) Write(_ context.Context, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	c.data = append(c.data, b)
	return nil
}

func (c *captureBackend) Read(_ context.Context) (io.ReadCloser, error) {
	if len(c.data) == 0 {
		return nil, errors.New("no data")
	}
	return io.NopCloser(bytes.NewReader(c.data[len(c.data)-1])), nil
}

// ---------------------------------------------------------------------------
// LocalBackend tests
// ---------------------------------------------------------------------------

func TestLocalBackend_ExistsNotFound(t *testing.T) {
	t.Parallel()
	b := &backends.LocalBackend{Path: filepath.Join(t.TempDir(), "nonexistent.db")}
	exists, err := b.Exists(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if exists {
		t.Fatal("expected false, got true")
	}
}

func TestLocalBackend_WriteAndExists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := &backends.LocalBackend{Path: filepath.Join(dir, "snap.db")}
	data := []byte("some data")

	if err := b.Write(context.Background(), bytes.NewReader(data)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	exists, err := b.Exists(context.Background())
	if err != nil {
		t.Fatalf("Exists returned error: %v", err)
	}
	if !exists {
		t.Fatal("expected true after write, got false")
	}
}

func TestLocalBackend_WriteAndRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := &backends.LocalBackend{Path: filepath.Join(dir, "snap.db")}
	want := []byte("hello sqlite")

	if err := b.Write(context.Background(), bytes.NewReader(want)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	rc, err := b.Read(context.Background())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("data mismatch: got %q, want %q", got, want)
	}
}

func TestLocalBackend_WriteIsAtomic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := &backends.LocalBackend{Path: filepath.Join(dir, "snap.db")}

	if err := b.Write(context.Background(), bytes.NewReader([]byte("atomic test"))); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Final file must exist.
	if _, err := os.Stat(b.Path); err != nil {
		t.Fatalf("final file not found: %v", err)
	}

	// No temp files should remain after a successful write.
	pattern := filepath.Join(dir, ".memdb-snap-*.db")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("Glob error: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temp files not cleaned up: %v", matches)
	}
}

func TestLocalBackend_ReadNotFound(t *testing.T) {
	t.Parallel()
	b := &backends.LocalBackend{Path: filepath.Join(t.TempDir(), "missing.db")}
	_, err := b.Read(context.Background())
	if err == nil {
		t.Fatal("expected error reading non-existent file, got nil")
	}
}

func TestLocalBackend_Write_ReaderError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "snap.db")
	b := &backends.LocalBackend{Path: target}

	// A reader that immediately returns an error.
	boom := errors.New("read boom")
	errReader := &errorReader{err: boom}

	err := b.Write(context.Background(), errReader)
	if err == nil {
		t.Fatal("expected error from Write with failing reader, got nil")
	}

	// The target path must not have been created (or must be empty/absent).
	info, statErr := os.Stat(target)
	if statErr == nil && info.Size() > 0 {
		t.Fatal("partial file left at target path after failed write")
	}

	// No temp files should remain either.
	matches, _ := filepath.Glob(filepath.Join(dir, ".memdb-snap-*.db"))
	if len(matches) != 0 {
		t.Fatalf("temp files not cleaned up after failed write: %v", matches)
	}
}

// errorReader is an io.Reader that always returns an error.
type errorReader struct{ err error }

func (e *errorReader) Read(_ []byte) (int, error) { return 0, e.err }

// ---------------------------------------------------------------------------
// CompressedBackend tests
// ---------------------------------------------------------------------------

func TestCompressedBackend_RoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inner := &backends.LocalBackend{Path: filepath.Join(dir, "snap.zst")}
	cb := &backends.CompressedBackend{Inner: inner}
	ctx := context.Background()

	want := []byte("the quick brown fox")

	if err := cb.Write(ctx, bytes.NewReader(want)); err != nil {
		t.Fatalf("CompressedBackend.Write failed: %v", err)
	}

	rc, err := cb.Read(ctx)
	if err != nil {
		t.Fatalf("CompressedBackend.Read failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %q, want %q", got, want)
	}
}

func TestCompressedBackend_ExistsDelegates(t *testing.T) {
	t.Parallel()
	inner := &backends.LocalBackend{Path: filepath.Join(t.TempDir(), "no-file.zst")}
	cb := &backends.CompressedBackend{Inner: inner}

	exists, err := cb.Exists(context.Background())
	if err != nil {
		t.Fatalf("Exists returned error: %v", err)
	}
	if exists {
		t.Fatal("expected false (inner has no file), got true")
	}
}

func TestCompressedBackend_Read_CorruptData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inner := &backends.LocalBackend{Path: filepath.Join(dir, "corrupt.zst")}
	ctx := context.Background()

	// Write raw, non-zstd bytes directly through the inner backend.
	if err := inner.Write(ctx, bytes.NewReader([]byte("not compressed"))); err != nil {
		t.Fatalf("inner.Write failed: %v", err)
	}

	cb := &backends.CompressedBackend{Inner: inner}
	rc, err := cb.Read(ctx)
	// The zstd decoder may surface the error either at Open or at first Read.
	if err != nil {
		// Error at open-time — acceptable.
		return
	}
	defer rc.Close()

	_, err = io.ReadAll(rc)
	if err == nil {
		t.Fatal("expected decompression error for corrupt data, got nil")
	}
}

func TestCompressedBackend_Write_InnerError(t *testing.T) {
	t.Parallel()
	boom := errors.New("inner write failed")
	cb := &backends.CompressedBackend{Inner: &failBackend{err: boom}}

	err := cb.Write(context.Background(), bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error from CompressedBackend.Write when inner fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// EncryptedBackend tests
// ---------------------------------------------------------------------------

func newTestKey() [32]byte {
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

func TestEncryptedBackend_RoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inner := &backends.LocalBackend{Path: filepath.Join(dir, "snap.enc")}
	eb := &backends.EncryptedBackend{Inner: inner, Key: newTestKey()}
	ctx := context.Background()

	want := []byte("secret data")

	if err := eb.Write(ctx, bytes.NewReader(want)); err != nil {
		t.Fatalf("EncryptedBackend.Write failed: %v", err)
	}

	rc, err := eb.Read(ctx)
	if err != nil {
		t.Fatalf("EncryptedBackend.Read failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %q, want %q", got, want)
	}
}

func TestEncryptedBackend_DifferentNonceEachWrite(t *testing.T) {
	t.Parallel()
	cap := &captureBackend{}
	eb := &backends.EncryptedBackend{Inner: cap, Key: newTestKey()}
	ctx := context.Background()

	plaintext := []byte("same plaintext")

	if err := eb.Write(ctx, bytes.NewReader(plaintext)); err != nil {
		t.Fatalf("first Write failed: %v", err)
	}
	if err := eb.Write(ctx, bytes.NewReader(plaintext)); err != nil {
		t.Fatalf("second Write failed: %v", err)
	}

	if len(cap.data) != 2 {
		t.Fatalf("expected 2 captured writes, got %d", len(cap.data))
	}
	if bytes.Equal(cap.data[0], cap.data[1]) {
		t.Fatal("expected different ciphertexts due to random nonce, but they are equal")
	}
}

func TestEncryptedBackend_ExistsDelegates(t *testing.T) {
	t.Parallel()
	inner := &backends.LocalBackend{Path: filepath.Join(t.TempDir(), "no-file.enc")}
	eb := &backends.EncryptedBackend{Inner: inner, Key: newTestKey()}

	exists, err := eb.Exists(context.Background())
	if err != nil {
		t.Fatalf("Exists returned error: %v", err)
	}
	if exists {
		t.Fatal("expected false (inner has no file), got true")
	}
}

func TestEncryptedBackend_Read_CiphertextTooShort(t *testing.T) {
	t.Parallel()
	// AES-GCM nonce size is 12 bytes; write fewer than that.
	short := []byte("tooshort") // 8 bytes
	inner := &captureBackend{}
	ctx := context.Background()
	if err := inner.Write(ctx, bytes.NewReader(short)); err != nil {
		t.Fatalf("captureBackend.Write failed: %v", err)
	}

	eb := &backends.EncryptedBackend{Inner: inner, Key: newTestKey()}
	_, err := eb.Read(ctx)
	if err == nil {
		t.Fatal("expected error for too-short ciphertext, got nil")
	}
	if !strings.Contains(err.Error(), "ciphertext too short") {
		t.Fatalf("expected 'ciphertext too short' in error, got: %v", err)
	}
}

func TestEncryptedBackend_Read_TamperedCiphertext(t *testing.T) {
	t.Parallel()
	cap := &captureBackend{}
	eb := &backends.EncryptedBackend{Inner: cap, Key: newTestKey()}
	ctx := context.Background()

	// Write valid data so cap.data[0] contains a valid ciphertext.
	if err := eb.Write(ctx, bytes.NewReader([]byte("tamper me"))); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Flip the last byte of the stored ciphertext.
	tampered := make([]byte, len(cap.data[0]))
	copy(tampered, cap.data[0])
	tampered[len(tampered)-1] ^= 0xFF
	cap.data[0] = tampered

	_, err := eb.Read(ctx)
	if err == nil {
		t.Fatal("expected error for tampered ciphertext, got nil")
	}
	if !strings.Contains(err.Error(), "decrypt") {
		t.Fatalf("expected 'decrypt' in error, got: %v", err)
	}
}

func TestEncryptedBackend_Write_ReadError(t *testing.T) {
	t.Parallel()
	boom := errors.New("read failed")
	eb := &backends.EncryptedBackend{Inner: &failBackend{err: boom}, Key: newTestKey()}

	_, err := eb.Read(context.Background())
	if err == nil {
		t.Fatal("expected error from EncryptedBackend.Read when inner fails, got nil")
	}
}

// ---------------------------------------------------------------------------
// Composed backend test
// ---------------------------------------------------------------------------

func TestCompressedEncrypted_RoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	local := &backends.LocalBackend{Path: filepath.Join(dir, "snap.enc.zst")}
	enc := &backends.EncryptedBackend{Inner: local, Key: newTestKey()}
	cb := &backends.CompressedBackend{Inner: enc}
	ctx := context.Background()

	want := []byte("compressed and encrypted round-trip data")

	if err := cb.Write(ctx, bytes.NewReader(want)); err != nil {
		t.Fatalf("composed Write failed: %v", err)
	}

	rc, err := cb.Read(ctx)
	if err != nil {
		t.Fatalf("composed Read failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("composed round-trip mismatch: got %q, want %q", got, want)
	}
}
