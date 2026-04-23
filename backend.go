package memdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ── Snapshot checksum format ──────────────────────────────────────────────────
//
// Every snapshot written by memdb is prefixed with a fixed 40-byte header so
// that a corrupt or truncated file is detected at restore time rather than
// silently loading bad data into memory.
//
// Header layout (all fields big-endian):
//
//	[4]byte  magic   = snapMagic  ("MDBK")
//	[4]byte  version = 1          (uint32)
//	[32]byte sha256  = SHA-256 of the raw SQLite bytes that follow
//
// The header is always written atomically with the snapshot bytes in a single
// temp-file write — the rename(2) that makes the snapshot visible only happens
// after both the header and the payload are flushed.
//
// Legacy snapshots written before this header was introduced do NOT start with
// "MDBK". restore() detects this and loads them without verification, logging a
// warning. This preserves backward compatibility with existing files.
// Any future flush of a legacy snapshot will upgrade it to the new format.

var snapMagic = [4]byte{'M', 'D', 'B', 'K'}

const snapHeaderLen = 4 + 4 + 32 // magic(4) + version(4) + sha256(32) = 40 bytes

// wrapSnapshotWriter returns an io.Writer that buffers all written bytes in
// memory, then — when Finish is called — prepends the checksum header and
// writes the complete (header + payload) stream to dst.
//
// This two-phase approach is necessary because SHA-256 of the payload must be
// known before the header can be written, and the SQLite backup API streams
// bytes incrementally, so we must buffer the payload.
type snapshotWriter struct {
	buf bytes.Buffer
}

// Write satisfies io.Writer; all bytes are accumulated in the internal buffer.
func (s *snapshotWriter) Write(p []byte) (int, error) { return s.buf.Write(p) }

// Finish writes [header][payload] to dst and returns any write error.
// After Finish the snapshotWriter should not be used again.
func (s *snapshotWriter) Finish(dst io.Writer) error {
	payload := s.buf.Bytes()
	sum := sha256.Sum256(payload)

	// Build the 40-byte header.
	var hdr [snapHeaderLen]byte
	copy(hdr[0:4], snapMagic[:])
	binary.BigEndian.PutUint32(hdr[4:8], 1) // version
	copy(hdr[8:40], sum[:])

	if _, err := dst.Write(hdr[:]); err != nil {
		return fmt.Errorf("snapshot: write header: %w", err)
	}
	if _, err := dst.Write(payload); err != nil {
		return fmt.Errorf("snapshot: write payload: %w", err)
	}
	return nil
}

// verifyAndStrip reads the snapshot from r, verifies the SHA-256 checksum in
// the header, and returns a reader over the raw SQLite payload bytes.
//
// If r does not begin with snapMagic, the data is assumed to be a legacy
// (pre-checksum) snapshot; it is returned unchanged and isLegacy == true.
// The caller may choose to log a warning in this case.
//
// Returns ErrSnapshotCorrupt when the checksum header is present but the
// digest does not match.
func verifyAndStrip(r io.Reader) (payload io.Reader, isLegacy bool, err error) {
	// Read the first 4 bytes to check for the magic.
	var magicBuf [4]byte
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		// File is smaller than 4 bytes — could be legacy empty or corrupt.
		// Return ErrSnapshotCorrupt so the caller can decide.
		return nil, false, fmt.Errorf("%w: too short to read magic", ErrSnapshotCorrupt)
	}

	if magicBuf != snapMagic {
		// Legacy snapshot: reassemble the full stream and return without check.
		return io.MultiReader(bytes.NewReader(magicBuf[:]), r), true, nil
	}

	// Read the rest of the header (version + sha256 = 36 bytes).
	var rest [snapHeaderLen - 4]byte
	if _, err := io.ReadFull(r, rest[:]); err != nil {
		return nil, false, fmt.Errorf("%w: truncated header", ErrSnapshotCorrupt)
	}
	// version := binary.BigEndian.Uint32(rest[0:4]) — reserved for future use.
	var storedSum [32]byte
	copy(storedSum[:], rest[4:36])

	// Read the full payload to verify the digest.
	rawPayload, err := io.ReadAll(r)
	if err != nil {
		return nil, false, fmt.Errorf("%w: read payload: %v", ErrSnapshotCorrupt, err)
	}

	computed := sha256.Sum256(rawPayload)
	if computed != storedSum {
		return nil, false, fmt.Errorf("%w: SHA-256 mismatch (expected %x, got %x)",
			ErrSnapshotCorrupt, storedSum, computed)
	}

	return bytes.NewReader(rawPayload), false, nil
}

// Backend is the interface that storage backends must implement.
// External implementations must use WrapBackend(ExternalBackend) — the
// flush and restore methods are unexported and only implementable within
// this package. See ExternalBackend for the public interface.
// Use the backends sub-package for pre-built implementations (compressed,
// encrypted, or custom). LocalBackend below is the default file-system backend.
type Backend interface {
	// Exists reports whether a snapshot already exists in the backend.
	Exists(ctx context.Context) (bool, error)

	// flush writes the current in-memory DB state to the backend.
	// Called by Flush; the d argument is the owning DB.
	flush(ctx context.Context, d *DB) error

	// restore reads a snapshot from the backend into the memory DB.
	restore(ctx context.Context, d *DB) error
}

// LocalBackend persists snapshots to the local filesystem using an atomic
// write-then-rename pattern so the on-disk file is always a valid SQLite DB.
type LocalBackend struct {
	Path string
}

func (b *LocalBackend) Exists(_ context.Context) (bool, error) {
	_, err := os.Stat(b.Path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

// flush copies the in-memory DB to a temp file then renames it into place.
func (b *LocalBackend) flush(ctx context.Context, d *DB) error {
	dir := filepath.Dir(b.Path)
	tmp, err := os.CreateTemp(dir, ".memdb-snap-*.db")
	if err != nil {
		return fmt.Errorf("local backend: create temp: %w", err)
	}
	tmpName := tmp.Name()

	// Buffer the SQLite bytes so we can compute SHA-256 before writing.
	var sw snapshotWriter
	if err := copyMemToWriter(ctx, d, &sw, d.cfg.BackupStepPages); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot: %w", err)
	}
	// Write [header][payload] to the temp file.
	if err := sw.Finish(tmp); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: write checksum header: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: fsync: %w", err)
	}
	tmp.Close()

	if err := os.Rename(tmpName, b.Path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("local backend: rename: %w", err)
	}
	// fsync the parent directory so the rename is durable.
	if dir, err := os.Open(filepath.Dir(b.Path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

// restore copies the on-disk SQLite file into the in-memory DB.
func (b *LocalBackend) restore(ctx context.Context, d *DB) error {
	f, err := os.Open(b.Path)
	if err != nil {
		return fmt.Errorf("local backend: open for restore: %w", err)
	}
	defer f.Close()

	payload, isLegacy, err := verifyAndStrip(f)
	if err != nil {
		return fmt.Errorf("local backend: %w", err)
	}
	if isLegacy {
		d.logger().Warn("memdb: snapshot has no integrity checksum (legacy format); " +
			"the next flush will upgrade it to the checksummed format")
	}
	return copyReaderToMem(ctx, d, payload, d.cfg.BackupStepPages)
}

// copyMemToWriter and copyReaderToMem are declared in backup.go alongside
// the rest of the SQLite Online Backup API plumbing. Signatures:
//
//	func copyMemToWriter(ctx context.Context, d *DB, w io.Writer, stepPages int) error
//	func copyReaderToMem(ctx context.Context, d *DB, r io.Reader, stepPages int) error

// Ensure the interface is satisfied at compile time.
var _ Backend = (*LocalBackend)(nil)

// ── ExternalBackend adapter ───────────────────────────────────────────────────

// ExternalBackend is the interface that third-party backends (CompressedBackend,
// EncryptedBackend, and custom implementations) must satisfy. It is the
// public-facing subset of memdb.Backend: callers supply a stream of bytes rather
// than interacting with the DB internals directly.
//
// All backends in the backends/ sub-package implement this interface.
type ExternalBackend interface {
	Exists(ctx context.Context) (bool, error)
	Write(ctx context.Context, r io.Reader) error
	Read(ctx context.Context) (io.ReadCloser, error)
}

// WrapBackend adapts any ExternalBackend so it can be used as Config.Backend.
// The adapter serialises the in-memory DB to bytes (via copyMemToWriter) before
// calling Write, and deserialises bytes back into memory (via copyReaderToMem)
// when restoring — bridging the public stream-oriented interface with the
// internal backup-API-based one.
//
// Usage:
//
//	cfg.Backend = memdb.WrapBackend(&backends.CompressedBackend{
//	    Inner: &backends.LocalBackend{Path: "/var/lib/app/data.db.zst"},
//	})
func WrapBackend(eb ExternalBackend) Backend {
	return &externalBackendAdapter{inner: eb}
}

type externalBackendAdapter struct {
	inner ExternalBackend
}

func (a *externalBackendAdapter) Exists(ctx context.Context) (bool, error) {
	return a.inner.Exists(ctx)
}

// flush serialises the in-memory DB to a byte stream, prepends the SHA-256
// checksum header, and passes the complete (header + payload) stream to the
// external backend's Write method.
//
// We buffer the payload in memory so that SHA-256 can be computed before the
// header is written. For very large databases this trades memory for integrity
// guarantees; operators with multi-GiB datasets may prefer to accept the
// memory cost or switch to the local backend which writes to a temp file.
func (a *externalBackendAdapter) flush(ctx context.Context, d *DB) error {
	// Step 1: buffer the raw SQLite bytes.
	var sw snapshotWriter
	if err := copyMemToWriter(ctx, d, &sw, d.cfg.BackupStepPages); err != nil {
		return fmt.Errorf("external backend: serialize: %w", err)
	}

	// Step 2: build [header][payload] in a second buffer, then stream it
	// to the external backend via a pipe so the backend's Write call can
	// proceed as the data flows.
	pr, pw := io.Pipe()

	var finishErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		finishErr = sw.Finish(pw)
		pw.CloseWithError(finishErr)
	}()

	writeErr := a.inner.Write(ctx, pr)
	pr.CloseWithError(writeErr)
	<-done

	if finishErr != nil {
		return fmt.Errorf("external backend: write checksum header: %w", finishErr)
	}
	if writeErr != nil {
		return fmt.Errorf("external backend: write: %w", writeErr)
	}
	return nil
}

// restore reads a snapshot from the external backend, verifies its SHA-256
// checksum, and deserialises the payload into the in-memory DB.
func (a *externalBackendAdapter) restore(ctx context.Context, d *DB) error {
	rc, err := a.inner.Read(ctx)
	if err != nil {
		return fmt.Errorf("external backend: read: %w", err)
	}
	defer rc.Close()

	payload, isLegacy, err := verifyAndStrip(rc)
	if err != nil {
		return fmt.Errorf("external backend: %w", err)
	}
	if isLegacy {
		d.logger().Warn("memdb: snapshot has no integrity checksum (legacy format); " +
			"the next flush will upgrade it to the checksummed format")
	}
	return copyReaderToMem(ctx, d, payload, d.cfg.BackupStepPages)
}

// Ensure the adapter satisfies the internal interface at compile time.
var _ Backend = (*externalBackendAdapter)(nil)
