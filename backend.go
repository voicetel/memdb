package memdb

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

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

	if err := copyMemToWriter(ctx, d, tmp, d.cfg.BackupStepPages); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot: %w", err)
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
	return nil
}

// restore copies the on-disk SQLite file into the in-memory DB.
func (b *LocalBackend) restore(ctx context.Context, d *DB) error {
	f, err := os.Open(b.Path)
	if err != nil {
		return fmt.Errorf("local backend: open for restore: %w", err)
	}
	defer f.Close()
	return copyReaderToMem(ctx, d, f, d.cfg.BackupStepPages)
}

// copyMemToWriter and copyReaderToMem are build-tag-specific helpers
// declared in backup.go (cgo) / backup_purego.go (purego).
// Signatures:
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

// flush serialises the in-memory DB to a byte stream and passes it to the
// external backend's Write method. The pipe allows streaming without buffering
// the entire database in memory.
func (a *externalBackendAdapter) flush(ctx context.Context, d *DB) error {
	pr, pw := io.Pipe()

	// Write the serialised DB into the pipe in a goroutine so that the
	// external backend's Write call (reading from pr) can proceed concurrently.
	var copyErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		copyErr = copyMemToWriter(ctx, d, pw, d.cfg.BackupStepPages)
		pw.CloseWithError(copyErr)
	}()

	writeErr := a.inner.Write(ctx, pr)
	// If Write returned early (error or context cancellation), close the read
	// end of the pipe so that the goroutine's copyMemToWriter call sees a
	// broken-pipe error and exits rather than blocking forever waiting for a
	// consumer.
	pr.CloseWithError(writeErr)
	<-done

	if copyErr != nil {
		return fmt.Errorf("external backend: serialize: %w", copyErr)
	}
	if writeErr != nil {
		return fmt.Errorf("external backend: write: %w", writeErr)
	}
	return nil
}

// restore reads a snapshot from the external backend and deserialises it into
// the in-memory DB.
func (a *externalBackendAdapter) restore(ctx context.Context, d *DB) error {
	rc, err := a.inner.Read(ctx)
	if err != nil {
		return fmt.Errorf("external backend: read: %w", err)
	}
	defer rc.Close()
	return copyReaderToMem(ctx, d, rc, d.cfg.BackupStepPages)
}

// Ensure the adapter satisfies the internal interface at compile time.
var _ Backend = (*externalBackendAdapter)(nil)
