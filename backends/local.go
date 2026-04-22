package backends

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// LocalBackend persists snapshots to the local filesystem using an atomic
// write-then-rename pattern. The file on disk is always a valid SQLite DB.
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

func (b *LocalBackend) Write(ctx context.Context, r io.Reader) error {
	dir := filepath.Dir(b.Path)
	tmp, err := os.CreateTemp(dir, ".memdb-snap-*.db")
	if err != nil {
		return fmt.Errorf("local backend: create temp: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: write: %w", err)
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

func (b *LocalBackend) Read(_ context.Context) (io.ReadCloser, error) {
	f, err := os.Open(b.Path)
	if err != nil {
		return nil, fmt.Errorf("local backend: read: %w", err)
	}
	return f, nil
}
