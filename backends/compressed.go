package backends

import (
	"context"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// CompressedBackend wraps any Backend with zstd compression.
// Typical SQLite files compress 50-70% at default settings.
type CompressedBackend struct {
	Inner Backend
}

// Backend is the interface all backends must satisfy.
type Backend interface {
	Exists(ctx context.Context) (bool, error)
	Write(ctx context.Context, r io.Reader) error
	Read(ctx context.Context) (io.ReadCloser, error)
}

func (b *CompressedBackend) Exists(ctx context.Context) (bool, error) {
	return b.Inner.Exists(ctx)
}

func (b *CompressedBackend) Write(ctx context.Context, r io.Reader) error {
	pr, pw := io.Pipe()
	enc, err := zstd.NewWriter(pw)
	if err != nil {
		return fmt.Errorf("compressed backend: encoder: %w", err)
	}

	go func() {
		_, copyErr := io.Copy(enc, r)
		enc.Close()
		pw.CloseWithError(copyErr)
	}()

	return b.Inner.Write(ctx, pr)
}

func (b *CompressedBackend) Read(ctx context.Context) (io.ReadCloser, error) {
	rc, err := b.Inner.Read(ctx)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(rc)
	if err != nil {
		rc.Close()
		return nil, fmt.Errorf("compressed backend: decoder: %w", err)
	}
	return dec.IOReadCloser(), nil
}
