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

// Backend is the interface all backends in this package must satisfy.
// It is structurally identical to memdb.ExternalBackend — any type that
// implements one implements the other. This declaration exists because
// the backends package cannot import package memdb without a cycle.
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

	var encErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, copyErr := io.Copy(enc, r)
		closeErr := enc.Close()
		// Propagate the first non-nil error from copy or close.
		if copyErr != nil {
			pw.CloseWithError(fmt.Errorf("compressed backend: copy: %w", copyErr))
			encErr = copyErr
		} else if closeErr != nil {
			pw.CloseWithError(fmt.Errorf("compressed backend: encoder close: %w", closeErr))
			encErr = closeErr
		} else {
			pw.Close()
		}
	}()

	writeErr := b.Inner.Write(ctx, pr)
	// Close the read end so the goroutine unblocks if Write returned early.
	pr.CloseWithError(writeErr)
	<-done

	if encErr != nil {
		return encErr
	}
	return writeErr
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
