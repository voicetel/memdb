//go:build purego

package memdb

import (
	"context"
	"fmt"
	"io"
)

// copyMemToWriter for the modernc/purego driver. modernc does not expose the
// backup API, so we fall back to file I/O via the URI file path.
func copyMemToWriter(_ context.Context, _ *DB, _ io.Writer, _ int) error {
	return fmt.Errorf("memdb: purego build does not support snapshot flush (no backup API)")
}

func copyReaderToMem(_ context.Context, _ *DB, _ io.Reader, _ int) error {
	return fmt.Errorf("memdb: purego build does not support snapshot restore (no backup API)")
}

func (d *DB) Serialize() ([]byte, error) {
	return nil, fmt.Errorf("memdb: Serialize not supported in purego builds")
}

func (d *DB) Restore(_ []byte) error {
	return fmt.Errorf("memdb: Restore not supported in purego builds")
}
