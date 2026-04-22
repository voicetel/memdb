//go:build !purego

package memdb

import (
	"context"
	"fmt"
	"io"
	"os"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// copyMemToWriter serialises the in-memory DB into w using the SQLite Online
// Backup API: memory → temp file on disk → stream temp file → delete temp file.
// stepPages controls pages per backup step; -1 = all at once.
func copyMemToWriter(ctx context.Context, d *DB, w io.Writer, stepPages int) error {
	// Write to a temp file first (backup API needs a file path), then stream it.
	tmp, err := os.CreateTemp("", ".memdb-backup-*.db")
	if err != nil {
		return fmt.Errorf("memdb: backup temp: %w", err)
	}
	tmpName := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpName)

	fileDB, err := openFileDB(tmpName)
	if err != nil {
		return err
	}
	defer fileDB.Close()

	if err := withRawConn(ctx, d.mem, func(memConn *sqlite3.SQLiteConn) error {
		return withRawConn(ctx, fileDB, func(fileConn *sqlite3.SQLiteConn) error {
			return copyDB(ctx, memConn, fileConn, stepPages)
		})
	}); err != nil {
		return err
	}
	fileDB.Close()

	f, err := os.Open(tmpName)
	if err != nil {
		return fmt.Errorf("memdb: open backup temp: %w", err)
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
}

// copyReaderToMem restores the in-memory DB from r using the SQLite Online
// Backup API: stream r → temp file → backup into memory.
func copyReaderToMem(ctx context.Context, d *DB, r io.Reader, stepPages int) error {
	tmp, err := os.CreateTemp("", ".memdb-restore-*.db")
	if err != nil {
		return fmt.Errorf("memdb: restore temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return fmt.Errorf("memdb: restore write temp: %w", err)
	}
	tmp.Close()

	fileDB, err := openFileDB(tmpName)
	if err != nil {
		return err
	}
	defer fileDB.Close()

	return withRawConn(ctx, fileDB, func(fileConn *sqlite3.SQLiteConn) error {
		return withRawConn(ctx, d.mem, func(memConn *sqlite3.SQLiteConn) error {
			return copyDB(ctx, fileConn, memConn, stepPages)
		})
	})
}

// copyDB copies all pages from src to dst using the SQLite Online Backup API.
// stepPages controls pages copied per step; -1 copies all pages in one call.
// src remains fully readable and writable during the copy.
func copyDB(ctx context.Context, src, dst *sqlite3.SQLiteConn, stepPages int) error {
	backup, err := dst.Backup("main", src, "main")
	if err != nil {
		return fmt.Errorf("memdb: backup init: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			_ = backup.Finish()
			return fmt.Errorf("memdb: backup cancelled: %w", err)
		}
		done, err := backup.Step(stepPages)
		if err != nil {
			_ = backup.Finish()
			return fmt.Errorf("memdb: backup step: %w", err)
		}
		if done {
			break
		}
	}

	if err := backup.Finish(); err != nil {
		return fmt.Errorf("memdb: backup finish: %w", err)
	}
	return nil
}
