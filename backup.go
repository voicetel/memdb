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

	if err := withRawConn(ctx, d.mem, func(memConn *sqlite3.SQLiteConn) error {
		return withRawConn(ctx, fileDB, func(fileConn *sqlite3.SQLiteConn) error {
			return copyDB(ctx, memConn, fileConn, stepPages)
		})
	}); err != nil {
		fileDB.Close()
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

// restoreVerifiedSnapshot streams a wrapped snapshot from r through
// verifyAndStreamPayload directly into a temp file (combining the
// verification and the temp-file staging that copyReaderToMem would do
// in two passes), then loads the verified payload into d.mem via the
// SQLite Online Backup API.
//
// Returns isLegacy=true when r contained an unwrapped raw SQLite file
// (no "MDBK" prefix); the caller decides whether to log a warning. On
// verification failure (length or hash mismatch, truncated footer)
// returns ErrSnapshotCorrupt without touching d.mem.
//
// Memory is O(1) regardless of snapshot size — the verifier uses a
// 64 KiB sliding window and writes the verified payload straight to
// disk; the temp file is then handed to the SQLite backup API.
func restoreVerifiedSnapshot(ctx context.Context, d *DB, r io.Reader) (isLegacy bool, err error) {
	tmp, err := os.CreateTemp("", ".memdb-restore-*.db")
	if err != nil {
		return false, fmt.Errorf("memdb: restore temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	isLegacy, verr := verifyAndStreamPayload(r, tmp)
	if cerr := tmp.Close(); cerr != nil && verr == nil {
		verr = cerr
	}
	if verr != nil {
		return isLegacy, verr
	}

	fileDB, err := openFileDB(tmpName)
	if err != nil {
		return isLegacy, err
	}
	defer fileDB.Close()

	return isLegacy, withRawConn(ctx, fileDB, func(fileConn *sqlite3.SQLiteConn) error {
		return withRawConn(ctx, d.mem, func(memConn *sqlite3.SQLiteConn) error {
			return copyDB(ctx, fileConn, memConn, d.cfg.BackupStepPages)
		})
	})
}

// Serialize returns the complete in-memory database as a raw SQLite byte
// slice using sqlite3_serialize. Used by the Raft FSM for snapshots.
func (d *DB) Serialize() ([]byte, error) {
	var data []byte
	if err := withRawConn(context.Background(), d.mem, func(conn *sqlite3.SQLiteConn) error {
		var err error
		data, err = conn.Serialize("main")
		return err
	}); err != nil {
		return nil, fmt.Errorf("memdb: serialize: %w", err)
	}
	return data, nil
}

// Restore replaces the complete in-memory database from raw SQLite bytes
// using sqlite3_deserialize. Used by the Raft FSM to install snapshots.
func (d *DB) Restore(data []byte) error {
	return withRawConn(context.Background(), d.mem, func(conn *sqlite3.SQLiteConn) error {
		return conn.Deserialize(data, "main")
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
