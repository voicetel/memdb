package memdb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// sqliteFileHeader is the 16-byte magic that prefixes every well-formed
// SQLite database file (https://sqlite.org/fileformat.html §1.3). DB.Restore
// rejects payloads that do not start with this prefix.
var sqliteFileHeader = []byte("SQLite format 3\x00")

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
//
// Since v1.9.0 the deserialize is performed via deserializeResizable
// (SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE) so
// the database is allowed to grow past len(data) on subsequent writes
// — before that, the cap was frozen at the snapshot size and produced
// SQLITE_FULL on every post-restore write.
//
// A PRAGMA max_page_count cap is applied immediately after the
// deserialize to honour Config.RestoreMaxBytes. The cap is enforced
// in pages so it is automatically scaled by whatever page_size the
// snapshot was written with.
//
// Defence-in-depth: a 16-byte SQLite header check rejects malformed
// payloads with ErrSnapshotCorrupt before reaching sqlite3_deserialize.
// The Raft stream that feeds this entry point is already authenticated,
// but accepting a stray byte slice into the raw deserialize API would
// crash the FSM goroutine on the next query instead of returning
// cleanly.
func (d *DB) Restore(data []byte) error {
	// minSnapshotBytes is the SQLite file header length (100 bytes,
	// https://sqlite.org/fileformat.html §1.3). A payload shorter
	// than this cannot be a valid database; rejecting it here keeps
	// sqlite3_deserialize from being asked to install bytes that
	// would corrupt the connection's internal state on the next
	// query — sqlite3_deserialize itself does not promise atomicity
	// on malformed input.
	const minSnapshotBytes = 100
	if len(data) < minSnapshotBytes || !bytes.HasPrefix(data, sqliteFileHeader) {
		return ErrSnapshotCorrupt
	}
	return loadSnapshot(context.Background(), d.mem, data, d.cfg.RestoreMaxBytes)
}

// loadSnapshot installs data into db via RESIZEABLE deserialize and
// then enforces maxBytes (when > 0) as PRAGMA max_page_count. Shared
// by DB.Restore (writer) and replicaPool.refresh (each replica) so
// every in-memory copy of the database goes through the same code
// path and ends up with the same growth policy.
func loadSnapshot(ctx context.Context, db *sql.DB, data []byte, maxBytes int64) error {
	if err := withRawConn(ctx, db, func(conn *sqlite3.SQLiteConn) error {
		return deserializeResizable(conn, data)
	}); err != nil {
		return err
	}
	return applyMaxPageCount(ctx, db, maxBytes)
}

// applyMaxPageCount sets PRAGMA max_page_count = (maxBytes/page_size)
// on db. Reads page_size first because the deserialised database can
// have been written with any supported page size and we want the cap
// expressed in bytes regardless. Maps maxBytes <= 0 to a no-op so
// callers can pass through their config field without a guard.
func applyMaxPageCount(ctx context.Context, db *sql.DB, maxBytes int64) error {
	if maxBytes <= 0 {
		return nil
	}
	var pageSize int64
	if err := db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return fmt.Errorf("memdb: read page_size after restore: %w", err)
	}
	if pageSize <= 0 {
		return fmt.Errorf("memdb: unexpected page_size=%d after restore", pageSize)
	}
	maxPages := maxBytes / pageSize
	if maxPages < 1 {
		maxPages = 1
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA max_page_count = %d", maxPages)); err != nil {
		return fmt.Errorf("memdb: apply max_page_count=%d after restore: %w", maxPages, err)
	}
	return nil
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
