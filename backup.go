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

// serializeFlushMaxBytes bounds the in-memory fast path in copyMemToWriter.
// At or below this size, a flush uses sqlite3_serialize to produce the whole
// image in one C→Go copy instead of the backup-API-via-temp-file round trip.
// Serialize is markedly cheaper for a small DB — pprof of BenchmarkFlush
// showed the temp-file path spending ~21% of CPU in file syscalls plus most
// of a further ~33% in backup-API cgocall, all of which serialize replaces
// with a single memcpy — but it costs an O(DB-size) allocation (transiently
// ~3× during the C→Go copy), so above this threshold copyMemToWriter keeps
// the streaming path whose flush memory stays O(1). 64 MiB comfortably covers
// any realistic in-memory hot DB while bounding the serialize spike to
// well under 200 MiB.
const serializeFlushMaxBytes = 64 << 20

// copyMemToWriter serialises the in-memory DB into w.
//
// Small DBs (≤ serializeFlushMaxBytes) take an in-memory fast path via
// sqlite3_serialize, avoiding a temp-file write/read round trip. Larger DBs
// stream through copyMemToWriterViaBackup (the SQLite Online Backup API),
// which keeps flush memory O(1) at the cost of the extra disk I/O. stepPages
// controls pages per backup step on the streaming path (-1 = all at once); it
// is unused by the serialize path.
func copyMemToWriter(ctx context.Context, d *DB, w io.Writer, stepPages int) error {
	if size, ok := d.approxBytes(ctx); ok && size <= serializeFlushMaxBytes {
		var data []byte
		err := withRawConn(ctx, d.mem, func(conn *sqlite3.SQLiteConn) error {
			var serr error
			data, serr = conn.Serialize("main")
			return serr
		})
		if err == nil {
			_, werr := w.Write(data)
			return werr
		}
		// Serialize failed before any byte reached w (typically OOM from
		// the sqlite3_malloc64 of the image). Fall through to the streaming
		// backup path, which flushes with O(1) memory. Falling through is
		// only safe here because nothing has been written to w yet — a
		// failure after w.Write began must not double-write, so that case
		// returns above.
	}
	return copyMemToWriterViaBackup(ctx, d, w, stepPages)
}

// copyMemToWriterViaBackup streams the in-memory DB into w using the SQLite
// Online Backup API: memory → temp file on disk → stream temp file → delete
// temp file. Flush memory is O(io.Copy buffer) regardless of DB size, which
// is why large DBs (> serializeFlushMaxBytes) use this path instead of the
// whole-image sqlite3_serialize fast path. stepPages controls pages per backup
// step; -1 = all at once.
func copyMemToWriterViaBackup(ctx context.Context, d *DB, w io.Writer, stepPages int) error {
	// Write to a temp file first (backup API needs a file path), then stream it.
	tmp, err := os.CreateTemp("", ".memdb-backup-*.db")
	if err != nil {
		return fmt.Errorf("memdb: backup temp: %w", err)
	}
	tmpName := tmp.Name()
	_ = tmp.Close()
	defer func() { _ = os.Remove(tmpName) }()

	fileDB, err := openFileDB(tmpName)
	if err != nil {
		return err
	}

	if err := withRawConn(ctx, d.mem, func(memConn *sqlite3.SQLiteConn) error {
		return withRawConn(ctx, fileDB, func(fileConn *sqlite3.SQLiteConn) error {
			return copyDB(ctx, memConn, fileConn, stepPages)
		})
	}); err != nil {
		_ = fileDB.Close()
		return err
	}
	_ = fileDB.Close()

	f, err := os.Open(tmpName)
	if err != nil {
		return fmt.Errorf("memdb: open backup temp: %w", err)
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(w, f)
	return err
}

// approxBytes returns the approximate on-disk size of the in-memory database
// (page_count × page_size), used to choose the flush strategy. It reports
// ok=false if the size cannot be determined, in which case callers fall back
// to the streaming path. The query reads only DB-header metadata so it is
// cheap relative to the flush it gates.
func (d *DB) approxBytes(ctx context.Context) (int64, bool) {
	var size int64
	err := d.mem.QueryRowContext(ctx,
		"SELECT (SELECT page_count FROM pragma_page_count) * "+
			"(SELECT page_size FROM pragma_page_size)",
	).Scan(&size)
	if err != nil || size < 0 {
		return 0, false
	}
	return size, true
}

// restoreVerifiedSnapshot streams a wrapped snapshot from r through
// verifyAndStreamPayload directly into a temp file (combining the
// verification and the temp-file staging that copyReaderToMem would do
// in two passes), then loads the verified payload into d.mem via the
// SQLite Online Backup API.
//
// Returns isLegacy=true when r had no "MDBK" integrity wrap — an unwrapped
// raw SQLite file, or the "MDBC" checkpoint prefix an authenticated
// backend's stream carries; the caller decides whether to log a warning. On
// verification failure (length or hash mismatch, truncated footer)
// returns ErrSnapshotCorrupt without touching d.mem.
//
// Memory is O(1) regardless of snapshot size — the verifier uses a
// 64 KiB sliding window and writes the verified payload straight to
// disk; the temp file is then handed to the SQLite backup API.
func restoreVerifiedSnapshot(ctx context.Context, d *DB, r io.Reader) (snapSeq uint64, isLegacy bool, err error) {
	tmp, err := os.CreateTemp("", ".memdb-restore-*.db")
	if err != nil {
		return 0, false, fmt.Errorf("memdb: restore temp: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	snapSeq, isLegacy, verr := verifyAndStreamPayload(r, tmp)
	if cerr := tmp.Close(); cerr != nil && verr == nil {
		verr = cerr
	}
	if verr != nil {
		return snapSeq, isLegacy, verr
	}

	fileDB, err := openFileDB(tmpName)
	if err != nil {
		return snapSeq, isLegacy, err
	}
	defer func() { _ = fileDB.Close() }()

	return snapSeq, isLegacy, withRawConn(ctx, fileDB, func(fileConn *sqlite3.SQLiteConn) error {
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
