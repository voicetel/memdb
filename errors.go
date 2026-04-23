package memdb

import "errors"

var (
	ErrFlushFailed             = errors.New("memdb: flush to disk failed")
	ErrRestoreFailed           = errors.New("memdb: restore from disk failed")
	ErrClosed                  = errors.New("memdb: database is closed")
	ErrTransactionNotSupported = errors.New("memdb: transactions not supported in Raft mode")

	// ErrWALUnsupportedArgType is returned by Exec/ExecContext when
	// DurabilityWAL or DurabilitySync is configured and a bound parameter
	// is of a Go type the binary WAL encoder does not recognise (e.g. a
	// custom struct). The write is rejected cleanly so the caller can fix
	// the argument type rather than having an unreplayable record written
	// to disk.
	//
	// Supported arg types: nil, all integer widths, float32/64, bool,
	// string, []byte, and time.Time.
	ErrWALUnsupportedArgType = errors.New("memdb: wal: unsupported arg type")

	// ErrSnapshotCorrupt is returned by Open/Restore when the on-disk
	// snapshot fails its integrity checksum. The file exists but its
	// contents are untrustworthy and the database refuses to load it.
	ErrSnapshotCorrupt = errors.New("memdb: snapshot integrity check failed")
)
