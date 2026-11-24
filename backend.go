package memdb

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
)

// ── Snapshot integrity format ─────────────────────────────────────────────────
//
// Every snapshot written by memdb is wrapped with a fixed prefix and footer
// so that a corrupt or truncated file is detected at restore time rather
// than silently loading bad data into memory.
//
// Wire layout (all multi-byte fields big-endian):
//
//	[4]byte  magic         = snapMagic  ("MDBK")
//	[4]byte  version       = 2          (uint32)
//	[8]byte  walSeq        = highest WAL sequence number the snapshot includes
//	[N]byte  payload       = raw SQLite bytes
//	[8]byte  payloadLen    = uint64(N)
//	[32]byte sha256        = SHA-256 of payload
//
// walSeq is the snapshot's checkpoint. On restore, WAL entries at or below it
// are skipped, because their effect is already in the snapshot. That makes
// recovery idempotent across a crash between the snapshot's rename and the WAL
// truncate. Version 1 snapshots have no walSeq and replay every entry.
//
// Why footer (not header) hash: the SHA-256 cannot be known until the
// payload has been seen in full. A header-hash format forced us to buffer
// the entire payload in memory before flushing — for a 1 GiB snapshot
// that meant a transient ~2 GiB allocation (bytes.Buffer doubles on
// growth) every flush interval. With the hash in a footer, the writer
// streams payload bytes straight to the backend through io.MultiWriter,
// updating an incremental SHA-256 as it goes; peak heap drops from
// O(payload) to O(1) and large-DB flushes stop triggering GC stalls.
//
// The reader reciprocates with a 40-byte sliding window: data from r is
// streamed to dst with the most-recently-read 40 bytes withheld, so on
// EOF the contents of the window IS the footer. No buffering, O(1) RAM
// on restore as well.
//
// Legacy snapshots that do NOT start with "MDBK" — typically a raw SQLite
// file written by some other tool — are loaded directly without
// verification, with a warning logged. Any subsequent flush upgrades
// them to the wrapped format.
//
// A version word greater than snapVersion is rejected with
// ErrSnapshotVersion: the file was written by a newer memdb and its layout
// is unknown here, so guessing would misread it.
//
// # Checkpoint prefix for authenticated backends
//
// A backend that authenticates its own payload (AuthenticatedBackend, e.g.
// AES-GCM in backends.EncryptedBackend) skips the MDBK wrap, because its
// tag already is the integrity check. It still needs the checkpoint, so the
// stream handed to such a backend is:
//
//	[4]byte  magic         = snapCkptMagic ("MDBC")
//	[4]byte  version       = 1             (uint32)
//	[8]byte  walSeq        = highest WAL sequence number the snapshot includes
//	[N]byte  payload       = raw SQLite bytes
//
// There is no footer and no SHA-256: the prefix travels inside the
// authenticated stream, so the backend's tag covers it together with the
// payload. A stream from an authenticated backend that starts with neither
// magic is a snapshot from before this prefix existed (raw SQLite bytes,
// "SQLite format 3\x00"): it loads as before, with checkpoint 0.

var snapMagic = [4]byte{'M', 'D', 'B', 'K'}

// snapCkptMagic opens the checkpoint-only prefix written for authenticated
// backends. It differs from snapMagic because the layout does: no footer.
var snapCkptMagic = [4]byte{'M', 'D', 'B', 'C'}

const (
	// snapVersion is the current snapshot format. Version 2 added the walSeq
	// checkpoint after the version word; version 1 had neither.
	snapVersion uint32 = 2

	// snapHeaderLen is the whole header: [magic][version][walSeq].
	snapHeaderLen = 4 + 4 + 8

	// snapCkptVersion is the current version of the checkpoint-only prefix.
	snapCkptVersion uint32 = 1

	// snapCkptHeaderLen is the whole checkpoint prefix: [magic][version][walSeq].
	snapCkptHeaderLen = 4 + 4 + 8

	// snapFooterLen is the trailing [payloadLen][sha256] = 40 bytes
	// written after the payload. The reader uses this as the size of
	// its sliding withhold window.
	snapFooterLen = 8 + 32
)

// snapshotWriter wraps an io.Writer with the streaming snapshot wire
// format. The constructor writes [magic][version] eagerly to dst; each
// Write passes payload bytes through to dst while feeding an incremental
// SHA-256; Finish writes the [payloadLen][sha256] footer.
//
// Memory is O(1) — only the running hash state (~200 B) plus an 8-byte
// length counter — regardless of payload size.
type snapshotWriter struct {
	dst      io.Writer
	hasher   hash.Hash
	n        uint64 // payload bytes successfully written to dst
	finished bool
}

// newSnapshotWriter writes the leading [magic][version][walSeq] header to dst
// and returns a writer that streams payload bytes to dst while hashing them.
// walSeq is the snapshot's checkpoint: the highest WAL sequence number whose
// effect the snapshot includes. The caller must invoke Finish to write the
// trailing footer.
func newSnapshotWriter(dst io.Writer, walSeq uint64) (*snapshotWriter, error) {
	var hdr [snapHeaderLen]byte
	copy(hdr[0:4], snapMagic[:])
	binary.BigEndian.PutUint32(hdr[4:8], snapVersion)
	binary.BigEndian.PutUint64(hdr[8:16], walSeq)
	if _, err := dst.Write(hdr[:]); err != nil {
		return nil, fmt.Errorf("snapshot: write header: %w", err)
	}
	return &snapshotWriter{dst: dst, hasher: sha256.New()}, nil
}

// Write streams p to dst and updates the running SHA-256. Only bytes
// accepted by dst are hashed, so on a short write the running hash and
// dst stay in sync — the caller (typically copyMemToWriter) decides
// whether to retry or surface the error.
func (s *snapshotWriter) Write(p []byte) (int, error) {
	n, err := s.dst.Write(p)
	if n > 0 {
		// sha256.Hash.Write never returns an error.
		_, _ = s.hasher.Write(p[:n])
		s.n += uint64(n)
	}
	return n, err
}

// Finish writes the [payloadLen][sha256] footer to dst. Idempotent so
// callers may invoke it from a defer without worrying about double-write.
func (s *snapshotWriter) Finish() error {
	if s.finished {
		return nil
	}
	s.finished = true
	var footer [snapFooterLen]byte
	binary.BigEndian.PutUint64(footer[0:8], s.n)
	sum := s.hasher.Sum(nil) // 32 bytes
	copy(footer[8:40], sum)
	if _, err := s.dst.Write(footer[:]); err != nil {
		return fmt.Errorf("snapshot: write footer: %w", err)
	}
	return nil
}

// writeCheckpointPrefix writes the [magic][version][walSeq] checkpoint prefix
// used for authenticated backends to dst. The raw SQLite payload follows it
// directly; there is no footer.
func writeCheckpointPrefix(dst io.Writer, walSeq uint64) error {
	var hdr [snapCkptHeaderLen]byte
	copy(hdr[0:4], snapCkptMagic[:])
	binary.BigEndian.PutUint32(hdr[4:8], snapCkptVersion)
	binary.BigEndian.PutUint64(hdr[8:16], walSeq)
	if _, err := dst.Write(hdr[:]); err != nil {
		return fmt.Errorf("snapshot: write checkpoint prefix: %w", err)
	}
	return nil
}

// UnwrapSnapshot reads a memdb snapshot from r, verifies its MDBK
// integrity wrap, and writes the raw SQLite payload bytes to dst as
// they stream. The result is a vanilla SQLite database file that can
// be opened by any SQLite tool (the sqlite3 CLI, DB Browser, etc.).
//
// If r contains a legacy unwrapped snapshot — raw SQLite bytes
// without the MDBK header — the bytes are passed through unchanged
// and isLegacy is true so the caller may warn or upgrade the file.
// The same goes for a stream carrying the "MDBC" checkpoint prefix
// written for authenticated backends (what such a backend hands back
// after authenticating it): the prefix is stripped, the payload is
// passed through, and isLegacy is true because nothing here verified it.
//
// Returns ErrSnapshotCorrupt for a wrapped snapshot whose footer
// indicates a length or hash mismatch, or whose footer is truncated,
// and ErrSnapshotVersion for an envelope version newer than this
// build understands.
//
// Memory is O(1) regardless of snapshot size: the caller may pass an
// arbitrarily large stream without buffering it. This is the public
// entry point used by external tooling such as cmd/memdb-cli.
func UnwrapSnapshot(r io.Reader, dst io.Writer) (isLegacy bool, err error) {
	_, isLegacy, err = verifyAndStreamPayload(r, dst)
	return isLegacy, err
}

// verifyAndStreamPayload reads a wrapped snapshot from r, verifies its
// integrity, and writes the raw SQLite payload bytes to dst as it streams.
// It returns the snapshot's checkpoint walSeq (0 for a version-1 or legacy
// snapshot), and isLegacy. Memory is O(1) — only a 64 KiB read buffer plus the
// running SHA-256 state.
//
// If r begins with snapCkptMagic it carries the checkpoint-only prefix an
// authenticated backend's stream has: the checkpoint is read, the rest of r
// is copied unchecked (the backend authenticated it), and isLegacy == true is
// returned — "legacy" here means only "no MDBK integrity wrap".
//
// If r begins with neither magic, the data is assumed to be a legacy
// (unwrapped) raw SQLite file. The 4 bytes already read are emitted to dst,
// the rest of r is copied unchecked, and isLegacy == true is returned. The
// caller decides whether to log a warning.
//
// Returns ErrSnapshotCorrupt for a wrapped snapshot whose footer indicates
// a length or hash mismatch, or whose footer is truncated, and
// ErrSnapshotVersion when either header names a version newer than this
// build knows.
func verifyAndStreamPayload(r io.Reader, dst io.Writer) (snapSeq uint64, isLegacy bool, err error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return 0, false, fmt.Errorf("%w: too short to read magic", ErrSnapshotCorrupt)
	}

	if magic == snapCkptMagic {
		var hdr [snapCkptHeaderLen - 4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return 0, false, fmt.Errorf("%w: truncated checkpoint prefix", ErrSnapshotCorrupt)
		}
		if version := binary.BigEndian.Uint32(hdr[0:4]); version == 0 || version > snapCkptVersion {
			return 0, false, fmt.Errorf("%w: checkpoint prefix version %d (this build reads up to %d)",
				ErrSnapshotVersion, version, snapCkptVersion)
		}
		walSeq := binary.BigEndian.Uint64(hdr[4:12])
		if _, cerr := io.Copy(dst, r); cerr != nil {
			return walSeq, true, cerr
		}
		return walSeq, true, nil
	}

	if magic != snapMagic {
		// Legacy raw SQLite file. Re-emit the magic bytes (they are
		// actually the start of the SQLite header) and stream the rest
		// unchanged.
		if _, werr := dst.Write(magic[:]); werr != nil {
			return 0, true, werr
		}
		if _, cerr := io.Copy(dst, r); cerr != nil {
			return 0, true, cerr
		}
		return 0, true, nil
	}

	var verBuf [4]byte
	if _, err := io.ReadFull(r, verBuf[:]); err != nil {
		return 0, false, fmt.Errorf("%w: truncated version", ErrSnapshotCorrupt)
	}
	version := binary.BigEndian.Uint32(verBuf[:])
	if version > snapVersion {
		// Written by a newer memdb. Its layout past the version word is
		// unknown here; reading it as version 2 would at best fail the hash
		// and at worst load a misparsed checkpoint.
		return 0, false, fmt.Errorf("%w: envelope version %d (this build reads up to %d)",
			ErrSnapshotVersion, version, snapVersion)
	}

	var walSeq uint64
	if version >= 2 {
		var seqBuf [8]byte
		if _, err := io.ReadFull(r, seqBuf[:]); err != nil {
			return 0, false, fmt.Errorf("%w: truncated checkpoint", ErrSnapshotCorrupt)
		}
		walSeq = binary.BigEndian.Uint64(seqBuf[:])
	}

	hasher := sha256.New()
	var payloadLen uint64

	// Streaming withhold: bufio.Reader.Peek(peekSize) returns up to
	// peekSize bytes without advancing the reader. We can safely emit
	// (len(peek) - snapFooterLen) bytes as payload — the trailing
	// snapFooterLen are still potentially footer. On the final batch
	// (Peek returns io.EOF) the trailing snapFooterLen bytes ARE the
	// footer.
	const peekSize = 64 * 1024
	br := bufio.NewReaderSize(r, peekSize)

	for {
		peek, perr := br.Peek(peekSize)
		if perr != nil && perr != io.EOF {
			return walSeq, false, fmt.Errorf("%w: read: %v", ErrSnapshotCorrupt, perr)
		}

		if perr == io.EOF {
			// Final batch — everything left is in peek. Trailing
			// snapFooterLen bytes are the footer.
			if len(peek) < snapFooterLen {
				return walSeq, false, fmt.Errorf("%w: footer truncated (got %d bytes, want >= %d)",
					ErrSnapshotCorrupt, len(peek), snapFooterLen)
			}
			payload := peek[:len(peek)-snapFooterLen]
			footer := peek[len(peek)-snapFooterLen:]
			if len(payload) > 0 {
				if _, werr := dst.Write(payload); werr != nil {
					return walSeq, false, werr
				}
				_, _ = hasher.Write(payload)
				payloadLen += uint64(len(payload))
			}

			storedLen := binary.BigEndian.Uint64(footer[0:8])
			var storedSum [32]byte
			copy(storedSum[:], footer[8:40])

			if storedLen != payloadLen {
				return walSeq, false, fmt.Errorf("%w: payload length mismatch (footer claims %d, got %d)",
					ErrSnapshotCorrupt, storedLen, payloadLen)
			}
			var computed [32]byte
			copy(computed[:], hasher.Sum(nil))
			if computed != storedSum {
				return walSeq, false, fmt.Errorf("%w: SHA-256 mismatch (expected %x, got %x)",
					ErrSnapshotCorrupt, storedSum, computed)
			}
			return walSeq, false, nil
		}

		// More data follows after this peek window — safe to consume
		// everything except the trailing snapFooterLen bytes.
		consume := len(peek) - snapFooterLen
		if consume <= 0 {
			// Buffer is full but smaller than snapFooterLen would require —
			// impossible since peekSize >> snapFooterLen, but defensive.
			return walSeq, false, fmt.Errorf("%w: peek returned %d bytes, expected at least %d",
				ErrSnapshotCorrupt, len(peek), snapFooterLen+1)
		}
		if _, werr := dst.Write(peek[:consume]); werr != nil {
			return walSeq, false, werr
		}
		_, _ = hasher.Write(peek[:consume])
		payloadLen += uint64(consume)
		if _, derr := br.Discard(consume); derr != nil {
			return walSeq, false, fmt.Errorf("%w: discard: %v", ErrSnapshotCorrupt, derr)
		}
	}
}

// Backend is the interface that storage backends must implement.
// External implementations must use WrapBackend(ExternalBackend) — the
// flush and restore methods are unexported and only implementable within
// this package. See ExternalBackend for the public interface.
// Use the backends sub-package for pre-built implementations (compressed,
// encrypted, or custom). LocalBackend below is the default file-system backend.
type Backend interface {
	// Exists reports whether a snapshot already exists in the backend.
	Exists(ctx context.Context) (bool, error)

	// flush writes the current in-memory DB state to the backend. snapSeq
	// is the WAL checkpoint to record in the snapshot: the highest WAL
	// sequence number whose effect the snapshot includes. Called by Flush;
	// the d argument is the owning DB.
	flush(ctx context.Context, d *DB, snapSeq uint64) error

	// restore reads a snapshot from the backend into the memory DB and
	// returns the checkpoint the snapshot records (0 when it records none).
	restore(ctx context.Context, d *DB) (uint64, error)
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

// flush streams the in-memory DB through the snapshot writer to a temp
// file (hashing as the bytes pass through), writes the integrity footer,
// then renames into place.
func (b *LocalBackend) flush(ctx context.Context, d *DB, snapSeq uint64) error {
	dir := filepath.Dir(b.Path)
	tmp, err := os.CreateTemp(dir, ".memdb-snap-*.db")
	if err != nil {
		return fmt.Errorf("local backend: create temp: %w", err)
	}
	tmpName := tmp.Name()

	sw, err := newSnapshotWriter(tmp, snapSeq)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot header: %w", err)
	}
	if err := copyMemToWriter(ctx, d, sw, d.cfg.BackupStepPages); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot: %w", err)
	}
	if err := sw.Finish(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot footer: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("local backend: fsync: %w", err)
	}
	_ = tmp.Close()

	if err := os.Rename(tmpName, b.Path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("local backend: rename: %w", err)
	}
	// fsync the parent directory so the rename is durable.
	if dir, err := os.Open(filepath.Dir(b.Path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// restore streams the on-disk snapshot through the verifier into the
// in-memory DB. Verification is incremental — bytes flow to a temp file
// while the SHA-256 is computed and the trailing footer is checked on
// EOF, so peak memory is independent of snapshot size.
func (b *LocalBackend) restore(ctx context.Context, d *DB) (uint64, error) {
	f, err := os.Open(b.Path)
	if err != nil {
		return 0, fmt.Errorf("local backend: open for restore: %w", err)
	}
	defer func() { _ = f.Close() }()

	snapSeq, isLegacy, err := restoreVerifiedSnapshot(ctx, d, f)
	if err != nil {
		return 0, fmt.Errorf("local backend: %w", err)
	}
	if isLegacy {
		d.logger().Warn("memdb: snapshot has no integrity checksum (legacy format); " +
			"the next flush will upgrade it to the checksummed format")
	}
	return snapSeq, nil
}

// copyMemToWriter and copyReaderToMem are declared in backup.go alongside
// the rest of the SQLite Online Backup API plumbing. Signatures:
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

// AuthenticatedBackend is an optional capability marker an ExternalBackend
// may implement to declare that it provides its own authenticated
// integrity (typically AEAD encryption such as AES-GCM). When the
// adapter detects this marker and Authenticated returns true, the
// MDBK SHA-256 wrap normally put around every snapshot is skipped — the
// backend's own authentication tag is the integrity check. The stream the
// backend receives instead starts with a 16-byte checkpoint prefix
// ([magic "MDBC"][version][walSeq]) followed by the raw SQLite bytes; the
// backend must return exactly those bytes from Read.
//
// Why this matters: pprof of the encrypted-flush path showed sha256
// blockSHANI at ~17% of CPU. AEAD wrappers like backends.EncryptedBackend
// already authenticate the payload, so the SHA pass is redundant.
//
// Backward compatibility on restore is preserved: the adapter inspects
// the first four bytes of the stored payload. A snapshot written before
// the marker was added still has the MDBK header and is verified
// normally; a snapshot with the checkpoint prefix, or a headerless one
// from before that prefix existed, is loaded directly without a
// "legacy" warning when the inner backend is Authenticated.
type AuthenticatedBackend interface {
	Authenticated() bool
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

// flush streams the in-memory DB to the external backend through a
// pipe. By default the stream is wrapped in the snapshot integrity
// format ([magic][version][walSeq][payload][len][sha256]); when the inner
// backend implements AuthenticatedBackend and reports true the wrap is
// skipped because the backend's own authenticator (e.g. AES-GCM tag) is
// the integrity check, saving the SHA-256 pass measured at ~17% of CPU
// on the encrypted-flush pprof. Such a stream still starts with the
// 16-byte checkpoint prefix ([magic "MDBC"][version][walSeq]) so the
// snapshot records which WAL entries it already holds; without it a crash
// between the snapshot write and the WAL truncate replayed them twice.
//
// Memory is O(1) regardless of snapshot size: bytes flow from
// copyMemToWriter through the optional snapshotWriter (which hashes as
// it streams) into the pipe, with the backend's Write consuming from
// the read end.
func (a *externalBackendAdapter) flush(ctx context.Context, d *DB, snapSeq uint64) error {
	pr, pw := io.Pipe()

	var copyErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		var w io.Writer = pw
		var sw *snapshotWriter
		if a.skipHeader() {
			if err := writeCheckpointPrefix(pw, snapSeq); err != nil {
				copyErr = err
				pw.CloseWithError(err)
				return
			}
		} else {
			var err error
			sw, err = newSnapshotWriter(pw, snapSeq)
			if err != nil {
				copyErr = err
				pw.CloseWithError(err)
				return
			}
			w = sw
		}
		if err := copyMemToWriter(ctx, d, w, d.cfg.BackupStepPages); err != nil {
			copyErr = err
			pw.CloseWithError(err)
			return
		}
		if sw != nil {
			if err := sw.Finish(); err != nil {
				copyErr = err
				pw.CloseWithError(err)
				return
			}
		}
		_ = pw.Close()
	}()

	writeErr := a.inner.Write(ctx, pr)
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

// skipHeader reports whether the inner backend opts out of the SHA-256
// integrity header by implementing AuthenticatedBackend with a true
// return. Hidden behind a helper so the interface assertion happens in
// one place and adding more capability markers later stays cheap.
func (a *externalBackendAdapter) skipHeader() bool {
	ab, ok := a.inner.(AuthenticatedBackend)
	return ok && ab.Authenticated()
}

// restore streams a snapshot from the external backend through the
// verifier into the in-memory DB. When the inner backend is
// Authenticated, an absent MDBK prefix is the expected format — the
// checkpoint prefix ("MDBC"), or raw SQLite bytes from before that
// prefix existed (checkpoint 0) — rather than a legacy snapshot, and no
// warning is emitted because the AEAD tag has already authenticated the
// bytes.
func (a *externalBackendAdapter) restore(ctx context.Context, d *DB) (uint64, error) {
	rc, err := a.inner.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("external backend: read: %w", err)
	}
	defer func() { _ = rc.Close() }()

	snapSeq, isLegacy, err := restoreVerifiedSnapshot(ctx, d, rc)
	if err != nil {
		return 0, fmt.Errorf("external backend: %w", err)
	}
	if isLegacy && !a.skipHeader() {
		d.logger().Warn("memdb: snapshot has no integrity checksum (legacy format); " +
			"the next flush will upgrade it to the checksummed format")
	}
	return snapSeq, nil
}

// Ensure the adapter satisfies the internal interface at compile time.
var _ Backend = (*externalBackendAdapter)(nil)
