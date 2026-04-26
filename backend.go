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
//	[4]byte  version       = 1          (uint32)
//	[N]byte  payload       = raw SQLite bytes
//	[8]byte  payloadLen    = uint64(N)
//	[32]byte sha256        = SHA-256 of payload
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

var snapMagic = [4]byte{'M', 'D', 'B', 'K'}

const (
	// snapPrefixLen is the leading [magic][version] = 8 bytes that
	// identifies the wrapped format and selects a decoder version.
	snapPrefixLen = 4 + 4

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

// newSnapshotWriter writes the leading [magic][version] header to dst
// and returns a writer that streams payload bytes to dst while hashing
// them. The caller must invoke Finish to write the trailing footer.
func newSnapshotWriter(dst io.Writer) (*snapshotWriter, error) {
	var hdr [snapPrefixLen]byte
	copy(hdr[0:4], snapMagic[:])
	binary.BigEndian.PutUint32(hdr[4:8], 1) // version
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

// verifyAndStreamPayload reads a wrapped snapshot from r, verifies its
// integrity, and writes the raw SQLite payload bytes to dst as it streams.
// Memory is O(1) — only a 64 KiB read buffer plus the running SHA-256 state.
//
// If r does not begin with snapMagic, the data is assumed to be a legacy
// (unwrapped) raw SQLite file. The 4 bytes already read are emitted to dst,
// the rest of r is copied unchecked, and isLegacy == true is returned. The
// caller decides whether to log a warning.
//
// Returns ErrSnapshotCorrupt for a wrapped snapshot whose footer indicates
// a length or hash mismatch, or whose footer is truncated.
func verifyAndStreamPayload(r io.Reader, dst io.Writer) (isLegacy bool, err error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return false, fmt.Errorf("%w: too short to read magic", ErrSnapshotCorrupt)
	}

	if magic != snapMagic {
		// Legacy raw SQLite file. Re-emit the magic bytes (they are
		// actually the start of the SQLite header) and stream the rest
		// unchanged.
		if _, werr := dst.Write(magic[:]); werr != nil {
			return true, werr
		}
		if _, cerr := io.Copy(dst, r); cerr != nil {
			return true, cerr
		}
		return true, nil
	}

	var verBuf [4]byte
	if _, err := io.ReadFull(r, verBuf[:]); err != nil {
		return false, fmt.Errorf("%w: truncated version", ErrSnapshotCorrupt)
	}
	// version := binary.BigEndian.Uint32(verBuf[:]) — reserved for future use.

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
			return false, fmt.Errorf("%w: read: %v", ErrSnapshotCorrupt, perr)
		}

		if perr == io.EOF {
			// Final batch — everything left is in peek. Trailing
			// snapFooterLen bytes are the footer.
			if len(peek) < snapFooterLen {
				return false, fmt.Errorf("%w: footer truncated (got %d bytes, want >= %d)",
					ErrSnapshotCorrupt, len(peek), snapFooterLen)
			}
			payload := peek[:len(peek)-snapFooterLen]
			footer := peek[len(peek)-snapFooterLen:]
			if len(payload) > 0 {
				if _, werr := dst.Write(payload); werr != nil {
					return false, werr
				}
				_, _ = hasher.Write(payload)
				payloadLen += uint64(len(payload))
			}

			storedLen := binary.BigEndian.Uint64(footer[0:8])
			var storedSum [32]byte
			copy(storedSum[:], footer[8:40])

			if storedLen != payloadLen {
				return false, fmt.Errorf("%w: payload length mismatch (footer claims %d, got %d)",
					ErrSnapshotCorrupt, storedLen, payloadLen)
			}
			var computed [32]byte
			copy(computed[:], hasher.Sum(nil))
			if computed != storedSum {
				return false, fmt.Errorf("%w: SHA-256 mismatch (expected %x, got %x)",
					ErrSnapshotCorrupt, storedSum, computed)
			}
			return false, nil
		}

		// More data follows after this peek window — safe to consume
		// everything except the trailing snapFooterLen bytes.
		consume := len(peek) - snapFooterLen
		if consume <= 0 {
			// Buffer is full but smaller than snapFooterLen would require —
			// impossible since peekSize >> snapFooterLen, but defensive.
			return false, fmt.Errorf("%w: peek returned %d bytes, expected at least %d",
				ErrSnapshotCorrupt, len(peek), snapFooterLen+1)
		}
		if _, werr := dst.Write(peek[:consume]); werr != nil {
			return false, werr
		}
		_, _ = hasher.Write(peek[:consume])
		payloadLen += uint64(consume)
		if _, derr := br.Discard(consume); derr != nil {
			return false, fmt.Errorf("%w: discard: %v", ErrSnapshotCorrupt, derr)
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

	// flush writes the current in-memory DB state to the backend.
	// Called by Flush; the d argument is the owning DB.
	flush(ctx context.Context, d *DB) error

	// restore reads a snapshot from the backend into the memory DB.
	restore(ctx context.Context, d *DB) error
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
func (b *LocalBackend) flush(ctx context.Context, d *DB) error {
	dir := filepath.Dir(b.Path)
	tmp, err := os.CreateTemp(dir, ".memdb-snap-*.db")
	if err != nil {
		return fmt.Errorf("local backend: create temp: %w", err)
	}
	tmpName := tmp.Name()

	sw, err := newSnapshotWriter(tmp)
	if err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot header: %w", err)
	}
	if err := copyMemToWriter(ctx, d, sw, d.cfg.BackupStepPages); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot: %w", err)
	}
	if err := sw.Finish(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("local backend: write snapshot footer: %w", err)
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
	// fsync the parent directory so the rename is durable.
	if dir, err := os.Open(filepath.Dir(b.Path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

// restore streams the on-disk snapshot through the verifier into the
// in-memory DB. Verification is incremental — bytes flow to a temp file
// while the SHA-256 is computed and the trailing footer is checked on
// EOF, so peak memory is independent of snapshot size.
func (b *LocalBackend) restore(ctx context.Context, d *DB) error {
	f, err := os.Open(b.Path)
	if err != nil {
		return fmt.Errorf("local backend: open for restore: %w", err)
	}
	defer f.Close()

	isLegacy, err := restoreVerifiedSnapshot(ctx, d, f)
	if err != nil {
		return fmt.Errorf("local backend: %w", err)
	}
	if isLegacy {
		d.logger().Warn("memdb: snapshot has no integrity checksum (legacy format); " +
			"the next flush will upgrade it to the checksummed format")
	}
	return nil
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
// 40-byte MDBK SHA-256 header normally prepended to every snapshot is
// skipped — the backend's own authentication tag is the integrity check.
//
// Why this matters: pprof of the encrypted-flush path showed sha256
// blockSHANI at ~17% of CPU. AEAD wrappers like backends.EncryptedBackend
// already authenticate the payload, so the SHA pass is redundant.
//
// Backward compatibility on restore is preserved: the adapter inspects
// the first four bytes of the stored payload. A snapshot written before
// the marker was added still has the MDBK header and is verified
// normally; a new headerless snapshot is loaded directly without a
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
// format ([magic][version][payload][len][sha256]); when the inner
// backend implements AuthenticatedBackend and reports true the wrap is
// skipped because the backend's own authenticator (e.g. AES-GCM tag) is
// the integrity check, saving the SHA-256 pass measured at ~17% of CPU
// on the encrypted-flush pprof.
//
// Memory is O(1) regardless of snapshot size: bytes flow from
// copyMemToWriter through the optional snapshotWriter (which hashes as
// it streams) into the pipe, with the backend's Write consuming from
// the read end.
func (a *externalBackendAdapter) flush(ctx context.Context, d *DB) error {
	pr, pw := io.Pipe()

	var copyErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		var w io.Writer = pw
		var sw *snapshotWriter
		if !a.skipHeader() {
			var err error
			sw, err = newSnapshotWriter(pw)
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
		pw.Close()
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
// Authenticated, an absent MDBK prefix is treated as the headerless
// format rather than a legacy snapshot — no warning is emitted because
// the AEAD tag has already authenticated the bytes.
func (a *externalBackendAdapter) restore(ctx context.Context, d *DB) error {
	rc, err := a.inner.Read(ctx)
	if err != nil {
		return fmt.Errorf("external backend: read: %w", err)
	}
	defer rc.Close()

	isLegacy, err := restoreVerifiedSnapshot(ctx, d, rc)
	if err != nil {
		return fmt.Errorf("external backend: %w", err)
	}
	if isLegacy && !a.skipHeader() {
		d.logger().Warn("memdb: snapshot has no integrity checksum (legacy format); " +
			"the next flush will upgrade it to the checksummed format")
	}
	return nil
}

// Ensure the adapter satisfies the internal interface at compile time.
var _ Backend = (*externalBackendAdapter)(nil)
