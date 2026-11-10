package raft

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
)

// A forwarded write's arguments travel as []any, which gob carries only for
// registered concrete types. It registers the basic ones itself; time.Time is
// the one argument type the Raft log codec takes that it does not, so without
// this a follower could not forward a time argument the leader would take.
func init() {
	gob.Register(time.Time{})
}

// ForwardRequest is sent from a follower to the leader to forward a write.
type ForwardRequest struct {
	SQL  string
	Args []any
}

// ForwardResponse is sent from the leader back to the follower.
type ForwardResponse struct {
	ErrMsg string // empty means success

	// ErrCode is an optional sentinel identifier that lets the caller
	// reconstruct a typed Go error on the follower side. Known values:
	//   "ErrNotLeader" — the forwarded request was sent to a node that is
	//                    no longer the leader, or that refused it because it
	//                    is handing leadership over. The write was not made.
	// An empty ErrCode with a non-empty ErrMsg means "some other error";
	// callers should treat it as a generic failure.
	ErrCode string

	// RowsAffected is the count the leader's FSM apply reported for the
	// forwarded statement. Gob keeps mixed-version clusters working: a
	// pre-count leader simply never encodes the field (the follower sees
	// 0), and a pre-count follower ignores it on decode.
	RowsAffected int64

	// SQLiteCode, SQLiteExtendedCode, and SQLiteErrno carry the codes of a
	// SQLite error the write met on the leader, so the follower can return
	// the same typed error (ForwardedError). All three are zero for any other
	// error. Like RowsAffected, an older peer leaves them out or ignores them,
	// and the follower then sees a plain error, as before.
	SQLiteCode         int
	SQLiteExtendedCode int
	SQLiteErrno        uint64
}

// ── wire helpers ──────────────────────────────────────────────────────────────

// RPCConn wraps a net.Conn with a cached gob Encoder and Decoder so the
// type schema for ForwardRequest / ForwardResponse is sent only once per
// conn lifetime. Without this caching every writeMsg / readMsg created
// fresh Encoder/Decoder pairs that re-sent the type descriptors,
// dominating the per-RPC allocation count (~45% of total) measured by
// BenchmarkForwarding_PoolReuse.
//
// RPCConn embeds net.Conn so the rest of the codebase (deadlines, Close)
// continues to work transparently.
type RPCConn struct {
	net.Conn
	encBuf *bytes.Buffer
	enc    *gob.Encoder
	decBuf *bytes.Buffer
	dec    *gob.Decoder
}

func NewRPCConn(c net.Conn) *RPCConn {
	encBuf := new(bytes.Buffer)
	decBuf := new(bytes.Buffer)
	return &RPCConn{
		Conn:   c,
		encBuf: encBuf,
		enc:    gob.NewEncoder(encBuf),
		decBuf: decBuf,
		dec:    gob.NewDecoder(decBuf),
	}
}

// writeMsg gob-encodes v using the cached encoder, then writes
// [4-byte big-endian length][gob bytes] to the underlying conn. The
// length prefix is preserved (rather than relying on gob's stream framing
// alone) so a malformed or oversized peer message can be rejected before
// the decoder allocates a body buffer.
func (rc *RPCConn) writeMsg(v any) error {
	rc.encBuf.Reset()
	if err := rc.enc.Encode(v); err != nil {
		return fmt.Errorf("rpc encode: %w", err)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(rc.encBuf.Len()))
	if _, err := rc.Write(hdr[:]); err != nil {
		return fmt.Errorf("rpc write length: %w", err)
	}
	if _, err := rc.Write(rc.encBuf.Bytes()); err != nil {
		return fmt.Errorf("rpc write body: %w", err)
	}
	return nil
}

// readMsg reads one length-prefixed gob message from the conn into v
// using the cached decoder. Each decode consumes exactly the bytes the
// matching writeMsg produced, so the bytes.Buffer is empty at entry to
// every subsequent call.
func (rc *RPCConn) readMsg(v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(rc.Conn, hdr[:]); err != nil {
		return fmt.Errorf("rpc read length: %w", err)
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 || length > 64*1024*1024 { // 64 MB sanity limit
		return fmt.Errorf("rpc: invalid message length %d", length)
	}
	rc.decBuf.Reset()
	if _, err := io.CopyN(rc.decBuf, rc.Conn, int64(length)); err != nil {
		return fmt.Errorf("rpc read body: %w", err)
	}
	return rc.dec.Decode(v)
}

// ── connection pool ───────────────────────────────────────────────────────────

const (
	// defaultPoolSize is the number of idle TLS connections kept per leader.
	// Sized to GOMAXPROCS-ish; callers that exceed the pool simply dial fresh.
	defaultPoolSize = 8

	// idleTimeout is how long a pooled connection may sit idle before it is
	// considered stale and discarded rather than reused.
	idleTimeout = 30 * time.Second

	// dialTimeout bounds the TCP+TLS handshake portion of Get separately
	// from the overall apply timeout. Without this, a caller that supplies
	// a 10s apply timeout would spend up to 10s on the dial alone, leaving
	// no time for the actual Raft round-trip on the leader.
	dialTimeout = 2 * time.Second
)

// pooledConn wraps an RPCConn with the time it was returned to the pool so
// that stale connections can be detected and discarded on checkout.
type pooledConn struct {
	conn      *RPCConn
	idleSince time.Time
}

// ConnPool is a channel-based pool of TLS connections to a single remote
// address. The buffered channel holds idle connections; a non-blocking receive
// either gets a warm connection immediately or falls through to a fresh dial.
//
// When the leader changes, the old pool is closed (draining the channel and
// closing every idle connection) and a new pool is created for the new leader.
// This is safe because ConnPool itself is immutable after construction — the
// Node swaps the pointer atomically.
type ConnPool struct {
	addr   string
	tlsCfg *tls.Config
	idle   chan pooledConn // buffered channel — this IS the pool
	closed atomic.Bool
}

// NewConnPool creates a pool for the given address. size is the maximum number
// of idle connections to keep. Use defaultPoolSize unless testing.
func NewConnPool(addr string, tlsCfg *tls.Config, size int) *ConnPool {
	if size <= 0 {
		size = defaultPoolSize
	}
	return &ConnPool{
		addr:   addr,
		tlsCfg: tlsCfg,
		idle:   make(chan pooledConn, size),
	}
}

// Get returns a usable net.Conn. It attempts a non-blocking receive from the
// idle channel first; if the channel is empty or the connection has been idle
// longer than idleTimeout it dials a new TLS connection.
//
// The timeout argument is the overall deadline budget for the caller. The
// dial itself is capped at min(timeout, dialTimeout) so that the handshake
// does not consume the entire budget when the peer is slow or unreachable —
// leaving time for the actual request/response round-trip.
//
// Liveness is not probed at checkout — attempting a zero-deadline read over
// TLS is unreliable because TLS close_notify alerts may be buffered. Instead,
// sendForward detects a dead connection when the write or read fails and
// discards it rather than returning it to the pool.
func (p *ConnPool) Get(timeout time.Duration) (*RPCConn, error) {
	return p.GetContext(context.Background(), timeout)
}

// GetContext is Get whose dial also ends when ctx does. Both the TCP connect
// and the TLS handshake stop then, still capped by min(timeout, dialTimeout),
// so a peer that accepts the connection and then stalls the handshake does
// not hold a caller past its own deadline. The error then wraps ctx's, and
// nothing was sent.
func (p *ConnPool) GetContext(ctx context.Context, timeout time.Duration) (*RPCConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("rpc: connection to %s not taken: %w", p.addr, err)
	}
	// Reject early if the pool has been closed — avoids dialing a fresh
	// connection that would be immediately closed by Put.
	if p.closed.Load() {
		return nil, fmt.Errorf("rpc: pool is closed")
	}

	// Drain connections that have exceeded the idle timeout. A connection
	// that has sat idle too long may have been closed by the remote end;
	// discarding it here avoids handing a likely-dead connection to the caller.
	// The labelled break exits the for loop rather than just the select.
drainLoop:
	for {
		select {
		case pc := <-p.idle:
			if time.Since(pc.idleSince) > idleTimeout {
				_ = pc.conn.Close()
				continue drainLoop
			}
			return pc.conn, nil
		default:
			break drainLoop
		}
	}

	// Cap the dial portion so that the handshake cannot consume the entire
	// caller budget. If the caller's budget is smaller than dialTimeout,
	// honour that instead.
	d := dialTimeout
	if timeout > 0 && timeout < d {
		d = timeout
	}
	if d <= 0 {
		return nil, fmt.Errorf("rpc: deadline exceeded before dial")
	}
	dialCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	dialer := &tls.Dialer{NetDialer: &net.Dialer{}, Config: p.tlsCfg}
	conn, err := dialer.DialContext(dialCtx, "tcp", p.addr)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// The caller's context ended the dial, not the cap on it: say
			// so with its error, whatever the network reported.
			return nil, fmt.Errorf("rpc: dial %s: %w", p.addr, ctxErr)
		}
		return nil, fmt.Errorf("rpc: dial %s: %w", p.addr, err)
	}
	return NewRPCConn(conn), nil
}

// Put returns conn to the idle channel. If the pool is full or has been
// closed, the connection is closed immediately rather than leaked.
func (p *ConnPool) Put(conn *RPCConn) {
	if p.closed.Load() {
		_ = conn.Close()
		return
	}
	// Non-blocking send: if the channel is full we discard the connection
	// rather than block or grow the pool unboundedly.
	select {
	case p.idle <- pooledConn{conn: conn, idleSince: time.Now()}:
	default:
		_ = conn.Close()
	}
}

// Close drains the idle channel and closes every connection. After Close,
// any subsequent Put calls will close the connection immediately.
func (p *ConnPool) Close() {
	p.closed.Store(true)
	for {
		select {
		case pc := <-p.idle:
			_ = pc.conn.Close()
		default:
			return
		}
	}
}

// IdleCount returns the number of connections currently sitting idle in the
// pool. Intended for testing and monitoring only.
func (p *ConnPool) IdleCount() int {
	return len(p.idle)
}

// ── sendForward ───────────────────────────────────────────────────────────────

// sendForward sends req to the leader via pool, waits for the response, and
// returns the rows-affected count the leader reported along with the
// leader's error (or nil on success).
//
// It checks out a connection from pool, sends the request, reads the response,
// and — if both succeeded — returns the connection to the pool for reuse. On
// any transport error the connection is discarded rather than pooled, so
// a broken connection is never handed to the next caller.
func sendForward(ctx context.Context, pool *ConnPool, req ForwardRequest, timeout time.Duration) (int64, error) {
	conn, err := pool.GetContext(ctx, timeout)
	if err != nil {
		return 0, err
	}

	// Set a single deadline covering both the write and the read. The read
	// includes the full Raft round-trip on the leader side, so timeout must
	// be the caller's ApplyTimeout, not a shorter network timeout.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return 0, fmt.Errorf("rpc: set deadline: %w", err)
	}
	// A context that can end, by cancellation or its own deadline, expires
	// the connection's deadline when it does, which ends the exchange at
	// once with the context's error. A context that cannot costs nothing.
	stop := func() bool { return true }
	if ctx.Done() != nil {
		stop = context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	}

	resp, err := exchange(conn, req)
	if !stop() || err != nil {
		// The context ended during the exchange, or the exchange failed: the
		// connection's state is unknown, so it is not reused.
		_ = conn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil && err != nil {
			return 0, fmt.Errorf("rpc: forward stopped, and the write may still commit: %w", ctxErr)
		}
		if err != nil {
			return 0, err
		}
	} else {
		// Clear the deadline before returning the connection to the pool so
		// it doesn't expire while idle.
		_ = conn.SetDeadline(time.Time{})
		pool.Put(conn)
	}

	if resp.ErrMsg != "" {
		// Translate known sentinel codes back into typed errors so callers
		// on the follower side can use errors.Is(err, ErrNotLeader) to
		// drive retry logic without string-matching.
		switch {
		case resp.ErrCode == "ErrNotLeader",
			// A leader from before it sent the code for this: it refused the
			// write because it was handing leadership over, in these words.
			resp.ErrCode == "" && resp.ErrMsg == transferInProgressMsg:
			return 0, fmt.Errorf("%w: forwarded: %s", ErrNotLeader, resp.ErrMsg)
		}
		return 0, forwardedError(resp)
	}
	return resp.RowsAffected, nil
}

// transferInProgressMsg is the whole message of a write a leader refused while
// handing leadership over, as a leader that predates marking it ErrNotLeader
// sends it: with no ErrCode.
var transferInProgressMsg = "raft: apply: " + hraft.ErrLeadershipTransferInProgress.Error()

// exchange sends one request and reads its answer.
func exchange(conn *RPCConn, req ForwardRequest) (ForwardResponse, error) {
	var resp ForwardResponse
	if err := conn.writeMsg(req); err != nil {
		return resp, err
	}
	err := conn.readMsg(&resp)
	return resp, err
}

// ForwardedError is an error a forwarded write met on the leader, as the
// follower that forwarded it sees it. Its message is the leader's. For a
// SQLite error it unwraps to a sqlite3.Error with the leader's codes, so
// errors.As finds one, and callers can tell a constraint or a busy error apart
// on a follower as they do on the leader.
type ForwardedError struct {
	msg    string
	sqlite *sqlite3.Error
}

// forwardedError rebuilds the error a response reports.
func forwardedError(resp ForwardResponse) *ForwardedError {
	e := &ForwardedError{msg: resp.ErrMsg}
	if resp.SQLiteCode != 0 {
		e.sqlite = &sqlite3.Error{
			Code:         sqlite3.ErrNo(resp.SQLiteCode),
			ExtendedCode: sqlite3.ErrNoExtended(resp.SQLiteExtendedCode),
			SystemErrno:  syscall.Errno(resp.SQLiteErrno),
		}
	}
	return e
}

// Error returns the leader's message.
func (e *ForwardedError) Error() string { return e.msg }

// Unwrap returns the SQLite error with the leader's codes, or nil for any
// other error.
func (e *ForwardedError) Unwrap() error {
	if e.sqlite == nil {
		return nil
	}
	return *e.sqlite
}

// sqliteCodes fills a response with the codes of the SQLite error err holds,
// if it holds one.
func sqliteCodes(resp *ForwardResponse, err error) {
	var se sqlite3.Error
	if errors.As(err, &se) {
		resp.SQLiteCode, resp.SQLiteExtendedCode, resp.SQLiteErrno = int(se.Code), int(se.ExtendedCode), uint64(se.SystemErrno)
	}
}
