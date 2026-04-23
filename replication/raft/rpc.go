//go:build !purego

package raft

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

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
	//                    no longer the leader.
	// An empty ErrCode with a non-empty ErrMsg means "some other error";
	// callers should treat it as a generic failure.
	ErrCode string
}

// ── wire helpers ──────────────────────────────────────────────────────────────

// writeMsg encodes v as gob and writes it to w with a 4-byte big-endian
// length prefix.
func writeMsg(w io.Writer, v any) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return fmt.Errorf("rpc encode: %w", err)
	}
	length := uint32(buf.Len())
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return fmt.Errorf("rpc write length: %w", err)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("rpc write body: %w", err)
	}
	return nil
}

// readMsg reads a length-prefixed gob message from r into v.
func readMsg(r io.Reader, v any) error {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return fmt.Errorf("rpc read length: %w", err)
	}
	if length == 0 || length > 64*1024*1024 { // 64 MB sanity limit
		return fmt.Errorf("rpc: invalid message length %d", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("rpc read body: %w", err)
	}
	return gob.NewDecoder(bytes.NewReader(buf)).Decode(v)
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

// pooledConn wraps a net.Conn with the time it was returned to the pool so
// that stale connections can be detected and discarded on checkout.
type pooledConn struct {
	conn      net.Conn
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
func (p *ConnPool) Get(timeout time.Duration) (net.Conn, error) {
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
	dialer := &net.Dialer{Timeout: d}
	conn, err := tls.DialWithDialer(dialer, "tcp", p.addr, p.tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("rpc: dial %s: %w", p.addr, err)
	}
	return conn, nil
}

// Put returns conn to the idle channel. If the pool is full or has been
// closed, the connection is closed immediately rather than leaked.
func (p *ConnPool) Put(conn net.Conn) {
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
// returns the leader's error (or nil on success).
//
// It checks out a connection from pool, sends the request, reads the response,
// and — if both succeeded — returns the connection to the pool for reuse. On
// any transport error the connection is discarded rather than pooled, so
// a broken connection is never handed to the next caller.
func sendForward(pool *ConnPool, req ForwardRequest, timeout time.Duration) error {
	conn, err := pool.Get(timeout)
	if err != nil {
		return err
	}

	// Set a single deadline covering both the write and the read. The read
	// includes the full Raft round-trip on the leader side, so timeout must
	// be the caller's ApplyTimeout, not a shorter network timeout.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return fmt.Errorf("rpc: set deadline: %w", err)
	}

	if err := writeMsg(conn, req); err != nil {
		_ = conn.Close()
		return err
	}

	var resp ForwardResponse
	if err := readMsg(conn, &resp); err != nil {
		_ = conn.Close()
		return err
	}

	// Clear the deadline before returning the connection to the pool so it
	// doesn't expire while idle.
	_ = conn.SetDeadline(time.Time{})
	pool.Put(conn)

	if resp.ErrMsg != "" {
		// Translate known sentinel codes back into typed errors so callers
		// on the follower side can use errors.Is(err, ErrNotLeader) to
		// drive retry logic without string-matching.
		switch resp.ErrCode {
		case "ErrNotLeader":
			return fmt.Errorf("%w: forwarded: %s", ErrNotLeader, resp.ErrMsg)
		}
		return errors.New(resp.ErrMsg)
	}
	return nil
}
