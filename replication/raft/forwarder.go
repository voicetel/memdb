package raft

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// forwarder listens for incoming ForwardRequests from follower nodes and
// applies them through Raft consensus on the leader.
type forwarder struct {
	ln   net.Listener
	node *Node
	// exec applies one forwarded write; it is the node's ExecResult.
	exec  func(sql string, args ...any) (int64, error)
	wg    sync.WaitGroup
	done  atomic.Bool
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// requestTimeout bounds one request on a forwarded connection, from waiting for
// it to answering it. The follower's ConnPool reuses connections across
// requests, so a handler loops until the peer closes or stays idle longer than
// this window.
const requestTimeout = 30 * time.Second

// newForwarder creates a TLS listener on addr and starts accepting connections.
func newForwarder(addr string, tlsCfg *tls.Config, node *Node) (*forwarder, error) {
	return newForwarderWith(addr, tlsCfg, node, node.ExecResult)
}

// newForwarderWith is newForwarder applying forwarded writes with exec.
func newForwarderWith(addr string, tlsCfg *tls.Config, node *Node, exec func(string, ...any) (int64, error)) (*forwarder, error) {
	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("forwarder: listen %s: %w", addr, err)
	}
	f := &forwarder{ln: ln, node: node, exec: exec, conns: map[net.Conn]struct{}{}}
	go f.serve()
	return f, nil
}

func (f *forwarder) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			// Distinguish an intentional close from a transient/unexpected error.
			// After close() sets done and closes the listener, Accept returns an
			// error — that is expected and should not be logged.
			if !f.done.Load() {
				f.node.logger().Warn("forwarder: accept error", "error", err)
			}
			return
		}
		f.mu.Lock()
		if f.done.Load() {
			f.mu.Unlock()
			_ = conn.Close()
			return
		}
		f.conns[conn] = struct{}{}
		f.wg.Add(1)
		f.mu.Unlock()
		go func() {
			defer f.wg.Done()
			defer f.removeConn(conn)
			defer func() {
				if r := recover(); r != nil {
					// A panicking handler must not crash the forwarder or
					// the Raft node. Log and close the connection so the
					// follower gets a clean EOF and retries.
					_ = conn.Close()
					f.node.logger().Error("forwarder: handler panic recovered",
						"panic", r,
						"remote", conn.RemoteAddr(),
					)
				}
			}()
			f.handleConn(conn)
		}()
	}
}

func (f *forwarder) removeConn(conn net.Conn) {
	f.mu.Lock()
	delete(f.conns, conn)
	f.mu.Unlock()
}

func (f *forwarder) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	// Wrap the conn so the gob Encoder/Decoder are created once and reused
	// across every request on this conn. Without this, each request would
	// re-send the ForwardRequest/ForwardResponse type schemas, which
	// dominated allocations on the forwarding hot path.
	rc := NewRPCConn(conn)

	// Resetting the deadline before every readMsg lets a long-lived conn serve
	// many requests while still bounding the goroutine's lifetime when the
	// follower goes quiet.
	for f.arm(rc) {
		var req ForwardRequest
		if err := rc.readMsg(&req); err != nil {
			// Clean EOF (peer closed), idle timeout, or malformed framing —
			// close silently. The follower will dial fresh on its next call.
			return
		}

		// Apply through Raft. This node must be the leader; if not (e.g. a
		// leadership change happened mid-flight), ExecResult returns
		// ErrNotLeader and the follower will retry on the new leader.
		rowsAffected, err := f.exec(req.SQL, req.Args...)

		resp := ForwardResponse{RowsAffected: rowsAffected}
		if err != nil {
			resp.ErrMsg = err.Error()
			// Sentinel code for known error types so sendForward can
			// reconstitute a typed error and drive retry via errors.Is
			// rather than message-string matching.
			if errors.Is(err, ErrNotLeader) {
				resp.ErrCode = "ErrNotLeader"
			}
		}

		if err := rc.writeMsg(resp); err != nil {
			// Response write failed (peer closed mid-request). Cannot
			// continue this conn — the follower will dial again.
			return
		}
	}
}

// arm sets the deadline for a connection's next request, unless the forwarder
// is closing. It holds the lock close takes, so a closing forwarder's expired
// read deadline is never pushed back out by a handler starting its next read.
func (f *forwarder) arm(rc *RPCConn) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.done.Load() && rc.SetDeadline(time.Now().Add(requestTimeout)) == nil
}

// close stops the forwarder. A handler waiting for its next request, which is
// what a pooled connection mostly does, ends at once: its read deadline is
// expired, without waiting out the idle window. A handler that already holds a
// request is past its read, so it applies the write and answers before it sees
// the forwarder closing; otherwise a write the leader applied could reach the
// follower as a failure. That wait is bounded by the apply timeout and the
// request's own deadline.
func (f *forwarder) close() error {
	f.mu.Lock()
	// Signal serve() that this close is intentional before closing the
	// listener so that the Accept error is not logged as unexpected.
	f.done.Store(true)
	err := f.ln.Close()
	now := time.Now()
	for conn := range f.conns {
		_ = conn.SetReadDeadline(now)
	}
	f.mu.Unlock()
	// Wait for all handleConn goroutines to complete so that callers (e.g.
	// Node.Shutdown) can be sure no goroutines are still running after close
	// returns.
	f.wg.Wait()
	return err
}
