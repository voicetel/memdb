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
	wg   sync.WaitGroup
	done atomic.Bool
}

// newForwarder creates a TLS listener on addr and starts accepting connections.
func newForwarder(addr string, tlsCfg *tls.Config, node *Node) (*forwarder, error) {
	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("forwarder: listen %s: %w", addr, err)
	}
	f := &forwarder{ln: ln, node: node}
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
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			f.handleConn(conn)
		}()
	}
}

func (f *forwarder) handleConn(conn net.Conn) {
	defer conn.Close()

	// Set an overall deadline for receiving the request. If the peer sends
	// nothing within this window, the connection is torn down so the
	// goroutine cannot leak waiting for a length-prefix that never arrives.
	const requestTimeout = 30 * time.Second
	if err := conn.SetDeadline(time.Now().Add(requestTimeout)); err != nil {
		return
	}

	var req ForwardRequest
	if err := readMsg(conn, &req); err != nil {
		// Malformed request or timeout — close silently.
		return
	}

	// Apply through Raft. This node must be the leader; if not (e.g. a
	// leadership change happened mid-flight), Exec returns ErrNotLeader and
	// the follower will retry on the new leader.
	err := f.node.Exec(req.SQL, req.Args...)

	resp := ForwardResponse{}
	if err != nil {
		resp.ErrMsg = err.Error()
		// Set a sentinel code for known error types so the caller of
		// sendForward can reconstitute a typed error and use errors.Is
		// for retry logic (rather than matching on the message string).
		if errors.Is(err, ErrNotLeader) {
			resp.ErrCode = "ErrNotLeader"
		}
	}

	// Best-effort response write — ignore errors (connection may have closed).
	_ = writeMsg(conn, resp)
}

func (f *forwarder) close() error {
	// Signal serve() that this close is intentional before closing the
	// listener so that the Accept error is not logged as unexpected.
	f.done.Store(true)
	err := f.ln.Close()
	// Wait for all in-flight handleConn goroutines to complete so that
	// callers (e.g. Node.Shutdown) can be sure no goroutines are still
	// running after close returns.
	f.wg.Wait()
	return err
}
