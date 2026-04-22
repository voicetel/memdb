//go:build !purego

package raft

import (
	"crypto/tls"
	"fmt"
	"net"
)

// forwarder listens for incoming ForwardRequests from follower nodes and
// applies them through Raft consensus on the leader.
type forwarder struct {
	ln   net.Listener
	node *Node
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
			// Listener closed — stop accepting.
			return
		}
		go f.handleConn(conn)
	}
}

func (f *forwarder) handleConn(conn net.Conn) {
	defer conn.Close()

	var req ForwardRequest
	if err := readMsg(conn, &req); err != nil {
		// Malformed request — close silently.
		return
	}

	// Apply through Raft. This node must be the leader; if not (e.g. a
	// leadership change happened mid-flight), Exec returns ErrNotLeader and
	// the follower will retry on the new leader.
	err := f.node.Exec(req.SQL, req.Args...)

	resp := ForwardResponse{}
	if err != nil {
		resp.ErrMsg = err.Error()
	}

	// Best-effort response write — ignore errors (connection may have closed).
	_ = writeMsg(conn, resp)
}

func (f *forwarder) close() error {
	return f.ln.Close()
}
