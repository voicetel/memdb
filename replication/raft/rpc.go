//go:build !purego

package raft

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"net"
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
}

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

// sendForward dials addr over TLS, sends req, reads the response.
// Returns the error from the leader's ForwardResponse, or a transport error.
func sendForward(addr string, tlsCfg *tls.Config, req ForwardRequest, timeout time.Duration) error {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
	if err != nil {
		return fmt.Errorf("rpc: dial leader %s: %w", addr, err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("rpc: set deadline: %w", err)
	}
	if err := writeMsg(conn, req); err != nil {
		return err
	}

	var resp ForwardResponse
	if err := readMsg(conn, &resp); err != nil {
		return err
	}
	if resp.ErrMsg != "" {
		return fmt.Errorf("%s", resp.ErrMsg)
	}
	return nil
}
