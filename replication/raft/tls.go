package raft

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	hraft "github.com/hashicorp/raft"
)

// tlsStreamLayer is a raft.StreamLayer backed by a TLS TCP listener.
// All inbound and outbound Raft RPC connections are encrypted.
type tlsStreamLayer struct {
	ln        net.Listener // underlying TLS listener
	advertise net.Addr     // address advertised to peers (may differ from bind addr)
	cfg       *tls.Config
}

// newTLSStreamLayer creates a TLS listener on bindAddr.
// advertisedAddr is the address peers should connect to (use bindAddr if empty).
func newTLSStreamLayer(bindAddr, advertisedAddr string, tlsCfg *tls.Config) (*tlsStreamLayer, error) {
	ln, err := tls.Listen("tcp", bindAddr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("raft tls: listen %s: %w", bindAddr, err)
	}
	advertise := ln.Addr()
	if advertisedAddr != "" {
		advertise, err = net.ResolveTCPAddr("tcp", advertisedAddr)
		if err != nil {
			ln.Close()
			return nil, fmt.Errorf("raft tls: resolve advertise addr %s: %w", advertisedAddr, err)
		}
	}
	return &tlsStreamLayer{ln: ln, advertise: advertise, cfg: tlsCfg}, nil
}

func (t *tlsStreamLayer) Accept() (net.Conn, error) { return t.ln.Accept() }
func (t *tlsStreamLayer) Close() error              { return t.ln.Close() }
func (t *tlsStreamLayer) Addr() net.Addr            { return t.advertise }

// Dial connects to a peer using TLS with the same config as the listener.
func (t *tlsStreamLayer) Dial(addr hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", string(addr), t.cfg)
	if err != nil {
		return nil, fmt.Errorf("raft tls: dial %s: %w", addr, err)
	}
	return conn, nil
}
