//go:build purego

package raft

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	hraft "github.com/hashicorp/raft"
)

type tlsStreamLayer struct{}

func newTLSStreamLayer(_, _ string, _ *tls.Config) (*tlsStreamLayer, error) {
	return nil, fmt.Errorf("raft: TLS transport not supported in purego builds")
}

func (t *tlsStreamLayer) Accept() (net.Conn, error) { return nil, fmt.Errorf("not supported") }
func (t *tlsStreamLayer) Close() error              { return nil }
func (t *tlsStreamLayer) Addr() net.Addr            { return nil }
func (t *tlsStreamLayer) Dial(_ hraft.ServerAddress, _ time.Duration) (net.Conn, error) {
	return nil, fmt.Errorf("not supported")
}
