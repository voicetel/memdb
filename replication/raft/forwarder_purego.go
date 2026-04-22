//go:build purego

package raft

import (
	"crypto/tls"
	"fmt"
)

type forwarder struct{}

func newForwarder(_ string, _ *tls.Config, _ *Node) (*forwarder, error) {
	return nil, fmt.Errorf("raft: forwarding not supported in purego builds")
}

func (f *forwarder) close() error { return nil }
