//go:build purego

package raft

import (
	"crypto/tls"
	"fmt"
	"time"
)

// ForwardRequest is sent from a follower to the leader to forward a write.
type ForwardRequest struct {
	SQL  string
	Args []any
}

// ForwardResponse is sent from the leader back to the follower.
type ForwardResponse struct {
	ErrMsg string
}

// sendForward is not supported in purego builds.
func sendForward(_ string, _ *tls.Config, _ ForwardRequest, _ time.Duration) error {
	return fmt.Errorf("raft: forwarding not supported in purego builds")
}
