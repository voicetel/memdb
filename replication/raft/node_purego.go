//go:build purego

package raft

import (
	"crypto/tls"
	"fmt"
	"time"
)

// NodeConfig holds all options for a Raft cluster node.
// In purego builds this is a stub; NewNode always returns an error.
type NodeConfig struct {
	NodeID            string
	BindAddr          string
	AdvertiseAddr     string
	Peers             []string
	DataDir           string
	TLSConfig         *tls.Config
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	CommitTimeout     time.Duration
	ApplyTimeout      time.Duration
	OnLeaderChange    func(isLeader bool)
	OnApplyError      func(err error)
}

// DB is the subset of memdb.DB that Node needs.
type DB interface {
	ExecLocal(sql string, args ...any) error
	Serialize() ([]byte, error)
	Restore(data []byte) error
}

// Node is a stub Raft cluster node for purego builds.
// All methods return errors or zero values.
type Node struct{}

// NewNode always returns an error in purego builds.
func NewNode(_ DB, _ NodeConfig) (*Node, error) {
	return nil, fmt.Errorf("raft: not supported in purego builds")
}

// Exec always returns an error in purego builds.
func (n *Node) Exec(_ string, _ ...any) error { return fmt.Errorf("not supported") }

// IsLeader always returns false in purego builds.
func (n *Node) IsLeader() bool { return false }

// LeaderAddr always returns an empty string in purego builds.
func (n *Node) LeaderAddr() string { return "" }

// AddVoter always returns an error in purego builds.
func (n *Node) AddVoter(_, _ string, _ time.Duration) error { return fmt.Errorf("not supported") }

// RemoveServer always returns an error in purego builds.
func (n *Node) RemoveServer(_ string, _ time.Duration) error { return fmt.Errorf("not supported") }

// Shutdown is a no-op in purego builds.
func (n *Node) Shutdown() error { return nil }

// Stats always returns nil in purego builds.
func (n *Node) Stats() map[string]string { return nil }

// ErrNotLeader is returned when a write is attempted on a non-leader node.
var ErrNotLeader = fmt.Errorf("raft: not leader")
