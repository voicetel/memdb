//go:build !purego

package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/voicetel/memdb/logging"
	"github.com/voicetel/memdb/replication"

	"crypto/tls"
	"log/slog"
)

// NodeConfig holds all options for a Raft cluster node.
type NodeConfig struct {
	// NodeID uniquely identifies this node in the cluster. Must be unique
	// across all nodes. Example: "node-1" or a UUID.
	NodeID string

	// BindAddr is the TCP address this node listens on for Raft RPCs.
	// Example: "0.0.0.0:7000"
	BindAddr string

	// AdvertiseAddr is the address peers use to reach this node.
	// Defaults to BindAddr if empty. Use this when binding 0.0.0.0.
	// Example: "10.0.0.1:7000"
	AdvertiseAddr string

	// ForwardAddr is the TCP address this node listens on for forwarded write
	// RPCs from follower nodes. Must be different from BindAddr.
	// Example: "0.0.0.0:7001"
	ForwardAddr string

	// ForwardPeers maps each peer's nodeID to its ForwardAddr, in the same
	// "nodeID=addr" format as Peers. Used by followers to locate the leader's
	// forwarding endpoint. Must include an entry for every node in the cluster,
	// including this node.
	// Example: ["node-1=10.0.0.1:7001", "node-2=10.0.0.2:7001"]
	ForwardPeers []string

	// Peers is the list of all nodes in the initial cluster, including this
	// node. Format: "nodeID=addr", e.g. ["node-1=10.0.0.1:7000", "node-2=10.0.0.2:7000"].
	// Only used during first bootstrap. Ignored if state already exists.
	Peers []string

	// DataDir is the directory where Raft stores its logs, stable state, and
	// snapshots. Must be persistent across restarts.
	DataDir string

	// TLSConfig is required. All inter-node communication is encrypted.
	// Both server and client sides use the same config (mutual TLS recommended).
	TLSConfig *tls.Config

	// SnapshotInterval controls how often Raft checks whether a snapshot
	// should be taken. Default: 30s.
	SnapshotInterval time.Duration

	// SnapshotThreshold is the number of log entries between snapshots.
	// Default: 8192.
	SnapshotThreshold uint64

	// HeartbeatTimeout is the Raft heartbeat timeout. Default: 500ms.
	HeartbeatTimeout time.Duration

	// ElectionTimeout is the Raft election timeout. Default: 500ms.
	ElectionTimeout time.Duration

	// CommitTimeout is the maximum time to wait for a commit. Default: 50ms.
	CommitTimeout time.Duration

	// ApplyTimeout is the timeout for a single Apply call (one write).
	// Default: 10s.
	ApplyTimeout time.Duration

	// Logger is used for structured log output from this node and the underlying
	// hashicorp/raft instance. If nil, slog.Default() is used.
	// Use logging.NewSyslogHandler, logging.NewJSONHandler, or
	// logging.NewTextHandler to construct a suitable logger.
	Logger *slog.Logger

	// OnLeaderChange is called whenever this node gains or loses leadership.
	OnLeaderChange func(isLeader bool)

	// OnApplyError is called when the FSM fails to apply a log entry.
	OnApplyError func(err error)
}

func (c *NodeConfig) applyDefaults() {
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 30 * time.Second
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 8192
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 500 * time.Millisecond
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 500 * time.Millisecond
	}
	if c.CommitTimeout == 0 {
		c.CommitTimeout = 50 * time.Millisecond
	}
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = 10 * time.Second
	}
}

// DB is the subset of memdb.DB that Node needs.
type DB interface {
	// ExecLocal applies SQL directly to the local database, bypassing Raft.
	// Used by the FSM when applying committed log entries.
	ExecLocal(sql string, args ...any) error

	// Serialize returns the complete database as a raw byte slice.
	Serialize() ([]byte, error)

	// Restore replaces the complete database from a raw byte slice.
	Restore(data []byte) error
}

// logger returns the slog.Logger to use, falling back to slog.Default() when
// no logger was provided in NodeConfig.
func (n *Node) logger() *slog.Logger {
	if n.cfg.Logger != nil {
		return n.cfg.Logger
	}
	return slog.Default()
}

// Node is a member of a Raft cluster backed by a memdb database.
// All writes go through Raft consensus before being applied to the local DB.
type Node struct {
	cfg          NodeConfig
	raft         *hraft.Raft
	transport    *hraft.NetworkTransport
	forwarder    *forwarder               // nil when ForwardAddr is empty
	forwardPeers map[string]string        // nodeID → forwardAddr
	pool         atomic.Pointer[ConnPool] // current leader's connection pool
	seq          atomic.Uint64
	isLeader     atomic.Bool
	observerWg   sync.WaitGroup
	observer     *hraft.Observer        // stored so Shutdown can deregister it
	observerCh   chan hraft.Observation // stored so Shutdown can close it
	shutdownOnce sync.Once              // ensures Shutdown is idempotent
}

// storeCloser is satisfied by fileLogStore and fileStableStore, which both
// hold resources that must be released on error paths in NewNode.
type storeCloser interface {
	Close() error
}

// NewNode creates and starts a Raft cluster node.
//
// db must implement the DB interface — typically a *memdb.DB wrapped with a
// thin adapter that routes FSM callbacks to the in-memory database without
// triggering the OnExec hook (which would cause an infinite Raft→Exec→Raft
// loop).
//
// On the very first start (no existing state in DataDir), the cluster is
// bootstrapped using NodeConfig.Peers. On subsequent starts the existing
// state is recovered automatically.
func NewNode(db DB, cfg NodeConfig) (*Node, error) {
	cfg.applyDefaults()

	if cfg.NodeID == "" {
		return nil, fmt.Errorf("raft node: NodeID is required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("raft node: BindAddr is required")
	}
	if cfg.TLSConfig == nil {
		return nil, fmt.Errorf("raft node: TLSConfig is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("raft node: DataDir is required")
	}
	// Peers must always be provided — even on restart they are used to validate
	// the caller's intent, and on first boot they define the cluster membership.
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("raft node: Peers must not be empty")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("raft node: create data dir: %w", err)
	}

	// ── stores ───────────────────────────────────────────────────────────────
	// Open stores before the transport so we can call HasExistingState and
	// validate the even-peer constraint cheaply (no socket bind) before any
	// network I/O. Error paths after each open close all previously opened
	// resources via the storeCloser interface.
	logStore, err := newLogStore(filepath.Join(cfg.DataDir, "raft-log"))
	if err != nil {
		return nil, fmt.Errorf("raft node: log store: %w", err)
	}

	stableStore, err := newStableStore(filepath.Join(cfg.DataDir, "raft-stable"))
	if err != nil {
		if c, ok := logStore.(storeCloser); ok {
			c.Close()
		}
		return nil, fmt.Errorf("raft node: stable store: %w", err)
	}

	snapStore, err := hraft.NewFileSnapshotStoreWithLogger(
		filepath.Join(cfg.DataDir, "snapshots"), 3,
		logging.NewHCLogAdapter(cfg.Logger, "raft.snapshot"),
	)
	if err != nil {
		if c, ok := logStore.(storeCloser); ok {
			c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			c.Close()
		}
		return nil, fmt.Errorf("raft node: snapshot store: %w", err)
	}

	// ── check existing state & validate peer count ───────────────────────────
	// HasExistingState only reads already-open store handles — no extra I/O.
	// We do this before binding the transport so that an even-peer error is
	// returned without ever touching a socket.
	hasState, err := hraft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		if c, ok := logStore.(storeCloser); ok {
			c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			c.Close()
		}
		return nil, fmt.Errorf("raft node: check state: %w", err)
	}
	if !hasState {
		// Even-sized clusters have no fault-tolerance advantage over the next
		// smaller odd size. Only enforce this on first boot — a node restarting
		// into an existing cluster uses whatever membership is already stored.
		if len(cfg.Peers)%2 == 0 {
			if c, ok := logStore.(storeCloser); ok {
				c.Close()
			}
			if c, ok := stableStore.(storeCloser); ok {
				c.Close()
			}
			return nil, fmt.Errorf("raft node: cluster size must be odd (got %d peers) — "+
				"even-sized clusters have no fault tolerance advantage over the next smaller odd size; "+
				"use 1, 3, 5, or 7 nodes", len(cfg.Peers))
		}
		if len(cfg.Peers) == 1 {
			l := cfg.Logger
			if l == nil {
				l = slog.Default()
			}
			l.Warn("memdb raft: single-node cluster has no fault tolerance",
				"nodeID", cfg.NodeID,
			)
		}
	}

	// ── TLS transport ────────────────────────────────────────────────────────
	stream, err := newTLSStreamLayer(cfg.BindAddr, cfg.AdvertiseAddr, cfg.TLSConfig)
	if err != nil {
		if c, ok := logStore.(storeCloser); ok {
			c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			c.Close()
		}
		return nil, err
	}

	transport := hraft.NewNetworkTransportWithConfig(&hraft.NetworkTransportConfig{
		Stream:  stream,
		MaxPool: 5,
		Timeout: 10 * time.Second,
		Logger:  logging.NewHCLogAdapter(cfg.Logger, "raft.transport"),
	})

	// ── FSM ──────────────────────────────────────────────────────────────────
	node := &Node{cfg: cfg, transport: transport}

	fsm := NewFSM(
		func(sql string, args ...any) error {
			return db.ExecLocal(sql, args...)
		},
		func() ([]byte, error) {
			return db.Serialize()
		},
		func(data []byte) error {
			return db.Restore(data)
		},
	)
	if cfg.OnApplyError != nil {
		fsm.SetApplyErrorHandler(cfg.OnApplyError)
	}

	// ── raft config ──────────────────────────────────────────────────────────
	raftCfg := hraft.DefaultConfig()
	raftCfg.LocalID = hraft.ServerID(cfg.NodeID)
	raftCfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftCfg.ElectionTimeout = cfg.ElectionTimeout
	raftCfg.CommitTimeout = cfg.CommitTimeout
	raftCfg.SnapshotInterval = cfg.SnapshotInterval
	raftCfg.SnapshotThreshold = cfg.SnapshotThreshold
	raftCfg.Logger = logging.NewHCLogAdapter(cfg.Logger, "raft")

	r, err := hraft.NewRaft(raftCfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		transport.Close()
		if c, ok := logStore.(storeCloser); ok {
			c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			c.Close()
		}
		return nil, fmt.Errorf("raft node: new raft: %w", err)
	}
	node.raft = r

	// ── bootstrap if no existing state ───────────────────────────────────────
	if !hasState {
		node.logger().Info("memdb raft: bootstrapping new cluster",
			"nodeID", cfg.NodeID,
			"peers", len(cfg.Peers),
		)
		servers, err := parsePeers(cfg.Peers)
		if err != nil {
			r.Shutdown()
			return nil, fmt.Errorf("raft node: parse peers: %w", err)
		}
		bootstrap := hraft.Configuration{Servers: servers}
		if f := r.BootstrapCluster(bootstrap); f.Error() != nil {
			r.Shutdown()
			return nil, fmt.Errorf("raft node: bootstrap: %w", f.Error())
		}
	}

	// ── forward peers map ────────────────────────────────────────────────────
	// Must be built before the observer goroutine starts — the goroutine reads
	// forwardPeers via poolForLeader and would race with this write otherwise.
	node.forwardPeers = parseKVPairs(cfg.ForwardPeers)

	// ── leader change observation ─────────────────────────────────────────────
	// Always register the observer so we can maintain the connection pool
	// regardless of whether the caller supplied an OnLeaderChange callback.
	//
	// hashicorp/raft never closes observer channels — DeregisterObserver only
	// removes the observer from its internal map. We therefore store both the
	// Observer and the channel so that Shutdown can deregister the observer
	// and then close the channel, which causes the for-range loop below to exit
	// and lets observerWg.Wait() return.
	obs := make(chan hraft.Observation, 8)
	observer := hraft.NewObserver(obs, false, func(o *hraft.Observation) bool {
		_, ok := o.Data.(hraft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	node.observer = observer
	node.observerCh = obs
	node.observerWg.Add(1)
	go func() {
		defer node.observerWg.Done()
		for o := range obs {
			lo, ok := o.Data.(hraft.LeaderObservation)
			if !ok {
				continue
			}
			isLeader := string(lo.LeaderID) == cfg.NodeID
			node.isLeader.Store(isLeader)

			node.logger().Info("memdb raft: leadership changed",
				"nodeID", cfg.NodeID,
				"isLeader", isLeader,
				"leaderID", string(lo.LeaderID),
			)

			// Swap the connection pool to point at the new leader's
			// ForwardAddr. Close the old pool so its idle connections are
			// not handed to callers targeting the previous leader.
			if old := node.pool.Swap(node.poolForLeader(string(lo.LeaderID))); old != nil {
				old.Close()
			}

			if cfg.OnLeaderChange != nil {
				cfg.OnLeaderChange(isLeader)
			}
		}
	}()

	// ── forwarder (optional) ─────────────────────────────────────────────────
	if cfg.ForwardAddr != "" {
		fwd, err := newForwarder(cfg.ForwardAddr, cfg.TLSConfig, node)
		if err != nil {
			r.Shutdown()
			return nil, fmt.Errorf("raft node: forwarder: %w", err)
		}
		node.forwarder = fwd
	}

	return node, nil
}

// Exec submits a SQL write through Raft consensus. Blocks until the entry
// is committed on a quorum of nodes (or the apply timeout is reached).
// If this node is not the leader and ForwardPeers is configured, the write
// is transparently forwarded to the leader using a pooled TLS connection.
// Otherwise ErrNotLeader is returned.
func (n *Node) Exec(sql string, args ...any) error {
	// If we are the leader, apply directly through Raft.
	if n.raft.State() == hraft.Leader {
		entry := replication.WALEntry{
			Seq:       n.seq.Add(1),
			Timestamp: time.Now().UnixNano(),
			SQL:       sql,
			Args:      args,
		}
		return Apply(n.raft, entry, n.cfg.ApplyTimeout)
	}

	// We are not the leader. Forward to the leader using the connection pool.
	leaderAddr, leaderID := n.raft.LeaderWithID()
	if leaderAddr == "" {
		return fmt.Errorf("%w: no leader elected yet", ErrNotLeader)
	}

	n.logger().Debug("memdb raft: forwarding write to leader",
		"leaderID", string(leaderID),
		"sql", sql,
	)

	pool := n.pool.Load()
	if pool == nil {
		// Pool not yet initialised (leader just elected) — try to build one.
		pool = n.poolForLeader(string(leaderID))
		if pool == nil {
			return fmt.Errorf("%w: leader is %s (configure ForwardPeers to enable transparent forwarding)", ErrNotLeader, leaderID)
		}
		// Store only if nobody beat us to it; if they did, use theirs and
		// discard the one we just created.
		if !n.pool.CompareAndSwap(nil, pool) {
			pool.Close()
			pool = n.pool.Load()
		}
	}

	return sendForward(pool, ForwardRequest{
		SQL:  sql,
		Args: args,
	}, n.cfg.ApplyTimeout)
}

// poolForLeader returns a ConnPool for the given leader's ForwardAddr, or nil
// if ForwardPeers is not configured for that leader ID.
func (n *Node) poolForLeader(leaderID string) *ConnPool {
	if leaderID == "" {
		return nil
	}
	fwdAddr, ok := n.forwardPeers[leaderID]
	if !ok {
		return nil
	}
	return NewConnPool(fwdAddr, n.cfg.TLSConfig, defaultPoolSize)
}

// IsLeader reports whether this node is the current Raft leader.
// It reads the atomic bool maintained by the observer goroutine, which is
// faster than calling n.raft.State() (no internal lock contention).
func (n *Node) IsLeader() bool {
	return n.isLeader.Load()
}

// LeaderAddr returns the Raft address of the current leader, or empty string
// if the leader is unknown.
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// AddVoter adds a new voting member to the cluster. Must be called on the leader.
func (n *Node) AddVoter(nodeID, addr string, timeout time.Duration) error {
	f := n.raft.AddVoter(hraft.ServerID(nodeID), hraft.ServerAddress(addr), 0, timeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft node: add voter %s: %w", nodeID, err)
	}
	return nil
}

// RemoveServer removes a node from the cluster. Must be called on the leader.
func (n *Node) RemoveServer(nodeID string, timeout time.Duration) error {
	f := n.raft.RemoveServer(hraft.ServerID(nodeID), 0, timeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft node: remove server %s: %w", nodeID, err)
	}
	return nil
}

// Shutdown gracefully stops this Raft node.
// It is safe to call Shutdown more than once; subsequent calls are no-ops
// that return nil.
func (n *Node) Shutdown() error {
	var retErr error
	n.shutdownOnce.Do(func() {
		if n.forwarder != nil {
			_ = n.forwarder.close()
		}
		// Close and discard the connection pool so idle connections are not leaked.
		if pool := n.pool.Swap(nil); pool != nil {
			pool.Close()
		}
		// Deregister the observer and close its channel before calling
		// r.Shutdown(). hashicorp/raft never closes observer channels itself —
		// DeregisterObserver only removes the observer from the internal map.
		// Closing the channel here causes the for-range loop in the observer
		// goroutine to exit so that observerWg.Wait() below does not deadlock.
		if n.observer != nil {
			n.raft.DeregisterObserver(n.observer)
			close(n.observerCh)
		}
		f := n.raft.Shutdown()
		// Wait for the observer goroutine to finish before returning so callers
		// know no goroutines are still running after Shutdown returns.
		n.observerWg.Wait()
		retErr = f.Error()
	})
	return retErr
}

// Stats returns internal Raft statistics for monitoring.
func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

// ErrNotLeader is returned when a write is attempted on a non-leader node.
var ErrNotLeader = errors.New("raft: not leader")

// parsePeers converts "nodeID=addr" strings into raft.Server entries.
func parsePeers(peers []string) ([]hraft.Server, error) {
	servers := make([]hraft.Server, 0, len(peers))
	for _, p := range peers {
		// Split on the first '=' only.
		idx := -1
		for i, c := range p {
			if c == '=' {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("invalid peer %q: expected nodeID=addr", p)
		}
		id := p[:idx]
		addr := p[idx+1:]
		if id == "" || addr == "" {
			return nil, fmt.Errorf("invalid peer %q: nodeID and addr must be non-empty", p)
		}
		servers = append(servers, hraft.Server{
			ID:      hraft.ServerID(id),
			Address: hraft.ServerAddress(addr),
		})
	}
	if len(servers) == 0 {
		return nil, fmt.Errorf("no peers provided")
	}
	return servers, nil
}

// parseKVPairs parses "key=value" strings into a map, splitting on the first
// '=' character. Entries without '=' or with an empty key are silently skipped.
// Used to build the forwardPeers map from NodeConfig.ForwardPeers.
func parseKVPairs(pairs []string) map[string]string {
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		idx := -1
		for i, c := range p {
			if c == '=' {
				idx = i
				break
			}
		}
		if idx > 0 {
			m[p[:idx]] = p[idx+1:]
		}
	}
	return m
}
