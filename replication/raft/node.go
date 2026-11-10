package raft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	// including this node. It is only the starting map: a node that joins the
	// cluster later is added on each running node with SetForwardPeer, and one
	// that leaves is dropped with RemoveForwardPeer.
	// Example: ["node-1=10.0.0.1:7001", "node-2=10.0.0.2:7001"]
	ForwardPeers []string

	// Peers is the list of all nodes in the initial cluster, including this
	// node. Format: "nodeID=addr", e.g. ["node-1=10.0.0.1:7000", "node-2=10.0.0.2:7000"].
	// Only used during first bootstrap. Ignored if state already exists.
	// Required unless Join is set.
	Peers []string

	// Join starts a node that has no Raft state without bootstrapping a
	// cluster. It holds no configuration and starts no election until the
	// leader of a running cluster adds it with AddVoter, and from then on it
	// is an ordinary member. Use it for a node that replaces a lost one or
	// grows a running cluster: bootstrapping would give it a first
	// configuration of its own at index 1 and term 1, the same index and term
	// as the cluster's first entry, so the leader would never send its own and
	// Raft's log matching would not hold at that index. Peers is never read
	// in join mode: it may be left empty, and if set it is not bootstrapped
	// from, so its size need not be odd. A node that already has state ignores
	// Join, so it can stay set across restarts.
	//
	// Until it is added the node is idle: it holds no data, answers no vote,
	// and refuses writes. NewNode logs that at Warn, since nothing else will
	// say why a node started this way by mistake never does anything.
	Join bool

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

	// DisableShutdownHandover makes Shutdown on a leader stop without handing
	// leadership to another voter first, as a crash would: the cluster then
	// has no leader until an election timeout passes and a vote is won. Leave
	// it unset in production. It is for rehearsing a leader's sudden loss, and
	// for an operator who wants a node gone at once whatever it leads.
	DisableShutdownHandover bool

	// ForwardPoolSize is how many idle connections to the leader a follower
	// keeps warm for forwarding writes. It does not cap concurrent forwards:
	// a forward that finds none idle dials a connection of its own, which is
	// closed afterwards rather than kept, at the cost of a TLS handshake. Raise
	// it toward a follower's usual number of concurrent writes. Default: 8.
	ForwardPoolSize int

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
	if c.ForwardPoolSize <= 0 {
		c.ForwardPoolSize = defaultPoolSize
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

// ResultDB is an optional extension of DB. When the value passed to NewNode
// also implements it, the FSM applies entries through ExecLocalResult and
// the rows-affected count is returned to Exec/ExecResult callers on the
// leader (and to forwarded followers via ForwardResponse). memdb.DB
// satisfies it via ExecDirectResult.
type ResultDB interface {
	// ExecLocalResult applies SQL directly to the local database, bypassing
	// Raft, and returns the number of rows the statement affected.
	ExecLocalResult(sql string, args ...any) (int64, error)
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
	cfg       NodeConfig
	raft      *hraft.Raft
	transport *hraft.NetworkTransport
	forwarder *forwarder // nil when ForwardAddr is empty

	// fwdMu guards forwardPeers, poolLeader, and poolClosed, and is held for
	// every store to pool, so a pool is never installed for a peer address
	// that has just changed, nor after Shutdown has closed the last one. The
	// write path only loads pool and takes no lock while one is installed.
	fwdMu        sync.Mutex
	forwardPeers map[string]string        // nodeID → forwardAddr
	poolLeader   string                   // the leader pool was last built for
	poolClosed   bool                     // set by Shutdown; no pool is installed after it
	pool         atomic.Pointer[ConnPool] // current leader's connection pool
	isLeader     atomic.Bool
	observerWg   sync.WaitGroup
	observer     *hraft.Observer // stored so Shutdown can deregister it
	observerDone chan struct{}   // closed by Shutdown to signal observer goroutine to exit
	shutdownOnce sync.Once       // ensures Shutdown is idempotent
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
// bootstrapped using NodeConfig.Peers, unless NodeConfig.Join is set, in which
// case the node waits to be added to a running cluster. On subsequent starts
// the existing state is recovered automatically.
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

	// Clone the caller's TLS config and ensure a ClientSessionCache is set so
	// outbound dials (transport peer dials and forwarder ConnPool dials) can
	// resume TLS sessions instead of paying a full handshake every time the
	// pool drops a stale conn or rebuilds after a leader change. Cloning
	// rather than mutating preserves the caller's struct — Config docs are
	// explicit that callers must not mutate a Config in use, and we treat
	// the input the same way. Idle pool windows (30s) and leader rotations
	// are exactly when cold dials happen, so the saving is on the slow path
	// rather than the steady state.
	cfg.TLSConfig = cfg.TLSConfig.Clone()
	if cfg.TLSConfig.ClientSessionCache == nil {
		cfg.TLSConfig.ClientSessionCache = tls.NewLRUClientSessionCache(0)
	}
	// Peers must be provided unless the node joins — even on restart they are
	// used to validate the caller's intent, and on first boot they define the
	// cluster membership. A joining node takes its membership from the leader
	// that adds it and never reads Peers, so there it is optional.
	if len(cfg.Peers) == 0 && !cfg.Join {
		return nil, fmt.Errorf("raft node: Peers must not be empty (set Join to add this node to a running cluster instead)")
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
			_ = c.Close()
		}
		return nil, fmt.Errorf("raft node: stable store: %w", err)
	}

	snapStore, err := hraft.NewFileSnapshotStoreWithLogger(
		filepath.Join(cfg.DataDir, "snapshots"), 3,
		logging.NewHCLogAdapter(cfg.Logger, "raft.snapshot"),
	)
	if err != nil {
		if c, ok := logStore.(storeCloser); ok {
			_ = c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			_ = c.Close()
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
			_ = c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			_ = c.Close()
		}
		return nil, fmt.Errorf("raft node: check state: %w", err)
	}
	if !hasState && !cfg.Join {
		// Even-sized clusters have no fault-tolerance advantage over the next
		// smaller odd size. Only enforce this on first boot — a node restarting
		// into an existing cluster uses whatever membership is already stored,
		// and a joining one takes the membership the leader sends it.
		if len(cfg.Peers)%2 == 0 {
			if c, ok := logStore.(storeCloser); ok {
				_ = c.Close()
			}
			if c, ok := stableStore.(storeCloser); ok {
				_ = c.Close()
			}
			return nil, fmt.Errorf("raft node: cluster size must be odd (got %d peers) — "+
				"even-sized clusters have no fault tolerance advantage over the next smaller odd size; "+
				"use 1, 3, 5, or 7 nodes", len(cfg.Peers))
		}
		// Parse the peers here too, before any socket is bound or Raft
		// started, so a malformed entry costs nothing to refuse.
		if _, err := parsePeers(cfg.Peers); err != nil {
			if c, ok := logStore.(storeCloser); ok {
				_ = c.Close()
			}
			if c, ok := stableStore.(storeCloser); ok {
				_ = c.Close()
			}
			return nil, fmt.Errorf("raft node: parse peers: %w", err)
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
			_ = c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			_ = c.Close()
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

	// Prefer the count-carrying apply when the DB supports it so the
	// rows-affected count reaches ExecResult callers; fall back to the
	// legacy error-only apply (count always 0) otherwise.
	execFn := func(sql string, args ...any) (int64, error) {
		return 0, db.ExecLocal(sql, args...)
	}
	if rdb, ok := db.(ResultDB); ok {
		execFn = rdb.ExecLocalResult
	}
	fsm := NewResultFSM(
		execFn,
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
		_ = transport.Close()
		if c, ok := logStore.(storeCloser); ok {
			_ = c.Close()
		}
		if c, ok := stableStore.(storeCloser); ok {
			_ = c.Close()
		}
		return nil, fmt.Errorf("raft node: new raft: %w", err)
	}
	node.raft = r

	// ── bootstrap if no existing state ───────────────────────────────────────
	switch {
	case !hasState && cfg.Join:
		// Warn, not Info: until some leader adds it this node does nothing at
		// all, and a node left in join mode by mistake looks exactly like one
		// that is waiting, so say so where an operator will see it.
		node.logger().Warn("memdb raft: started in join mode with no existing state; "+
			"this node stays idle (no data, no votes, writes refused) until the leader of a running cluster adds it with AddVoter",
			"nodeID", cfg.NodeID,
			"advertise", string(transport.LocalAddr()),
			"peersIgnored", len(cfg.Peers),
		)
	case !hasState:
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
	// From here on it is read and changed only under fwdMu (SetForwardPeer).
	node.forwardPeers = parseKVPairs(cfg.ForwardPeers)

	// ── leader change observation ─────────────────────────────────────────────
	// Always register the observer so we can maintain the connection pool
	// regardless of whether the caller supplied an OnLeaderChange callback.
	//
	// hashicorp/raft never closes observer channels — DeregisterObserver only
	// removes the observer from its internal map. Closing the observer channel
	// ourselves is unsafe because hashicorp/raft may be mid-send and would
	// panic on a closed channel. Instead, we signal the goroutine via a
	// dedicated observerDone channel that it selects on, and never close obs.
	obs := make(chan hraft.Observation, 8)
	observer := hraft.NewObserver(obs, false, func(o *hraft.Observation) bool {
		_, ok := o.Data.(hraft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	node.observer = observer
	node.observerDone = make(chan struct{})
	node.observerWg.Add(1)
	go func() {
		defer node.observerWg.Done()
		for {
			select {
			case o := <-obs:
				// Recover from any panic inside the observation handler so
				// a misbehaving OnLeaderChange callback or an unexpected nil
				// dereference does not bring down the entire Raft node.
				func() {
					defer func() {
						if r := recover(); r != nil {
							node.logger().Error("memdb raft: panic in observer goroutine",
								"panic", r,
								"nodeID", cfg.NodeID,
							)
						}
					}()

					lo, ok := o.Data.(hraft.LeaderObservation)
					if !ok {
						return
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
					node.setLeaderPool(string(lo.LeaderID))

					if cfg.OnLeaderChange != nil {
						// Wrap in another recover so a panicking OnLeaderChange
						// callback does not suppress the pool-swap that
						// happened above.
						func() {
							defer func() {
								if r := recover(); r != nil {
									node.logger().Error("memdb raft: panic in OnLeaderChange",
										"panic", r,
										"nodeID", cfg.NodeID,
									)
								}
							}()
							cfg.OnLeaderChange(isLeader)
						}()
					}
				}()
			case <-node.observerDone:
				return
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
	_, err := n.ExecResult(sql, args...)
	return err
}

// ExecResult is Exec returning the number of rows the committed statement
// affected. The count originates from the FSM apply on the leader (see
// ResultDB); on the forwarded-follower path it arrives in ForwardResponse.
// A cluster whose DB does not implement ResultDB reports 0.
func (n *Node) ExecResult(sql string, args ...any) (int64, error) {
	return n.ExecResultContext(context.Background(), sql, args...)
}

// ExecResultContext is ExecResult bounded by ctx as well as by ApplyTimeout. On
// the leader it stops waiting for the commit once ctx ends; on a follower it
// ends the forward. Either way the error wraps ctx's, and the write may still
// commit, since Raft cannot recall an entry it has taken.
func (n *Node) ExecResultContext(ctx context.Context, sql string, args ...any) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// If we are the leader, apply directly through Raft.
	if n.raft.State() == hraft.Leader {
		entry := replication.WALEntry{
			// Seq is left zero. The Raft log index provides ordering across
			// leadership changes; a per-Node counter could not do so and
			// would restart at 0 after each restart or new leader. The
			// field is retained only for gob-encoding compatibility with
			// existing WAL/snapshot files.
			Timestamp: time.Now().UnixNano(),
			SQL:       sql,
			Args:      args,
		}
		return ApplyResultContext(ctx, n.raft, entry, n.cfg.ApplyTimeout)
	}

	// We are not the leader. Check the arguments as the leader will encode
	// them into the log first, so an argument the log cannot carry is refused
	// here with the leader's own error, not another one from the forwarding
	// hop, and a statement behaves alike on every node.
	if _, err := replication.EncodeEntry(nil, replication.WALEntry{SQL: sql, Args: args}); err != nil {
		return 0, fmt.Errorf("raft: encode entry: %w", err)
	}

	// Forward to the leader using the connection pool.
	leaderAddr, leaderID := n.raft.LeaderWithID()
	if leaderAddr == "" {
		return 0, fmt.Errorf("%w: no leader elected yet", ErrNotLeader)
	}

	n.logger().Debug("memdb raft: forwarding write to leader",
		"leaderID", string(leaderID),
		"sql", sql,
	)

	pool := n.pool.Load()
	if pool == nil {
		// Pool not yet initialised (leader just elected, or its forward
		// address only just set) — try to build one.
		var err error
		if pool, err = n.leaderPool(string(leaderID)); err != nil {
			return 0, err
		}
	}

	return sendForward(ctx, pool, ForwardRequest{
		SQL:  sql,
		Args: args,
	}, n.cfg.ApplyTimeout)
}

// poolForLeader returns a ConnPool for the given leader's ForwardAddr, or nil
// if no forward address is known for that leader ID. The caller holds fwdMu.
func (n *Node) poolForLeader(leaderID string) *ConnPool {
	if leaderID == "" {
		return nil
	}
	fwdAddr, ok := n.forwardPeers[leaderID]
	if !ok {
		return nil
	}
	return NewConnPool(fwdAddr, n.cfg.TLSConfig, n.cfg.ForwardPoolSize)
}

// setLeaderPool replaces the pool with one for leaderID, or with none when no
// forward address is known for it, and closes the pool it replaces. The
// observer goroutine calls it on every leader change. After Shutdown has
// closed the pool it installs nothing: a leader change seen between that and
// the observer stopping, which the handover in Shutdown makes likely, would
// otherwise leave a pool nobody closes.
func (n *Node) setLeaderPool(leaderID string) {
	n.fwdMu.Lock()
	var next *ConnPool
	if !n.poolClosed {
		next = n.poolForLeader(leaderID)
	}
	n.poolLeader = leaderID
	old := n.pool.Swap(next)
	n.fwdMu.Unlock()
	if old != nil {
		old.Close()
	}
}

// leaderPool returns the pool to forward to leaderID through, building it if
// there is none. It fails when no forward address is known for the leader, and
// once the node is shut down.
func (n *Node) leaderPool(leaderID string) (*ConnPool, error) {
	n.fwdMu.Lock()
	defer n.fwdMu.Unlock()
	if n.poolClosed {
		return nil, fmt.Errorf("raft: forward: %w", hraft.ErrRaftShutdown)
	}
	if pool := n.pool.Load(); pool != nil {
		return pool, nil
	}
	pool := n.poolForLeader(leaderID)
	if pool == nil {
		return nil, fmt.Errorf("%w: leader is %s, and this node has no forward address for it "+
			"(call SetForwardPeer(%q, addr) on this node, or add it to ForwardPeers and restart)",
			ErrNotLeader, leaderID, leaderID)
	}
	n.poolLeader = leaderID
	n.pool.Store(pool)
	return pool, nil
}

// closePool closes the pool for good: nothing installs another afterwards.
func (n *Node) closePool() {
	n.fwdMu.Lock()
	n.poolClosed = true
	old := n.pool.Swap(nil)
	n.fwdMu.Unlock()
	if old != nil {
		old.Close()
	}
}

// SetForwardPeer tells this node where nodeID listens for forwarded writes (its
// ForwardAddr), adding it to the map NodeConfig.ForwardPeers started, or
// replacing its address. It takes effect on the next write and is safe to call
// at any time, from any goroutine.
//
// ForwardPeers is read once, when the node starts, so a node that was running
// before another joined has no address for it: once the joiner leads, which
// any handover can bring about, every write on the older node fails with
// ErrNotLeader until this is called. After AddVoter, call it on every running
// node for the node added. It changes this node's memory only: put the address
// in ForwardPeers as well, so a restart keeps it.
func (n *Node) SetForwardPeer(nodeID, addr string) error {
	if nodeID == "" || addr == "" {
		return fmt.Errorf("raft node: set forward peer: nodeID and addr must be non-empty")
	}
	n.fwdMu.Lock()
	changed := n.forwardPeers[nodeID] != addr
	n.forwardPeers[nodeID] = addr
	var old *ConnPool
	if changed && n.poolLeader == nodeID {
		// The pool dials the leader's old address; the next write builds one
		// for the new address.
		old = n.pool.Swap(nil)
	}
	n.fwdMu.Unlock()
	if old != nil {
		old.Close()
	}
	return nil
}

// RemoveForwardPeer forgets nodeID's forward address, and closes the idle
// connections to it if it is the leader this node forwards to. Pair it with
// RemoveServer, on every node, so no node keeps dialling one that has left.
// Removing an unknown node does nothing.
func (n *Node) RemoveForwardPeer(nodeID string) {
	n.fwdMu.Lock()
	delete(n.forwardPeers, nodeID)
	var old *ConnPool
	if n.poolLeader == nodeID {
		old = n.pool.Swap(nil)
	}
	n.fwdMu.Unlock()
	if old != nil {
		old.Close()
	}
}

// ForwardPeers returns a copy of the forward addresses this node knows, by
// node ID.
func (n *Node) ForwardPeers() map[string]string {
	n.fwdMu.Lock()
	defer n.fwdMu.Unlock()
	peers := make(map[string]string, len(n.forwardPeers))
	for id, addr := range n.forwardPeers {
		peers[id] = addr
	}
	return peers
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

// AddVoter adds a new voting member to the cluster. Must be called on the
// leader; anywhere else the error is ErrNotLeader. Each node already running
// also needs the new node's forward address: see SetForwardPeer.
func (n *Node) AddVoter(nodeID, addr string, timeout time.Duration) error {
	f := n.raft.AddVoter(hraft.ServerID(nodeID), hraft.ServerAddress(addr), 0, timeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft node: add voter %s: %w", nodeID, notLeader(err))
	}
	return nil
}

// TransferLeadership hands leadership to the voter with the most of the log, and
// returns once this node has stepped down. It must be called on the leader.
// hashicorp/raft brings the target up to date, tells it to start an election at
// once, and refuses writes on this node meanwhile; it gives up after about the
// election timeout, so a cluster with no other voter, or none that answers,
// fails it in bounded time. Called on a node that is not the leader it fails
// with ErrNotLeader.
func (n *Node) TransferLeadership() error {
	if err := n.raft.LeadershipTransfer().Error(); err != nil {
		return fmt.Errorf("raft node: transfer leadership: %w", notLeader(err))
	}
	return nil
}

// TransferLeadershipTo is TransferLeadership to a named voter.
func (n *Node) TransferLeadershipTo(nodeID string) error {
	cfg := n.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return fmt.Errorf("raft node: transfer leadership: %w", err)
	}
	for _, s := range cfg.Configuration().Servers {
		if string(s.ID) == nodeID {
			if err := n.raft.LeadershipTransferToServer(s.ID, s.Address).Error(); err != nil {
				return fmt.Errorf("raft node: transfer leadership to %s: %w", nodeID, notLeader(err))
			}
			return nil
		}
	}
	return fmt.Errorf("raft node: transfer leadership: %s is not a member", nodeID)
}

// RemoveServer removes a node from the cluster. Must be called on the leader;
// anywhere else the error is ErrNotLeader. Pair it with RemoveForwardPeer on
// every remaining node.
func (n *Node) RemoveServer(nodeID string, timeout time.Duration) error {
	f := n.raft.RemoveServer(hraft.ServerID(nodeID), 0, timeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft node: remove server %s: %w", nodeID, notLeader(err))
	}
	return nil
}

// Shutdown gracefully stops this Raft node.
// It is safe to call Shutdown more than once; subsequent calls are no-ops
// that return nil.
//
// A leader first hands leadership to another voter, so the cluster has a new
// leader at once rather than after an election timeout with no leader. It asks
// hashicorp/raft for its own choice first, the voter with the most of the log;
// that choice takes no account of whether the voter is alive, so if it fails
// each other voter is asked by name. The whole handover, however many voters
// are tried, is bounded by shutdownHandoverTimeouts (3) election timeouts, 1.5 s
// with the default ElectionTimeout: when that is spent, or no voter takes
// over, the failure is logged and the node stops anyway. A leader with no
// other voter has nobody to hand over to and skips this, as does any node with
// NodeConfig.DisableShutdownHandover set.
func (n *Node) Shutdown() error {
	var retErr error
	n.shutdownOnce.Do(func() {
		if !n.cfg.DisableShutdownHandover {
			n.handOverLeadership()
		}
		if n.forwarder != nil {
			_ = n.forwarder.close()
		}
		// Close and discard the connection pool so idle connections are not
		// leaked. The observer goroutine is still running, and the handover
		// above makes a leader change about now likely; closePool also stops
		// it installing a new pool that nobody would close.
		n.closePool()
		// Deregister the observer FIRST so hashicorp/raft stops sending
		// observations into obs. Then signal the observer goroutine to exit
		// via the dedicated observerDone channel. We deliberately do NOT
		// close obs — hashicorp/raft may still hold a reference and closing
		// it would cause a panic on a concurrent send.
		if n.observer != nil {
			n.raft.DeregisterObserver(n.observer)
			close(n.observerDone)
		}
		// Wait for the observer goroutine to finish before tearing down raft
		// so it cannot observe a half-shutdown state.
		n.observerWg.Wait()
		f := n.raft.Shutdown()
		retErr = f.Error()
	})
	return retErr
}

// shutdownHandoverTimeouts is how many election timeouts Shutdown may spend
// handing leadership over, in total.
const shutdownHandoverTimeouts = 3

// handOverLeadership is the handover Shutdown makes on a leader: see Shutdown.
func (n *Node) handOverLeadership() {
	if n.raft.State() != hraft.Leader {
		return
	}
	voters, err := n.otherVoters()
	if err != nil {
		n.logger().Warn("memdb raft: could not hand leadership over before shutting down", "error", err)
		return
	}
	if len(voters) == 0 {
		// The only voter: there is nobody to hand over to, and nothing wrong.
		n.logger().Debug("memdb raft: no other voter to hand leadership over to", "nodeID", n.cfg.NodeID)
		return
	}
	deadline := time.Now().Add(shutdownHandoverTimeouts * n.cfg.ElectionTimeout)
	err = n.transferBy(deadline, n.raft.LeadershipTransfer)
	// hashicorp/raft's own choice goes to the first voter in configuration
	// order among those with the most of the log, alive or not, so the ones
	// it did not choose are reached soonest from the other end.
	for i := len(voters) - 1; i >= 0 && err != nil; i-- {
		if n.raft.State() != hraft.Leader || !time.Now().Before(deadline) {
			break
		}
		target := voters[i]
		n.logger().Info("memdb raft: handing leadership over failed; trying another voter",
			"error", err, "target", string(target.ID))
		err = n.transferBy(deadline, func() hraft.Future {
			return n.raft.LeadershipTransferToServer(target.ID, target.Address)
		})
	}
	if err != nil && n.raft.State() == hraft.Leader {
		n.logger().Warn("memdb raft: could not hand leadership over before shutting down",
			"error", err, "votersTried", len(voters))
	}
}

// otherVoters returns the cluster's voters other than this node, in
// configuration order.
func (n *Node) otherVoters() ([]hraft.Server, error) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	var voters []hraft.Server
	for _, s := range f.Configuration().Servers {
		if s.Suffrage == hraft.Voter && string(s.ID) != n.cfg.NodeID {
			voters = append(voters, s)
		}
	}
	return voters, nil
}

// errHandoverBudget is the outcome of a handover that ran out of time.
var errHandoverBudget = errors.New("raft node: transfer leadership: out of time")

// transferBy starts a leadership transfer and waits for its outcome, but no
// later than deadline. hashicorp/raft answers a transfer within an election
// timeout by itself; the deadline is what holds if it ever does not. A transfer
// refused because the one before it is still winding down inside raft, which it
// does for a moment after reporting its outcome, is started again.
func (n *Node) transferBy(deadline time.Time, start func() hraft.Future) error {
	for {
		outcome := make(chan error, 1)
		go func() { outcome <- start().Error() }()
		var err error
		select {
		case err = <-outcome:
		case <-time.After(time.Until(deadline)):
			return errHandoverBudget
		}
		if !errors.Is(err, hraft.ErrLeadershipTransferInProgress) && !errors.Is(err, hraft.ErrEnqueueTimeout) {
			return err
		}
		if time.Until(deadline) < 10*time.Millisecond {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Stats returns internal Raft statistics for monitoring.
func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

// ErrNotLeader is returned when a write is attempted on a non-leader node
// that cannot forward it, when the leader refuses a write because it is
// handing leadership over, and when a call only the leader can serve
// (AddVoter, RemoveServer, TransferLeadership) is made elsewhere. In each case
// the operation did not happen and can be retried once there is a leader.
// Where hashicorp/raft reported the condition, errors.Is also matches its own
// error (raft.ErrNotLeader or raft.ErrLeadershipTransferInProgress).
var ErrNotLeader = errors.New("raft: not leader")

// notLeaderError is an error from hashicorp/raft that means this node is not,
// or is ceasing to be, the leader. It reads exactly as the error it holds, and
// matches both that error and ErrNotLeader.
type notLeaderError struct{ err error }

func (e *notLeaderError) Error() string   { return e.err.Error() }
func (e *notLeaderError) Unwrap() []error { return []error{e.err, ErrNotLeader} }

// notLeader returns err matching ErrNotLeader as well, if it is one of the
// errors hashicorp/raft refuses an operation with for want of leadership:
// ErrNotLeader, and ErrLeadershipTransferInProgress, which a leader answers
// while it hands leadership over. Both refuse before anything is written to the
// log, so the caller may retry. ErrLeadershipLost is deliberately not one: it
// leaves an entry that may yet commit. Any other error is returned unchanged.
func notLeader(err error) error {
	if errors.Is(err, hraft.ErrNotLeader) || errors.Is(err, hraft.ErrLeadershipTransferInProgress) {
		return &notLeaderError{err: err}
	}
	return err
}

// parsePeers converts "nodeID=addr" strings into raft.Server entries.
func parsePeers(peers []string) ([]hraft.Server, error) {
	servers := make([]hraft.Server, 0, len(peers))
	for _, p := range peers {
		// Split on the first '=' only. strings.IndexByte is a single
		// memchr() on the underlying bytes — faster and clearer than a
		// rune-range loop, and safe here because '=' is a plain ASCII byte.
		idx := strings.IndexByte(p, '=')
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
		idx := strings.IndexByte(p, '=')
		if idx > 0 {
			m[p[:idx]] = p[idx+1:]
		}
	}
	return m
}
