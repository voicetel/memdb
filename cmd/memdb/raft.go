package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/voicetel/memdb"
	mraft "github.com/voicetel/memdb/replication/raft"
)

// raftFlags holds CLI options for enabling Raft replication on a serve node.
// Replication is enabled when NodeID is non-empty; in that case the remaining
// fields are validated by buildRaftNode.
type raftFlags struct {
	NodeID            string
	BindAddr          string
	AdvertiseAddr     string
	ForwardBindAddr   string
	Peers             string
	ForwardPeers      string
	DataDir           string
	TLSCertFile       string
	TLSKeyFile        string
	TLSCAFile         string
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	ApplyTimeout      time.Duration
}

// registerRaftFlags binds raft flags to fs and returns a pointer to the
// parsed values. Replication is opt-in: leaving -raft-node-id empty keeps
// the server in standalone mode.
func registerRaftFlags(fs *flag.FlagSet) *raftFlags {
	r := &raftFlags{}
	fs.StringVar(&r.NodeID, "raft-node-id", "",
		"unique node ID; setting this enables Raft replication")
	fs.StringVar(&r.BindAddr, "raft-bind", "",
		"Raft RPC bind address (e.g. 0.0.0.0:7000)")
	fs.StringVar(&r.AdvertiseAddr, "raft-advertise", "",
		"Raft RPC address advertised to peers (defaults to -raft-bind)")
	fs.StringVar(&r.ForwardBindAddr, "raft-forward-bind", "",
		"write-forwarding bind address (e.g. 0.0.0.0:7001)")
	fs.StringVar(&r.Peers, "raft-peers", "",
		"comma-separated initial cluster, e.g. node-1=10.0.0.1:7000,node-2=10.0.0.2:7000")
	fs.StringVar(&r.ForwardPeers, "raft-forward-peers", "",
		"comma-separated forwarding map, e.g. node-1=10.0.0.1:7001,node-2=10.0.0.2:7001")
	fs.StringVar(&r.DataDir, "raft-data-dir", "",
		"directory for Raft logs, stable state, and snapshots (must persist across restarts)")
	fs.StringVar(&r.TLSCertFile, "raft-tls-cert", "",
		"PEM-encoded certificate for inter-node TLS")
	fs.StringVar(&r.TLSKeyFile, "raft-tls-key", "",
		"PEM-encoded private key for inter-node TLS")
	fs.StringVar(&r.TLSCAFile, "raft-tls-ca", "",
		"PEM-encoded CA bundle used to verify peer certificates")
	fs.DurationVar(&r.SnapshotInterval, "raft-snapshot-interval", 0,
		"how often Raft considers taking a snapshot (0 uses library default)")
	fs.Uint64Var(&r.SnapshotThreshold, "raft-snapshot-threshold", 0,
		"log entries between snapshots (0 uses library default)")
	fs.DurationVar(&r.HeartbeatTimeout, "raft-heartbeat-timeout", 0,
		"Raft heartbeat timeout (0 uses library default)")
	fs.DurationVar(&r.ElectionTimeout, "raft-election-timeout", 0,
		"Raft election timeout (0 uses library default)")
	fs.DurationVar(&r.ApplyTimeout, "raft-apply-timeout", 0,
		"timeout for a single Apply call (0 uses library default)")
	return r
}

// enabled reports whether raft replication should be activated for this run.
func (r raftFlags) enabled() bool { return r.NodeID != "" }

// validate checks that required raft flags are present when replication is
// enabled. It is called before any network I/O so misconfiguration fails fast.
func (r raftFlags) validate() error {
	if !r.enabled() {
		return nil
	}
	missing := []string{}
	if r.BindAddr == "" {
		missing = append(missing, "-raft-bind")
	}
	if r.Peers == "" {
		missing = append(missing, "-raft-peers")
	}
	if r.DataDir == "" {
		missing = append(missing, "-raft-data-dir")
	}
	if r.TLSCertFile == "" {
		missing = append(missing, "-raft-tls-cert")
	}
	if r.TLSKeyFile == "" {
		missing = append(missing, "-raft-tls-key")
	}
	if r.TLSCAFile == "" {
		missing = append(missing, "-raft-tls-ca")
	}
	if len(missing) > 0 {
		return fmt.Errorf("raft replication enabled but missing required flags: %s",
			strings.Join(missing, ", "))
	}
	if r.ForwardBindAddr != "" && r.ForwardPeers == "" {
		return fmt.Errorf("-raft-forward-bind is set but -raft-forward-peers is empty")
	}
	if r.ForwardPeers != "" && r.ForwardBindAddr == "" {
		return fmt.Errorf("-raft-forward-peers is set but -raft-forward-bind is empty")
	}
	return nil
}

// loadRaftTLS builds a mutual-TLS config from on-disk PEM files. The same
// config is used for both the dialer and the listener (Raft requires a single
// TLS config covering both directions).
func loadRaftTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load tls keypair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ca bundle %q contained no usable certificates", caFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// raftDBAdapter bridges *memdb.DB to the mraft.DB interface. ExecLocal maps to
// ExecDirect so the FSM apply path bypasses the OnExec hook and avoids the
// Raft → Exec → Raft loop.
type raftDBAdapter struct{ db *memdb.DB }

func (a raftDBAdapter) ExecLocal(sql string, args ...any) error {
	return a.db.ExecDirect(sql, args...)
}

func (a raftDBAdapter) Serialize() ([]byte, error) { return a.db.Serialize() }
func (a raftDBAdapter) Restore(data []byte) error  { return a.db.Restore(data) }

// splitCSV trims and drops empty entries from a comma-separated list. Used so
// "a,b," and "a, b" both yield ["a", "b"] without leaking the trailing empty
// token into mraft.NewNode (which would reject it as a malformed peer).
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// buildRaftNode constructs a Raft node bound to db. The caller must invoke
// node.Shutdown() before db.Close() so in-flight FSM applies can drain.
func buildRaftNode(db *memdb.DB, r raftFlags, logger *slog.Logger) (*mraft.Node, error) {
	tlsCfg, err := loadRaftTLS(r.TLSCertFile, r.TLSKeyFile, r.TLSCAFile)
	if err != nil {
		return nil, err
	}
	cfg := mraft.NodeConfig{
		NodeID:            r.NodeID,
		BindAddr:          r.BindAddr,
		AdvertiseAddr:     r.AdvertiseAddr,
		ForwardAddr:       r.ForwardBindAddr,
		Peers:             splitCSV(r.Peers),
		ForwardPeers:      splitCSV(r.ForwardPeers),
		DataDir:           r.DataDir,
		TLSConfig:         tlsCfg,
		SnapshotInterval:  r.SnapshotInterval,
		SnapshotThreshold: r.SnapshotThreshold,
		HeartbeatTimeout:  r.HeartbeatTimeout,
		ElectionTimeout:   r.ElectionTimeout,
		ApplyTimeout:      r.ApplyTimeout,
		Logger:            logger,
		OnLeaderChange: func(isLeader bool) {
			logger.Info("memdb raft: leadership changed",
				"nodeID", r.NodeID, "isLeader", isLeader)
		},
		OnApplyError: func(err error) {
			logger.Error("memdb raft: apply error", "error", err)
		},
	}
	return mraft.NewNode(raftDBAdapter{db: db}, cfg)
}
