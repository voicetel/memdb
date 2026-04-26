// Three-node Raft cluster, each node serving the PostgreSQL wire
// protocol — all running in a single process for demo purposes.
//
// Why a single process? Because every memdb feature in play here
// (consensus, leader election, write forwarding, mutual TLS, the PG
// wire protocol) is just a Go library. The same shape would deploy
// across three machines by separating the calls to startNode and
// passing real addresses; the wiring is identical.
//
// What you can demonstrate:
//
//   - Connect psql to any node — read or write.
//   - Writes against a follower are forwarded to the leader over
//     mutual TLS, replicated, and applied on every node before
//     ReadyForQuery returns.
//   - SELECT against any follower sees the committed write.
//
// Run:
//
//	go run ./examples/cluster
//
// Connect (replace 5433 with 5434 or 5435 to talk to a follower):
//
//	psql -h 127.0.0.1 -p 5433 -U memdb -d memdb \
//	     -c "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"
//	psql -h 127.0.0.1 -p 5434 -U memdb -d memdb \
//	     -c "INSERT INTO kv VALUES ('hello','world')"
//	psql -h 127.0.0.1 -p 5435 -U memdb -d memdb \
//	     -c "SELECT * FROM kv"
//
// Press Ctrl-C to shut down all three nodes cleanly.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/voicetel/memdb"
	mraft "github.com/voicetel/memdb/replication/raft"
	pgserver "github.com/voicetel/memdb/server"
)

func main() {
	dataRoot := flag.String("data", "", "directory for raft state and snapshots; defaults to a fresh tempdir")
	pgBase := flag.Int("pg-port", 5433, "first PG-wire port (nodes get N, N+1, N+2)")
	raftBase := flag.Int("raft-port", 7000, "first Raft RPC port (nodes get N, N+1, N+2)")
	fwdBase := flag.Int("forward-port", 7100, "first write-forwarding port (nodes get N, N+1, N+2)")
	flag.Parse()

	root, cleanup, err := resolveDataRoot(*dataRoot)
	if err != nil {
		log.Fatalf("data dir: %v", err)
	}
	defer cleanup()

	tlsCfg := generateClusterTLS()

	// Compose peer lists once — every node shares the same membership
	// view at bootstrap. With raft, this list is consulted only on
	// first start; subsequent runs read membership from on-disk state
	// in DataDir, so callers can change the addresses across restarts
	// without breaking the cluster (as long as they update
	// raft-stable / raft-log accordingly — out of scope here).
	const n = 3
	ids := make([]string, n)
	raftAddrs := make([]string, n)
	fwdAddrs := make([]string, n)
	pgAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node-%d", i+1)
		raftAddrs[i] = fmt.Sprintf("127.0.0.1:%d", *raftBase+i)
		fwdAddrs[i] = fmt.Sprintf("127.0.0.1:%d", *fwdBase+i)
		pgAddrs[i] = fmt.Sprintf("127.0.0.1:%d", *pgBase+i)
	}
	peers := make([]string, n)
	fwdPeers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = ids[i] + "=" + raftAddrs[i]
		fwdPeers[i] = ids[i] + "=" + fwdAddrs[i]
	}

	// Bring up the three nodes. Each runs its own goroutine for the
	// PG listener; the raft node manages its own goroutines internally.
	nodes := make([]running, n)
	for i := 0; i < n; i++ {
		r, err := startNode(root, i, ids[i], raftAddrs[i], fwdAddrs[i], pgAddrs[i],
			peers, fwdPeers, tlsCfg)
		if err != nil {
			log.Fatalf("start %s: %v", ids[i], err)
		}
		nodes[i] = r
		log.Printf("%s: raft=%s forward=%s pg=%s", ids[i], raftAddrs[i], fwdAddrs[i], pgAddrs[i])
	}

	// Wait for leader election so the first psql connection doesn't
	// race the bootstrap and see "no leader yet".
	if err := waitForLeader(nodes, 20*time.Second); err != nil {
		log.Fatalf("waitForLeader: %v", err)
	}
	for i, r := range nodes {
		if r.rn.IsLeader() {
			log.Printf("leader: %s", ids[i])
		}
	}

	log.Print("cluster ready. Try:")
	log.Printf("  psql -h 127.0.0.1 -p %d -U memdb -d memdb -c 'CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)'", *pgBase)
	log.Printf("  psql -h 127.0.0.1 -p %d -U memdb -d memdb -c \"INSERT INTO kv VALUES ('hello','world')\"", *pgBase+1)
	log.Printf("  psql -h 127.0.0.1 -p %d -U memdb -d memdb -c 'SELECT * FROM kv'", *pgBase+2)

	// Run PG listeners in goroutines. server.ListenAndServe blocks
	// until Stop is called.
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := nodes[i].srv.ListenAndServe(); err != nil {
				log.Printf("%s pg listener exited: %v", ids[i], err)
			}
		}(i)
	}

	// Graceful shutdown: stop PG listeners → shutdown raft nodes →
	// close DBs → run final flush via memdb.Close. Raft must come
	// down before the DB so in-flight FSM applies have a writer to
	// land on.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Print("shutdown signal received")
	for i, r := range nodes {
		r.srv.Stop()
		if err := r.rn.Shutdown(); err != nil {
			log.Printf("%s raft shutdown: %v", ids[i], err)
		}
		if err := r.db.Close(); err != nil {
			log.Printf("%s db close: %v", ids[i], err)
		}
	}
	wg.Wait()
	log.Print("shutdown complete")
}

// running bundles the per-node handles main needs to manage shutdown
// — one DB, one raft node, one PG listener. Pulled out into a named
// type so helper signatures stay short.
type running struct {
	db  *memdb.DB
	rn  *mraft.Node
	srv *pgserver.Server
}

// startNode brings up one cluster node: opens an in-memory DB,
// creates its Raft membership, wires OnExec to forward writes through
// consensus, and prepares (but does not start) a PG-wire listener.
//
// Why prepare-but-not-start the listener? Because we want a single
// log message per node before any goroutine starts accepting traffic
// — keeps the demo output predictable.
func startNode(
	root string,
	index int,
	nodeID, raftAddr, fwdAddr, pgAddr string,
	peers, fwdPeers []string,
	tlsCfg *tls.Config,
) (running, error) {
	dataDir := filepath.Join(root, nodeID)
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return running{}, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}

	// nodeExec is captured by OnExec and assigned after the raft node
	// is built. The PG server is started later, so this nil-assignment
	// window is invisible to any concurrent goroutine.
	var nodeExec func(sql string, args ...any) error

	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(dataDir, "memdb.db"),
		FlushInterval: 0,
		// Raft's log is the durability layer in clustered mode, so
		// memdb's WAL would be a redundant per-write fsync.
		Durability: memdb.DurabilityNone,
		OnExec: func(sql string, args []any) error {
			return nodeExec(sql, args...)
		},
	})
	if err != nil {
		return running{}, fmt.Errorf("memdb.Open: %w", err)
	}

	rn, err := mraft.NewNode(memdbAdapter{db: db}, mraft.NodeConfig{
		NodeID:       nodeID,
		BindAddr:     raftAddr,
		ForwardAddr:  fwdAddr,
		Peers:        peers,
		ForwardPeers: fwdPeers,
		DataDir:      filepath.Join(dataDir, "raft"),
		TLSConfig:    tlsCfg,
		OnApplyError: func(err error) {
			log.Printf("%s FSM apply error: %v", nodeID, err)
		},
	})
	if err != nil {
		_ = db.Close()
		return running{}, fmt.Errorf("raft NewNode: %w", err)
	}
	nodeExec = rn.Exec

	srv := pgserver.New(db, pgserver.Config{ListenAddr: pgAddr})

	return running{db: db, rn: rn, srv: srv}, nil
}

// memdbAdapter bridges *memdb.DB to mraft.DB. ExecLocal maps to
// ExecDirect so the FSM apply path bypasses the OnExec hook (which
// would otherwise forward the write through Raft a second time).
type memdbAdapter struct{ db *memdb.DB }

func (a memdbAdapter) ExecLocal(sql string, args ...any) error {
	return a.db.ExecDirect(sql, args...)
}
func (a memdbAdapter) Serialize() ([]byte, error) { return a.db.Serialize() }
func (a memdbAdapter) Restore(data []byte) error  { return a.db.Restore(data) }

func waitForLeader(nodes []running, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := 0
		allHave := true
		for _, r := range nodes {
			if r.rn.IsLeader() {
				leaders++
			}
			if r.rn.LeaderAddr() == "" {
				allHave = false
			}
		}
		if leaders == 1 && allHave {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("cluster did not converge on a leader within %s", timeout)
}

// generateClusterTLS produces a self-signed ECDSA P-256 cert valid for
// 127.0.0.1 and returns a *tls.Config configured for mutual auth (the
// same cert serves as both presented identity and trusted CA, which
// is fine for a demo running entirely on loopback).
//
// In production the cert/key/CA would come from disk via
// loadRaftTLS-style helpers, but for a self-contained example
// generating fresh material on every run avoids any external setup.
func generateClusterTLS() *tls.Config {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "memdb-cluster-example"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		log.Fatalf("CreateCertificate: %v", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		log.Fatalf("ParseCertificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  key,
		}},
		RootCAs:    pool,
		ClientCAs:  pool,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
	}
}

func resolveDataRoot(flagVal string) (string, func(), error) {
	if flagVal != "" {
		if err := os.MkdirAll(flagVal, 0o700); err != nil {
			return "", nil, err
		}
		return flagVal, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "memdb-cluster-*")
	if err != nil {
		return "", nil, err
	}
	return dir, func() {
		// Best-effort cleanup. We deliberately don't propagate errors
		// — the program is exiting anyway.
		_ = os.RemoveAll(dir)
	}, nil
}

// keep context imported in case future shutdown refactors thread one through
var _ = context.Background
