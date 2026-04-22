//go:build !purego

package raft_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	memraft "github.com/voicetel/memdb/replication/raft"
)

// ---------------------------------------------------------------------------
// TLS helper
// ---------------------------------------------------------------------------

func generateTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "memdb-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	tlsCert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
	return &tls.Config{
		Certificates:       []tls.Certificate{tlsCert},
		RootCAs:            pool,
		ClientCAs:          pool,
		ClientAuth:         tls.RequireAndVerifyClientCert,
		InsecureSkipVerify: false,
	}
}

// ---------------------------------------------------------------------------
// Mock DB
// ---------------------------------------------------------------------------

type mockDB struct {
	mu   sync.Mutex
	log  []string // records every ExecLocal call as "sql:arg1,arg2,..."
	snap []byte
}

func (m *mockDB) ExecLocal(sql string, args ...any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := sql
	for _, a := range args {
		entry += fmt.Sprintf(":%v", a)
	}
	m.log = append(m.log, entry)
	return nil
}

func (m *mockDB) Serialize() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap != nil {
		return m.snap, nil
	}
	return []byte("empty-snapshot"), nil
}

func (m *mockDB) Restore(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = data
	return nil
}

func (m *mockDB) execCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.log)
}

func (m *mockDB) logEntry(i int) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i < 0 || i >= len(m.log) {
		return ""
	}
	return m.log[i]
}

// ---------------------------------------------------------------------------
// newTestNode helper
// ---------------------------------------------------------------------------

// pickFreeAddr finds a free TCP port on 127.0.0.1 and returns the address.
// The listener is closed before returning so the port can be reused.
func pickFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func newTestNode(t *testing.T, nodeID string, tlsCfg *tls.Config, peers []string) (*memraft.Node, *mockDB) {
	t.Helper()
	db := &mockDB{}

	addr := pickFreeAddr(t)

	peerList := peers
	if peerList == nil {
		peerList = []string{nodeID + "=" + addr}
	}

	cfg := memraft.NodeConfig{
		NodeID:           nodeID,
		BindAddr:         addr,
		Peers:            peerList,
		DataDir:          t.TempDir(),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 500 * time.Millisecond,
		ElectionTimeout:  500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     10 * time.Second,
	}

	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node, db
}

// ---------------------------------------------------------------------------
// waitForLeader helper
// ---------------------------------------------------------------------------

func waitForLeader(t *testing.T, node *memraft.Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node did not become leader within %s", timeout)
}

// ---------------------------------------------------------------------------
// TestNode_ValidationErrors
// ---------------------------------------------------------------------------

func TestNode_ValidationErrors(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	validAddr := pickFreeAddr(t)

	baseConfig := func() memraft.NodeConfig {
		return memraft.NodeConfig{
			NodeID:    "node-1",
			BindAddr:  validAddr,
			Peers:     []string{"node-1=" + validAddr},
			DataDir:   t.TempDir(),
			TLSConfig: tlsCfg,
		}
	}

	db := &mockDB{}

	t.Run("empty NodeID", func(t *testing.T) {
		t.Parallel()
		cfg := baseConfig()
		cfg.NodeID = ""
		_, err := memraft.NewNode(db, cfg)
		if err == nil {
			t.Fatal("expected error for empty NodeID, got nil")
		}
		if !strings.Contains(err.Error(), "NodeID") {
			t.Errorf("expected error to contain 'NodeID', got: %v", err)
		}
	})

	t.Run("empty BindAddr", func(t *testing.T) {
		t.Parallel()
		cfg := baseConfig()
		cfg.BindAddr = ""
		_, err := memraft.NewNode(db, cfg)
		if err == nil {
			t.Fatal("expected error for empty BindAddr, got nil")
		}
		if !strings.Contains(err.Error(), "BindAddr") {
			t.Errorf("expected error to contain 'BindAddr', got: %v", err)
		}
	})

	t.Run("nil TLSConfig", func(t *testing.T) {
		t.Parallel()
		cfg := baseConfig()
		cfg.TLSConfig = nil
		_, err := memraft.NewNode(db, cfg)
		if err == nil {
			t.Fatal("expected error for nil TLSConfig, got nil")
		}
		if !strings.Contains(err.Error(), "TLSConfig") {
			t.Errorf("expected error to contain 'TLSConfig', got: %v", err)
		}
	})

	t.Run("empty DataDir", func(t *testing.T) {
		t.Parallel()
		cfg := baseConfig()
		cfg.DataDir = ""
		_, err := memraft.NewNode(db, cfg)
		if err == nil {
			t.Fatal("expected error for empty DataDir, got nil")
		}
		if !strings.Contains(err.Error(), "DataDir") {
			t.Errorf("expected error to contain 'DataDir', got: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TestNode_SingleNode_BecomesLeader
// ---------------------------------------------------------------------------

func TestNode_SingleNode_BecomesLeader(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	node, _ := newTestNode(t, "node-1", tlsCfg, nil)

	waitForLeader(t, node, 10*time.Second)

	if !node.IsLeader() {
		t.Error("expected node to be leader after waitForLeader")
	}
}

// ---------------------------------------------------------------------------
// TestNode_Exec_NotLeader
// ---------------------------------------------------------------------------

func TestNode_Exec_NotLeader(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	db := &mockDB{}

	// Use a peer address that will never connect — this node can never win
	// an election since it can't form a quorum with the phantom peer.
	phantomAddr := pickFreeAddr(t)
	realAddr := pickFreeAddr(t)

	cfg := memraft.NodeConfig{
		NodeID:           "node-a",
		BindAddr:         realAddr,
		Peers:            []string{"node-a=" + realAddr, "node-b=" + phantomAddr},
		DataDir:          t.TempDir(),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 500 * time.Millisecond,
		ElectionTimeout:  500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     500 * time.Millisecond,
	}

	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })

	// The node will never become leader without quorum. Exec should return
	// an error wrapping ErrNotLeader.
	err = node.Exec("SELECT 1")
	if err == nil {
		t.Fatal("expected error from Exec on non-leader, got nil")
	}
	if !errors.Is(err, memraft.ErrNotLeader) {
		t.Errorf("expected error to wrap ErrNotLeader, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestNode_Exec_AppliedToFSM
// ---------------------------------------------------------------------------

func TestNode_Exec_AppliedToFSM(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	node, db := newTestNode(t, "node-1", tlsCfg, nil)

	waitForLeader(t, node, 10*time.Second)

	if err := node.Exec("INSERT INTO kv VALUES (1)", "key", 42); err != nil {
		t.Fatalf("Exec() returned unexpected error: %v", err)
	}

	// Give the FSM a moment to apply the entry.
	deadline := time.Now().Add(2 * time.Second)
	for db.execCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if db.execCount() != 1 {
		t.Fatalf("expected 1 FSM apply, got %d", db.execCount())
	}

	entry := db.logEntry(0)
	if !strings.Contains(entry, "INSERT") {
		t.Errorf("expected log entry to contain 'INSERT', got: %q", entry)
	}
}

// ---------------------------------------------------------------------------
// TestNode_Exec_MultipleWrites
// ---------------------------------------------------------------------------

func TestNode_Exec_MultipleWrites(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	node, db := newTestNode(t, "node-1", tlsCfg, nil)

	waitForLeader(t, node, 10*time.Second)

	const n = 5
	for i := 0; i < n; i++ {
		if err := node.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i)); err != nil {
			t.Fatalf("Exec(%d) returned unexpected error: %v", i, err)
		}
	}

	// Wait for all entries to be applied.
	deadline := time.Now().Add(10 * time.Second)
	for db.execCount() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if db.execCount() != n {
		t.Fatalf("expected %d FSM applies, got %d", n, db.execCount())
	}

	// Verify all entries are present and in order.
	for i := 0; i < n; i++ {
		entry := db.logEntry(i)
		want := fmt.Sprintf("INSERT INTO t VALUES (%d)", i)
		if entry != want {
			t.Errorf("log[%d]: got %q, want %q", i, entry, want)
		}
	}
}

// ---------------------------------------------------------------------------
// TestNode_IsLeader_Stats
// ---------------------------------------------------------------------------

func TestNode_IsLeader_Stats(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	node, _ := newTestNode(t, "node-1", tlsCfg, nil)

	waitForLeader(t, node, 10*time.Second)

	stats := node.Stats()
	if stats == nil {
		t.Fatal("expected non-nil stats map")
	}
	if len(stats) == 0 {
		t.Error("expected non-empty stats map")
	}

	leaderAddr := node.LeaderAddr()
	if leaderAddr == "" {
		t.Error("expected non-empty LeaderAddr after becoming leader")
	}
}

// ---------------------------------------------------------------------------
// TestNode_OnLeaderChange_Called
// ---------------------------------------------------------------------------

func TestNode_OnLeaderChange_Called(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	db := &mockDB{}

	addr := pickFreeAddr(t)
	var leaderCallbackFired atomic.Bool

	cfg := memraft.NodeConfig{
		NodeID:           "node-1",
		BindAddr:         addr,
		Peers:            []string{"node-1=" + addr},
		DataDir:          t.TempDir(),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 500 * time.Millisecond,
		ElectionTimeout:  500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     10 * time.Second,
		OnLeaderChange: func(isLeader bool) {
			if isLeader {
				leaderCallbackFired.Store(true)
			}
		},
	}

	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })

	waitForLeader(t, node, 10*time.Second)

	// Give the observer goroutine a moment to fire the callback.
	deadline := time.Now().Add(2 * time.Second)
	for !leaderCallbackFired.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if !leaderCallbackFired.Load() {
		t.Error("OnLeaderChange callback was not called with isLeader=true")
	}
}

// ---------------------------------------------------------------------------
// TestNode_AddRemoveVoter_NotLeader
// ---------------------------------------------------------------------------

func TestNode_AddRemoveVoter_NotLeader(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	db := &mockDB{}

	phantomAddr := pickFreeAddr(t)
	realAddr := pickFreeAddr(t)

	cfg := memraft.NodeConfig{
		NodeID:           "node-a",
		BindAddr:         realAddr,
		Peers:            []string{"node-a=" + realAddr, "node-b=" + phantomAddr},
		DataDir:          t.TempDir(),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 500 * time.Millisecond,
		ElectionTimeout:  500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     500 * time.Millisecond,
	}

	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })

	addErr := node.AddVoter("node-c", phantomAddr, 200*time.Millisecond)
	if addErr == nil {
		t.Error("expected error from AddVoter on non-leader, got nil")
	}

	removeErr := node.RemoveServer("node-b", 200*time.Millisecond)
	if removeErr == nil {
		t.Error("expected error from RemoveServer on non-leader, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestParsePeers_Valid
// ---------------------------------------------------------------------------

func TestParsePeers_Valid(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	db := &mockDB{}

	// Exercise parsePeers indirectly: build a node with two peers listed,
	// then shut it down immediately. We just verify NewNode does not error
	// on valid peer strings (the real parsePeers path).
	//
	// We use a direct call through NewNode since parsePeers is unexported.
	// Instead we test the logic via a dedicated unit test using NewNode's
	// error surface for invalid peers and rely on valid-peer acceptance.

	addr1 := pickFreeAddr(t)
	addr2 := pickFreeAddr(t)

	// Two-node config: bootstrapping with two peers. The second peer won't
	// connect, but parsePeers itself must succeed (no parse error).
	cfg := memraft.NodeConfig{
		NodeID:           "node-1",
		BindAddr:         addr1,
		Peers:            []string{"node-1=" + addr1, "node-2=" + addr2},
		DataDir:          t.TempDir(),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 500 * time.Millisecond,
		ElectionTimeout:  500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     500 * time.Millisecond,
	}

	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatalf("NewNode with valid peer strings returned error: %v", err)
	}
	_ = node.Shutdown()
}

// ---------------------------------------------------------------------------
// TestParsePeers_Invalid
// ---------------------------------------------------------------------------

func TestParsePeers_Invalid(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	db := &mockDB{}

	addr := pickFreeAddr(t)

	cases := []struct {
		name  string
		peers []string
	}{
		{
			name:  "no equals sign",
			peers: []string{"no-equals-sign"},
		},
		{
			name:  "empty nodeID",
			peers: []string{"=" + addr},
		},
		{
			name:  "empty addr",
			peers: []string{"nodeID="},
		},
		{
			name:  "no peers",
			peers: []string{},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := memraft.NodeConfig{
				NodeID:    "node-1",
				BindAddr:  addr,
				Peers:     tc.peers,
				DataDir:   t.TempDir(),
				TLSConfig: tlsCfg,
			}
			_, err := memraft.NewNode(db, cfg)
			if err == nil {
				t.Fatalf("expected error for invalid peers %v, got nil", tc.peers)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestNode_Shutdown_Idempotent
// ---------------------------------------------------------------------------

func TestNode_Shutdown_Idempotent(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	db := &mockDB{}

	addr := pickFreeAddr(t)

	cfg := memraft.NodeConfig{
		NodeID:           "node-1",
		BindAddr:         addr,
		Peers:            []string{"node-1=" + addr},
		DataDir:          t.TempDir(),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 500 * time.Millisecond,
		ElectionTimeout:  500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     10 * time.Second,
	}

	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// First shutdown — must not panic.
	if err := node.Shutdown(); err != nil {
		t.Logf("first Shutdown() returned (non-fatal): %v", err)
	}

	// Second shutdown — must not panic (hashicorp/raft returns nil on repeated Shutdown).
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("second Shutdown() panicked: %v", r)
			}
		}()
		_ = node.Shutdown()
	}()
}

// ---------------------------------------------------------------------------
// TestNode_Exec_AfterShutdown
// ---------------------------------------------------------------------------

func TestNode_Exec_AfterShutdown(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	node, _ := newTestNode(t, "node-1", tlsCfg, nil)

	waitForLeader(t, node, 10*time.Second)

	if err := node.Shutdown(); err != nil {
		t.Logf("Shutdown() returned: %v", err)
	}

	// After shutdown, Exec should return an error (not panic).
	err := node.Exec("SELECT 1")
	if err == nil {
		t.Error("expected error from Exec after shutdown, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestNode_Stats_NonNil
// ---------------------------------------------------------------------------

func TestNode_Stats_NonNil(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	node, _ := newTestNode(t, "node-1", tlsCfg, nil)

	// Stats should be non-nil even before becoming leader.
	stats := node.Stats()
	if stats == nil {
		t.Fatal("expected non-nil stats map before becoming leader")
	}
}

// ---------------------------------------------------------------------------
// TestNode_LeaderAddr_BecomesNonEmpty
// ---------------------------------------------------------------------------

func TestNode_LeaderAddr_BecomesNonEmpty(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	node, _ := newTestNode(t, "node-1", tlsCfg, nil)

	waitForLeader(t, node, 10*time.Second)

	addr := node.LeaderAddr()
	if addr == "" {
		t.Error("expected non-empty LeaderAddr after node becomes leader")
	}
}
