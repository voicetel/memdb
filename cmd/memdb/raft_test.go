package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/voicetel/memdb"
)

func TestSplitCSV(t *testing.T) {
	// Empty / whitespace-only inputs collapse to an empty (or nil) result;
	// we accept either since the function uses parts[:0] which can yield a
	// non-nil empty slice. Callers iterate, so length-zero is what matters.
	cases := []struct {
		in   string
		want []string
	}{
		{"a", []string{"a"}},
		{"a,b,c", []string{"a", "b", "c"}},
		{"a, b, c", []string{"a", "b", "c"}},
		{"a,,b", []string{"a", "b"}},
		{"a,b,", []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := splitCSV(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitCSV(%q)=%v, want %v", tc.in, got, tc.want)
			}
		})
	}

	for _, in := range []string{"", "   ", ",,,"} {
		t.Run("empty/"+in, func(t *testing.T) {
			if got := splitCSV(in); len(got) != 0 {
				t.Errorf("splitCSV(%q)=%v, want empty", in, got)
			}
		})
	}
}

func TestRaftFlags_Enabled(t *testing.T) {
	if (raftFlags{}).enabled() {
		t.Error("empty raftFlags should not be enabled")
	}
	if !(raftFlags{NodeID: "n1"}).enabled() {
		t.Error("NodeID set should enable raft")
	}
}

func TestRaftFlags_Validate_Disabled(t *testing.T) {
	// When NodeID is empty, validate() short-circuits and ignores all
	// other fields — even garbage ones.
	r := raftFlags{BindAddr: "ignored"}
	if err := r.validate(); err != nil {
		t.Errorf("disabled raftFlags should validate: %v", err)
	}
}

func TestRaftFlags_Validate_AllRequired(t *testing.T) {
	// NodeID set but everything else empty — every required flag should
	// be reported as missing.
	r := raftFlags{NodeID: "n1"}
	err := r.validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	for _, want := range []string{
		"-raft-bind", "-raft-peers", "-raft-data-dir",
		"-raft-tls-cert", "-raft-tls-key", "-raft-tls-ca",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q: %v", want, err)
		}
	}
}

func TestRaftFlags_Validate_HappyPath(t *testing.T) {
	r := raftFlags{
		NodeID:      "n1",
		BindAddr:    "0.0.0.0:7000",
		Peers:       "n1=127.0.0.1:7000",
		DataDir:     "/tmp/raft",
		TLSCertFile: "cert.pem",
		TLSKeyFile:  "key.pem",
		TLSCAFile:   "ca.pem",
	}
	if err := r.validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRaftFlags_Validate_ForwardPairing(t *testing.T) {
	base := raftFlags{
		NodeID:      "n1",
		BindAddr:    "0.0.0.0:7000",
		Peers:       "n1=127.0.0.1:7000",
		DataDir:     "/tmp/raft",
		TLSCertFile: "cert.pem",
		TLSKeyFile:  "key.pem",
		TLSCAFile:   "ca.pem",
	}

	t.Run("bind-without-peers", func(t *testing.T) {
		r := base
		r.ForwardBindAddr = "0.0.0.0:7001"
		err := r.validate()
		if err == nil || !strings.Contains(err.Error(), "forward-peers") {
			t.Errorf("expected forward-peers error, got: %v", err)
		}
	})

	t.Run("peers-without-bind", func(t *testing.T) {
		r := base
		r.ForwardPeers = "n1=127.0.0.1:7001"
		err := r.validate()
		if err == nil || !strings.Contains(err.Error(), "forward-bind") {
			t.Errorf("expected forward-bind error, got: %v", err)
		}
	})

	t.Run("both-set-ok", func(t *testing.T) {
		r := base
		r.ForwardBindAddr = "0.0.0.0:7001"
		r.ForwardPeers = "n1=127.0.0.1:7001"
		if err := r.validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestRegisterRaftFlags(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	r := registerRaftFlags(fs)
	args := []string{
		"-raft-node-id", "n1",
		"-raft-bind", "0.0.0.0:7000",
		"-raft-advertise", "10.0.0.1:7000",
		"-raft-forward-bind", "0.0.0.0:7001",
		"-raft-peers", "n1=10.0.0.1:7000,n2=10.0.0.2:7000",
		"-raft-forward-peers", "n1=10.0.0.1:7001,n2=10.0.0.2:7001",
		"-raft-data-dir", "/tmp/raft",
		"-raft-tls-cert", "/etc/cert.pem",
		"-raft-tls-key", "/etc/key.pem",
		"-raft-tls-ca", "/etc/ca.pem",
		"-raft-snapshot-interval", "30s",
		"-raft-snapshot-threshold", "1024",
		"-raft-heartbeat-timeout", "1s",
		"-raft-election-timeout", "1s",
		"-raft-apply-timeout", "5s",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if r.NodeID != "n1" {
		t.Errorf("NodeID=%q", r.NodeID)
	}
	if r.BindAddr != "0.0.0.0:7000" {
		t.Errorf("BindAddr=%q", r.BindAddr)
	}
	if r.AdvertiseAddr != "10.0.0.1:7000" {
		t.Errorf("AdvertiseAddr=%q", r.AdvertiseAddr)
	}
	if r.SnapshotInterval != 30*time.Second {
		t.Errorf("SnapshotInterval=%v", r.SnapshotInterval)
	}
	if r.SnapshotThreshold != 1024 {
		t.Errorf("SnapshotThreshold=%d", r.SnapshotThreshold)
	}
	if r.HeartbeatTimeout != time.Second {
		t.Errorf("HeartbeatTimeout=%v", r.HeartbeatTimeout)
	}
	if r.TLSCAFile != "/etc/ca.pem" {
		t.Errorf("TLSCAFile=%q", r.TLSCAFile)
	}
	if !r.enabled() {
		t.Error("flags with NodeID set should be enabled")
	}
}

func TestLoadRaftTLS_HappyPath(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := writeSelfSignedTLS(t, dir)
	cfg, err := loadRaftTLS(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("loadRaftTLS: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("Certificates len=%d, want 1", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs nil")
	}
	if cfg.ClientCAs == nil {
		t.Error("ClientCAs nil")
	}
	if cfg.ClientAuth.String() != "RequireAndVerifyClientCert" {
		t.Errorf("ClientAuth=%v", cfg.ClientAuth)
	}
	if cfg.MinVersion < 0x0304 { // TLS 1.3
		t.Errorf("MinVersion=%x, want >= TLS 1.3", cfg.MinVersion)
	}
}

func TestLoadRaftTLS_MissingFiles(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := writeSelfSignedTLS(t, dir)

	cases := []struct {
		name             string
		cert, key, ca    string
		wantErrSubstring string
	}{
		{"missing-cert", filepath.Join(dir, "nope.pem"), keyFile, caFile, "tls keypair"},
		{"missing-key", certFile, filepath.Join(dir, "nope.pem"), caFile, "tls keypair"},
		{"missing-ca", certFile, keyFile, filepath.Join(dir, "nope.pem"), "ca bundle"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadRaftTLS(tc.cert, tc.key, tc.ca)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstring) {
				t.Errorf("error %q missing %q", err, tc.wantErrSubstring)
			}
		})
	}
}

func TestLoadRaftTLS_EmptyCABundle(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, _ := writeSelfSignedTLS(t, dir)
	emptyCA := filepath.Join(dir, "empty-ca.pem")
	if err := os.WriteFile(emptyCA, []byte("not a pem certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadRaftTLS(certFile, keyFile, emptyCA)
	if err == nil {
		t.Fatal("expected error for non-PEM CA, got nil")
	}
	if !strings.Contains(err.Error(), "no usable certificates") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestBuildRaftNode_HappyPath(t *testing.T) {
	// Construct a single-node Raft cluster on a free port using the
	// self-signed cert helper from this file. We don't bootstrap an
	// election or send any logs — the test verifies that buildRaftNode
	// can wire up TLS, the FSM, the data dir, and the forwarding
	// listener without erroring. Shutdown is the load-bearing assertion
	// that nothing leaked.
	dir := t.TempDir()
	certFile, keyFile, caFile := writeSelfSignedTLS(t, dir)
	dataDir := filepath.Join(dir, "raft-data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}

	bindPort, err := freeTCPPort()
	if err != nil {
		t.Fatal(err)
	}
	bind := fmt.Sprintf("127.0.0.1:%d", bindPort)

	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(dir, "raft-db.db"),
		FlushInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cfg := raftFlags{
		NodeID:      "n1",
		BindAddr:    bind,
		Peers:       fmt.Sprintf("n1=%s", bind),
		DataDir:     dataDir,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
		TLSCAFile:   caFile,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	node, err := buildRaftNode(db, cfg, logger)
	if err != nil {
		t.Fatalf("buildRaftNode: %v", err)
	}
	t.Cleanup(func() {
		if err := node.Shutdown(); err != nil {
			t.Logf("node shutdown: %v", err)
		}
	})
	if node == nil {
		t.Fatal("buildRaftNode returned nil node")
	}
}

func TestBuildRaftNode_BadTLS(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "raft-data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}

	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(dir, "raft-db.db"),
		FlushInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cfg := raftFlags{
		NodeID:      "n1",
		BindAddr:    "127.0.0.1:0",
		Peers:       "n1=127.0.0.1:9999",
		DataDir:     dataDir,
		TLSCertFile: filepath.Join(dir, "missing.pem"),
		TLSKeyFile:  filepath.Join(dir, "missing.pem"),
		TLSCAFile:   filepath.Join(dir, "missing.pem"),
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := buildRaftNode(db, cfg, logger); err == nil {
		t.Fatal("expected TLS error, got nil")
	}
}

// freeTCPPort asks the kernel for a free TCP port.
func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func TestRaftDBAdapter_Passthroughs(t *testing.T) {
	// Pass through ExecLocal / Serialize / Restore against a real in-memory
	// memdb. The adapter is a thin wrapper, but the contract must be that
	// ExecLocal calls ExecDirect (not Exec — which would route through
	// OnExec and potentially recurse through Raft).
	dir := t.TempDir()
	db, err := memdb.Open(memdb.Config{
		FilePath:      filepath.Join(dir, "x.db"),
		FlushInterval: -1, // disable background flush
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	adapter := raftDBAdapter{db: db}

	if err := adapter.ExecLocal("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("ExecLocal CREATE: %v", err)
	}
	if err := adapter.ExecLocal("INSERT INTO t (id, v) VALUES (?, ?)", 1, "hello"); err != nil {
		t.Fatalf("ExecLocal INSERT: %v", err)
	}

	snap, err := adapter.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("Serialize returned empty payload")
	}

	// Round-trip: restore the snapshot back into the same DB. The data
	// should remain queryable.
	if err := adapter.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	row := db.QueryRow("SELECT v FROM t WHERE id = 1")
	var got string
	if err := row.Scan(&got); err != nil {
		t.Fatalf("post-restore Scan: %v", err)
	}
	if got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

// writeSelfSignedTLS creates a minimal self-signed ed25519 keypair and
// reuses the same cert as both leaf and CA — sufficient for loadRaftTLS
// which only parses the PEM, never validates a chain.
func writeSelfSignedTLS(t *testing.T, dir string) (certFile, keyFile, caFile string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "memdb-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	// Use the same cert as the CA bundle — loadRaftTLS only verifies that
	// the bundle parses to at least one cert.
	caFile = certFile
	return certFile, keyFile, caFile
}
