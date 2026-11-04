package raft

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

func forwarderTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "forwarder-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func TestForwarderAcceptCloseRace(t *testing.T) {
	for i := range 10 {
		f, err := newForwarder("127.0.0.1:0", forwarderTLS(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- f.close() }()
		conn, err := tls.Dial("tcp", f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			_ = conn.Close()
		}
		if err := <-done; err != nil {
			t.Fatalf("iteration %d: close: %v", i, err)
		}
	}
}

func TestForwarderCloseUnblocksHandler(t *testing.T) {
	f, err := newForwarder("127.0.0.1:0", forwarderTLS(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	done := make(chan error, 1)
	go func() { done <- f.close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not unblock the handler")
	}
}

func TestForwarderStartCloseRace(t *testing.T) {
	for i := range 10 {
		f, err := newForwarder("127.0.0.1:0", forwarderTLS(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- f.close() }()
		if err := <-done; err != nil {
			t.Fatalf("iteration %d: close: %v", i, err)
		}
	}
}
