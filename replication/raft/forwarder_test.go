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
	"runtime"
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

// dialForwarder opens a client connection to f, as a follower's pool does.
func dialForwarder(t *testing.T, f *forwarder) *RPCConn {
	t.Helper()
	conn, err := tls.Dial("tcp", f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	rc := NewRPCConn(conn)
	if err := rc.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return rc
}

// forward sends one request on rc and reads its answer.
func forward(rc *RPCConn, sql string) (ForwardResponse, error) {
	if err := rc.writeMsg(ForwardRequest{SQL: sql}); err != nil {
		return ForwardResponse{}, err
	}
	var resp ForwardResponse
	err := rc.readMsg(&resp)
	return resp, err
}

// settledGoroutines waits for the goroutine count to come back to base, and
// reports the count it last saw.
func settledGoroutines(base int) int {
	n := runtime.NumGoroutine()
	for deadline := time.Now().Add(5 * time.Second); n > base && time.Now().Before(deadline); n = runtime.NumGoroutine() {
		time.Sleep(10 * time.Millisecond)
	}
	return n
}

// TestForwarderCloseEndsIdleConnectionsAtOnce closes a forwarder whose pooled
// connections are idle after serving a request. It returns at once rather than
// after the 30 s idle window, and leaves no goroutine behind.
func TestForwarderCloseEndsIdleConnectionsAtOnce(t *testing.T) {
	base := runtime.NumGoroutine()
	f, err := newForwarderWith("127.0.0.1:0", forwarderTLS(t), nil, func(string, ...any) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if resp, err := forward(dialForwarder(t, f), "UPDATE t SET x = 1"); err != nil || resp.RowsAffected != 1 {
			t.Fatalf("a forward before closing = %+v, %v", resp, err)
		}
	}
	start := time.Now()
	if err := f.close(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("close took %v with only idle connections", took)
	}
	if n := settledGoroutines(base); n > base {
		t.Errorf("%d goroutines after close, %d before", n, base)
	}
}

// TestForwarderCloseLetsAnInFlightForwardAnswer starts closing a forwarder while
// it applies a forwarded write. The follower still gets the write's answer, so a
// write the leader applied is never reported to it as failed; close waits for
// it, and the connection then ends.
func TestForwarderCloseLetsAnInFlightForwardAnswer(t *testing.T) {
	base := runtime.NumGoroutine()
	applying, release := make(chan struct{}), make(chan struct{})
	f, err := newForwarderWith("127.0.0.1:0", forwarderTLS(t), nil, func(string, ...any) (int64, error) {
		close(applying)
		<-release
		return 7, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rc := dialForwarder(t, f)
	answered := make(chan error, 1)
	var resp ForwardResponse
	go func() {
		var err error
		resp, err = forward(rc, "INSERT INTO t VALUES (1)")
		answered <- err
	}()
	<-applying
	closed := make(chan error, 1)
	go func() { closed <- f.close() }()
	select {
	case err := <-closed:
		t.Fatalf("close returned while a write was being applied: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-answered; err != nil || resp.RowsAffected != 7 || resp.ErrMsg != "" {
		t.Fatalf("the in-flight forward = %+v, %v; want its answer", resp, err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if _, err := forward(rc, "INSERT INTO t VALUES (2)"); err == nil {
		t.Error("a forward after close was answered")
	}
	if n := settledGoroutines(base); n > base {
		t.Errorf("%d goroutines after close, %d before", n, base)
	}
}
