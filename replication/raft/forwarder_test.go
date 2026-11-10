package raft

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"os"
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
	// The handler has answered and must now see the forwarder closing, not arm
	// the connection for another request and wait out the idle window on it.
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close still waiting 5 s after the in-flight forward was answered")
	}
	if _, err := forward(rc, "INSERT INTO t VALUES (2)"); err == nil {
		t.Error("a forward after close was answered")
	}
	if n := settledGoroutines(base); n > base {
		t.Errorf("%d goroutines after close, %d before", n, base)
	}
}

// TestForwarderAnswersAfterIdleAndASlowApply sends a request on a connection
// that has sat idle, and whose apply then outlasts what was left of the window
// the request was read under, and all of that window. The write was applied, so
// its answer must reach the follower: the answer has a deadline of its own, and
// the one the request arrived under must not cut it off.
func TestForwarderAnswersAfterIdleAndASlowApply(t *testing.T) {
	const window = 500 * time.Millisecond
	f, err := newForwarderWithTimeout("127.0.0.1:0", forwarderTLS(t), nil, func(string, ...any) (int64, error) {
		time.Sleep(window + 100*time.Millisecond)
		return 3, nil
	}, window)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.close() })
	rc := dialForwarder(t, f)
	time.Sleep(window / 5) // idle, as a pooled connection is, well inside the window
	resp, err := forward(rc, "UPDATE t SET x = 1")
	if err != nil || resp.RowsAffected != 3 || resp.ErrMsg != "" {
		t.Fatalf("an applied write's answer = %+v, %v; want it delivered", resp, err)
	}
	// The connection is still good for the next request.
	if resp, err := forward(rc, "UPDATE t SET x = 2"); err != nil || resp.RowsAffected != 3 {
		t.Fatalf("the next forward on the connection = %+v, %v", resp, err)
	}
}

// TestForwarderArmRefusesWhenClosing checks arm on a closing forwarder: it
// reports the forwarder closed, and leaves alone the read deadline close has
// expired, so the handler's next read ends at once instead of waiting out a
// fresh idle window.
func TestForwarderArmRefusesWhenClosing(t *testing.T) {
	f := &forwarder{timeout: requestTimeout, conns: map[net.Conn]struct{}{}}
	server, client := net.Pipe()
	defer func() { _ = server.Close(); _ = client.Close() }()
	rc := NewRPCConn(server)

	if !f.arm(rc) {
		t.Fatal("arm refused on a forwarder that is not closing")
	}
	// As close does: mark it done and expire the read deadline.
	f.done.Store(true)
	if err := server.SetReadDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	if f.arm(rc) {
		t.Error("arm accepted a connection on a closing forwarder")
	}
	read := make(chan error, 1)
	go func() {
		_, err := server.Read(make([]byte, 1))
		read <- err
	}()
	select {
	case err := <-read:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("read on the closing forwarder's connection: %v; want its expired deadline", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("arm pushed the expired read deadline of a closing forwarder back out")
	}
}

// TestForwarderArmWaitsForClose checks arm takes the lock close holds while it
// marks the forwarder done and expires the deadlines. Without it a handler could
// read done as unset, lose the processor, and set its deadline after close had
// expired it.
func TestForwarderArmWaitsForClose(t *testing.T) {
	f := &forwarder{timeout: requestTimeout, conns: map[net.Conn]struct{}{}}
	server, client := net.Pipe()
	defer func() { _ = server.Close(); _ = client.Close() }()

	f.mu.Lock() // close, part way through
	armed := make(chan bool, 1)
	go func() { armed <- f.arm(NewRPCConn(server)) }()
	select {
	case <-armed:
		f.mu.Unlock()
		t.Fatal("arm did not wait for the lock close holds")
	case <-time.After(100 * time.Millisecond):
	}
	f.done.Store(true)
	f.mu.Unlock()
	if <-armed {
		t.Error("arm accepted a connection once close had marked the forwarder done")
	}
}
