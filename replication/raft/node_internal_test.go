package raft

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// nopDB is a DB that applies nothing.
type nopDB struct{}

func (nopDB) ExecLocal(string, ...any) error { return nil }
func (nopDB) Serialize() ([]byte, error)     { return []byte("snapshot"), nil }
func (nopDB) Restore([]byte) error           { return nil }

// freeAddr returns a loopback address nothing listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// TestShutdownLeavesNoPoolBehind is the regression test for a pool installed
// after Shutdown had closed the last one. The leader-observer goroutine runs
// until Shutdown deregisters it, later than Shutdown closes the pool, and the
// handover Shutdown makes produces a leader change in exactly that window; the
// pool the observer then built was never closed. Here the observer's step is
// taken by hand, after Shutdown, where it must install nothing; so must a write
// that finds no pool.
func TestShutdownLeavesNoPoolBehind(t *testing.T) {
	addr := freeAddr(t)
	n, err := NewNode(nopDB{}, NodeConfig{
		NodeID: "n1", BindAddr: addr, Peers: []string{"n1=" + addr}, DataDir: t.TempDir(),
		TLSConfig:    forwarderTLS(t),
		ForwardPeers: []string{"n1=127.0.0.1:1", "n2=127.0.0.1:2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.setLeaderPool("n2")
	before := n.pool.Load()
	if before == nil {
		t.Fatal("no pool for a leader with a forward address")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if !before.closed.Load() {
		t.Error("Shutdown left the pool open")
	}

	n.setLeaderPool("n2") // the observer, seeing one more leader change
	if pool := n.pool.Load(); pool != nil {
		t.Error("a leader change after Shutdown installed a pool that nothing will close")
	}
	if pool, err := n.leaderPool("n2"); pool != nil || !errors.Is(err, hraft.ErrRaftShutdown) {
		t.Errorf("a write after Shutdown got pool %v, error %v; want none, and raft's shutdown error", pool, err)
	}
	if pool := n.pool.Load(); pool != nil {
		t.Error("a write after Shutdown installed a pool that nothing will close")
	}
}

// TestForwardPeerChangesReachThePool checks the pool follows the forward peers
// as they change: setting the current leader's address to a new one, or
// removing it, closes the pool that dials the old one, and the next write
// builds a pool for the new address or fails for want of one. Another node's
// address changing leaves the pool alone.
func TestForwardPeerChangesReachThePool(t *testing.T) {
	n := &Node{
		cfg:          NodeConfig{TLSConfig: &tls.Config{}, ForwardPoolSize: 1},
		forwardPeers: map[string]string{},
	}
	if err := n.SetForwardPeer("", "127.0.0.1:1"); err == nil {
		t.Error("a forward peer with no node ID was accepted")
	}
	if err := n.SetForwardPeer("n2", ""); err == nil {
		t.Error("a forward peer with no address was accepted")
	}

	n.setLeaderPool("n2")
	if _, err := n.leaderPool("n2"); !errors.Is(err, ErrNotLeader) || !strings.Contains(err.Error(), "SetForwardPeer") {
		t.Errorf("forwarding to a leader with no forward address: %v; want ErrNotLeader naming SetForwardPeer", err)
	}
	if err := n.SetForwardPeer("n2", "127.0.0.1:2"); err != nil {
		t.Fatal(err)
	}
	first, err := n.leaderPool("n2")
	if err != nil || first.addr != "127.0.0.1:2" {
		t.Fatalf("the pool after SetForwardPeer: %+v, %v", first, err)
	}

	// Unrelated changes, and the same address again, keep the pool.
	_ = n.SetForwardPeer("n3", "127.0.0.1:3")
	_ = n.SetForwardPeer("n2", "127.0.0.1:2")
	n.RemoveForwardPeer("n3")
	n.RemoveForwardPeer("unknown")
	if n.pool.Load() != first || first.closed.Load() {
		t.Error("a change to another node's forward address disturbed the leader's pool")
	}

	_ = n.SetForwardPeer("n2", "127.0.0.1:22")
	if !first.closed.Load() {
		t.Error("the pool dialling the leader's old address was left open")
	}
	second, err := n.leaderPool("n2")
	if err != nil || second.addr != "127.0.0.1:22" {
		t.Fatalf("the pool after the leader's address changed: %+v, %v", second, err)
	}

	n.RemoveForwardPeer("n2")
	if !second.closed.Load() || n.pool.Load() != nil {
		t.Error("removing the leader's forward address left its pool")
	}
	if _, err := n.leaderPool("n2"); !errors.Is(err, ErrNotLeader) {
		t.Errorf("forwarding to a leader whose forward address was removed: %v", err)
	}
	if got := n.ForwardPeers(); len(got) != 0 {
		t.Errorf("forward peers left: %v", got)
	}
}

// refusedFuture is the future of an Apply that Raft refused with err.
type refusedFuture struct{ err error }

func (f refusedFuture) Error() error  { return f.err }
func (f refusedFuture) Response() any { return nil }
func (f refusedFuture) Index() uint64 { return 0 }

// TestRefusedWritesAreErrNotLeader checks a write the leader refuses because it
// is handing leadership over, or because it is no longer the leader, comes back
// as ErrNotLeader on the leader itself and through a forward, with raft's own
// error and words kept. A write that lost leadership after it was taken is not
// one: it may commit, and must not be retried as though it had not.
func TestRefusedWritesAreErrNotLeader(t *testing.T) {
	for _, refusal := range []error{hraft.ErrLeadershipTransferInProgress, hraft.ErrNotLeader} {
		// On the leader.
		_, local := applied(refusedFuture{refusal})
		if !errors.Is(local, ErrNotLeader) || !errors.Is(local, refusal) {
			t.Errorf("a write refused with %q: %v; want ErrNotLeader and raft's error both", refusal, local)
		}
		if want := "raft: apply: " + refusal.Error(); local == nil || local.Error() != want {
			t.Errorf("a write refused with %q reads %q, want %q", refusal, local, want)
		}

		// Through a forward: the leader's forwarder answers with that error.
		f, err := newForwarderWith("127.0.0.1:0", forwarderTLS(t), nil, func(string, ...any) (int64, error) {
			return 0, local
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.close() })
		resp, err := forward(dialForwarder(t, f), "INSERT INTO t VALUES (1)")
		if err != nil || resp.ErrCode != "ErrNotLeader" || resp.ErrMsg != local.Error() {
			t.Errorf("the leader's answer to a forward refused with %q = %+v, %v", refusal, resp, err)
		}
		pool := NewConnPool(f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}, 1)
		t.Cleanup(pool.Close)
		_, forwarded := sendForward(context.Background(), pool, ForwardRequest{SQL: "INSERT INTO t VALUES (1)"}, 5*time.Second)
		if !errors.Is(forwarded, ErrNotLeader) || !strings.Contains(forwarded.Error(), refusal.Error()) {
			t.Errorf("a forward refused with %q: %v; want ErrNotLeader with raft's words", refusal, forwarded)
		}
	}

	if _, err := applied(refusedFuture{hraft.ErrLeadershipLost}); errors.Is(err, ErrNotLeader) {
		t.Errorf("a write that lost leadership after it was taken reads as never made: %v", err)
	}
}

// TestForwardRefusedByAnOlderLeaderIsErrNotLeader forwards to a leader that
// predates marking a write refused during a handover: it sends the refusal as
// plain text with no code. The follower still reports ErrNotLeader, so a
// cluster part way through an upgrade behaves as one that has finished. Any
// other uncoded error stays what it was.
func TestForwardRefusedByAnOlderLeaderIsErrNotLeader(t *testing.T) {
	answer := errors.New("raft: apply: " + hraft.ErrLeadershipTransferInProgress.Error())
	f, err := newForwarderWith("127.0.0.1:0", forwarderTLS(t), nil, func(sql string, _ ...any) (int64, error) {
		if sql == "refused" {
			return 0, answer // not ErrNotLeader, so no code is sent, as an older leader sends none
		}
		return 0, errors.New("no such table: t, though a leadership transfer in progress is mentioned")
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.close() })
	pool := NewConnPool(f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}, 1)
	t.Cleanup(pool.Close)

	_, err = sendForward(context.Background(), pool, ForwardRequest{SQL: "refused"}, 5*time.Second)
	if !errors.Is(err, ErrNotLeader) || !strings.Contains(err.Error(), answer.Error()) {
		t.Errorf("an older leader's refusal: %v; want ErrNotLeader with its words", err)
	}
	_, err = sendForward(context.Background(), pool, ForwardRequest{SQL: "other"}, 5*time.Second)
	if err == nil || errors.Is(err, ErrNotLeader) {
		t.Errorf("an unrelated error from the leader: %v; want it left as it was", err)
	}
}
