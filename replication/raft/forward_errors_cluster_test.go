package raft_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	memraft "github.com/voicetel/memdb/replication/raft"
)

// sqliteStore is a node's database: a real in-memory SQLite one, so a write
// meets SQLite's own errors.
type sqliteStore struct{ db *sql.DB }

func newSQLiteStore(t *testing.T) *sqliteStore {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return &sqliteStore{db: db}
}

func (s *sqliteStore) ExecLocal(q string, args ...any) error {
	_, err := s.ExecLocalResult(q, args...)
	return err
}

func (s *sqliteStore) ExecLocalResult(q string, args ...any) (int64, error) {
	res, err := s.db.Exec(q, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *sqliteStore) Serialize() ([]byte, error) { return nil, nil }
func (s *sqliteStore) Restore([]byte) error       { return nil }

// TestClusterErrorsMatchOnLeaderAndFollower makes the same failing writes on the
// leader and through a follower of a three-node cluster, and gets the same
// message and the same SQLite codes either way.
func TestClusterErrorsMatchOnLeaderAndFollower(t *testing.T) {
	t.Parallel()
	tlsCfg := generateTLSConfig(t)
	var peers, fwdPeers []string
	ids := []string{"node-1", "node-2", "node-3"}
	raftAddrs, fwdAddrs := map[string]string{}, map[string]string{}
	for _, id := range ids {
		raftAddrs[id], fwdAddrs[id] = pickFreeAddr(t), pickFreeAddr(t)
		peers = append(peers, id+"="+raftAddrs[id])
		fwdPeers = append(fwdPeers, id+"="+fwdAddrs[id])
	}
	var nodes []*memraft.Node
	for _, id := range ids {
		node, err := memraft.NewNode(newSQLiteStore(t), memraft.NodeConfig{
			NodeID: id, BindAddr: raftAddrs[id], ForwardAddr: fwdAddrs[id], Peers: peers, ForwardPeers: fwdPeers,
			DataDir: t.TempDir(), TLSConfig: tlsCfg,
			HeartbeatTimeout: 1500 * time.Millisecond, ElectionTimeout: 1500 * time.Millisecond,
			CommitTimeout: 10 * time.Millisecond, ApplyTimeout: 10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Shutdown() })
		nodes = append(nodes, node)
	}
	var leader, follower *memraft.Node
	for deadline := time.Now().Add(15 * time.Second); leader == nil || follower == nil || follower.LeaderAddr() == ""; {
		if time.Now().After(deadline) {
			t.Fatal("no leader, or a follower that does not know it")
		}
		leader, follower = nil, nil
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
			} else if follower == nil {
				follower = n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := leader.ExecResult(`INSERT INTO kv VALUES ('a', '1')`); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		sql      string
		extended sqlite3.ErrNoExtended
	}{
		"primary key": {`INSERT INTO kv VALUES ('a', '2')`, sqlite3.ErrConstraintPrimaryKey},
		"not null":    {`INSERT INTO kv VALUES ('b', NULL)`, sqlite3.ErrConstraintNotNull},
	} {
		_, onLeader := leader.ExecResult(tc.sql)
		_, forwarded := follower.ExecResult(tc.sql)
		var want, got sqlite3.Error
		if !errors.As(onLeader, &want) || !errors.As(forwarded, &got) {
			t.Fatalf("%s: leader %v, follower %v; want SQLite errors on both", name, onLeader, forwarded)
		}
		if want.Code != sqlite3.ErrConstraint || want.ExtendedCode != tc.extended || got.Code != want.Code || got.ExtendedCode != want.ExtendedCode {
			t.Errorf("%s: leader %+v, follower %+v", name, want, got)
		}
		if onLeader.Error() != forwarded.Error() {
			t.Errorf("%s: leader says %q, follower %q", name, onLeader.Error(), forwarded.Error())
		}
	}
}
