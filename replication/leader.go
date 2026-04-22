package replication

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SyncMode controls whether writes block until followers acknowledge.
type SyncMode int

const (
	SyncModeAsync SyncMode = iota // fire-and-forget to followers
	SyncModeSync                  // block until quorum acknowledges
)

// LeaderConfig holds options for the leader node.
type LeaderConfig struct {
	Transport Transport
	SyncMode  SyncMode
	// Quorum is the number of followers that must ack before a sync write returns.
	// Ignored in SyncModeAsync. Default: 1.
	Quorum int
}

type followerState struct {
	id      string
	lastSeq uint64
	lastAck time.Time
}

// Leader manages WAL shipping to connected followers.
type Leader struct {
	cfg       LeaderConfig
	followers map[string]*followerState
	mu        sync.RWMutex
}

// NewLeader creates a new Leader.
func NewLeader(cfg LeaderConfig) *Leader {
	if cfg.Quorum == 0 {
		cfg.Quorum = 1
	}
	return &Leader{
		cfg:       cfg,
		followers: make(map[string]*followerState),
	}
}

// Ship sends a WALEntry to all followers.
// In SyncModeSync it blocks until Quorum followers have acknowledged the entry.
func (l *Leader) Ship(ctx context.Context, entry WALEntry) error {
	if l.cfg.SyncMode == SyncModeAsync {
		go func() {
			_ = l.cfg.Transport.Broadcast(context.Background(), entry)
		}()
		return nil
	}

	// Broadcast first, then wait for quorum acks.
	if err := l.cfg.Transport.Broadcast(ctx, entry); err != nil {
		return err
	}
	return l.waitForQuorum(ctx, entry.Seq)
}

// waitForQuorum blocks until at least l.cfg.Quorum followers report lastSeq >= seq,
// or the context is cancelled.
func (l *Leader) waitForQuorum(ctx context.Context, seq uint64) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if l.ackedCount(seq) >= l.cfg.Quorum {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("leader: quorum wait cancelled: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// ackedCount returns how many followers have acked up to seq.
func (l *Leader) ackedCount(seq uint64) int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	count := 0
	for _, f := range l.followers {
		if f.lastSeq >= seq {
			count++
		}
	}
	return count
}

// FollowerLag returns the replication lag per follower ID.
func (l *Leader) FollowerLag() map[string]time.Duration {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[string]time.Duration, len(l.followers))
	for id, f := range l.followers {
		out[id] = time.Since(f.lastAck)
	}
	return out
}

// Acknowledge records that a follower has applied up to seq.
func (l *Leader) Acknowledge(followerID string, seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.followers[followerID]
	if !ok {
		f = &followerState{id: followerID}
		l.followers[followerID] = f
	}
	if seq > f.lastSeq {
		f.lastSeq = seq
		f.lastAck = time.Now()
	}
}

// ErrNotLeader is returned when a non-leader node receives a write.
type ErrNotLeader struct {
	LeaderAddr string
}

func (e ErrNotLeader) Error() string {
	return fmt.Sprintf("memdb: not the leader — current leader: %s", e.LeaderAddr)
}
