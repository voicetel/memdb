package replication

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
)

// FollowerConfig holds options for a follower node.
type FollowerConfig struct {
	Transport Transport

	// OnGapDetected is called when a sequence gap is detected, before resync.
	OnGapDetected func(expected, got uint64)

	// OnApplyError is called when a WAL entry cannot be applied.
	// If nil, the error is silently dropped (not recommended in production).
	OnApplyError func(err error)
}

// Follower receives and applies WAL entries from a leader.
type Follower struct {
	db      *sql.DB
	cfg     FollowerConfig
	lastSeq atomic.Uint64
}

// NewFollower creates a new Follower backed by db.
func NewFollower(db *sql.DB, cfg FollowerConfig) *Follower {
	return &Follower{db: db, cfg: cfg}
}

// Start begins receiving WAL entries from the leader.
// Returns immediately; processing runs in a background goroutine that stops
// when ctx is cancelled or the subscription channel is closed.
func (f *Follower) Start(ctx context.Context) error {
	ch, err := f.cfg.Transport.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("follower: subscribe: %w", err)
	}

	go func() {
		for {
			select {
			case entry, ok := <-ch:
				if !ok {
					return
				}
				if err := f.applyEntry(ctx, entry); err != nil {
					if f.cfg.OnApplyError != nil {
						f.cfg.OnApplyError(err)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (f *Follower) applyEntry(ctx context.Context, entry WALEntry) error {
	expected := f.lastSeq.Load() + 1

	if entry.Seq != expected {
		// Allow the very first entry to start at seq 1 when we have no history.
		if f.lastSeq.Load() == 0 && entry.Seq == 1 {
			// Correct start — fall through to apply.
		} else {
			if f.cfg.OnGapDetected != nil {
				f.cfg.OnGapDetected(expected, entry.Seq)
			}
			if err := f.resync(ctx); err != nil {
				return err
			}
			// The entry that triggered the gap may already be covered by the
			// snapshot (entry.Seq <= snapshotSeq). If so, skip it; otherwise
			// fall through and apply it now so it is not lost.
			if entry.Seq <= f.lastSeq.Load() {
				return nil
			}
		}
	}

	if _, err := f.db.Exec(entry.SQL, entry.Args...); err != nil {
		return fmt.Errorf("follower: apply seq %d: %w", entry.Seq, err)
	}
	f.lastSeq.Store(entry.Seq)
	return nil
}

func (f *Follower) resync(ctx context.Context) error {
	snapshotSeq, err := f.cfg.Transport.RequestSync(ctx, f.lastSeq.Load())
	if err != nil {
		return fmt.Errorf("follower: resync: %w", err)
	}
	f.lastSeq.Store(snapshotSeq)
	return nil
}

// LastSeq returns the highest WAL sequence number applied by this follower.
func (f *Follower) LastSeq() uint64 {
	return f.lastSeq.Load()
}
