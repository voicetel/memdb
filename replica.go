//go:build !purego

package memdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// replicaPool holds N independent in-memory SQLite databases that mirror the
// writer via sqlite3_serialize / sqlite3_deserialize.
//
// # Design: channel-based pool with WaitGroup drain
//
// The idle channel is the pool. Each *sql.DB sits in the channel when no
// goroutine is using it. Callers check out a replica via checkout(), which
// removes it from the channel and increments inUse. The returned release
// function returns the replica to the channel and decrements inUse.
//
// refresh() drains the idle channel to collect all currently-idle replicas,
// then calls inUse.Wait() to block until every checked-out replica has been
// released. At that point no goroutine holds a reference to any replica, so
// sqlite3_deserialize is safe to call — no open cursor can exist.
//
// For QueryRow the checkout window is tiny (checkout → QueryRow → release
// before returning to the caller). For Query the replica is released as soon
// as Query() returns; the underlying *sql.DB connection is held by the
// *sql.Rows cursor until the caller calls rows.Close(). Since each replica
// is pinned to one connection (MaxOpenConns=1), the connection cannot be
// reused for another query while rows are open, which provides the same
// mutual-exclusion guarantee.
//
// Staleness is bounded by Config.ReplicaRefreshInterval (default 1 ms).
type replicaPool struct {
	idle       chan *sql.DB   // buffered channel of idle replicas
	inUse      sync.WaitGroup // counts replicas currently checked out
	refreshing atomic.Bool    // true while refresh() is draining/waiting
	closed     atomic.Bool    // true after close() has been called
	driverName string

	// lastRefreshedGen records the value of DB.writeGen observed at the
	// start of the most recent successful refresh. On the next tick,
	// refresh() compares the current writeGen to this value and skips
	// the expensive sqlite3_serialize + fan-out deserialize when no
	// write has occurred since. This turns a read-heavy steady state
	// into an atomic-load + compare — measured in nanoseconds — instead
	// of a per-replica memcpy of the entire serialised database image.
	//
	// The field is only written from the single refresh goroutine (see
	// replicaRefreshLoop in memdb.go) so it does not need atomic access.
	// It is read from the same goroutine too, so no ordering concern.
	lastRefreshedGen uint64

	// seeded is false until the first refresh has populated every replica
	// from the writer's serialised state. The no-op fast-path in refresh
	// keys off both seeded and lastRefreshedGen so the one-shot initial
	// seed in newReplicaPool always runs — even when writeGen is still 0
	// (for example when InitSchema is nil and no WAL replay happened, or
	// when WAL replay used d.mem.Exec directly and therefore did not
	// bump the write generation counter).
	//
	// Like lastRefreshedGen, this is only written from the single refresh
	// goroutine so it does not need atomic access.
	seeded bool
}

// releaser is the per-checkout state used to return a replica to the pool
// exactly once. Using a small struct with an atomic flag avoids the three
// allocations (sync.Once + outer closure + inner closure) that the previous
// implementation incurred on every read.
type releaser struct {
	pool *replicaPool
	r    *sql.DB
	done atomic.Bool
}

func (rl *releaser) release() {
	if !rl.done.CompareAndSwap(false, true) {
		return
	}
	if rl.pool.closed.Load() {
		_ = rl.r.Close()
		rl.pool.inUse.Done()
		return
	}
	rl.pool.idle <- rl.r
	rl.pool.inUse.Done()
}

// newReplicaPool creates n replica databases, seeds each with the current
// writer state, and places them into the idle channel ready for checkout.
func newReplicaPool(d *DB, n int, driverName string) (*replicaPool, error) {
	p := &replicaPool{
		idle:       make(chan *sql.DB, n),
		driverName: driverName,
	}

	for i := 0; i < n; i++ {
		r, err := sql.Open(driverName, ":memory:")
		if err != nil {
			p.close()
			return nil, fmt.Errorf("memdb: replica %d open: %w", i, err)
		}
		// Each replica is a single independent SQLite instance — pin it to
		// exactly one connection so there is no internal pool churn.
		r.SetMaxOpenConns(1)
		r.SetMaxIdleConns(1)
		p.idle <- r
	}

	// Seed all replicas with the writer's current state before the pool goes
	// live. refresh() drains and refills the channel, so this is safe even
	// though the replicas were just placed there above. The initial seed is
	// a one-shot during Open and cannot be interrupted, so context.Background
	// is appropriate here.
	if err := p.refresh(context.Background(), d); err != nil {
		p.close()
		return nil, err
	}

	return p, nil
}

// checkout removes a replica from the idle channel for exclusive use.
// The caller must call the returned release function when done — this
// returns the replica to the channel and decrements inUse.
//
// If the channel is empty (all replicas busy), or if a refresh is in progress,
// returns (nil, nil) and the caller should fall back to the writer connection.
// Yielding to refresh prevents read-load starvation: without this guard, a
// hot read path can repeatedly hand released replicas back to checkouts and
// keep the channel empty enough that refresh's blocking <-p.idle never
// completes.
func (p *replicaPool) checkout() (*sql.DB, func()) {
	if p.refreshing.Load() {
		return nil, nil
	}
	var r *sql.DB
	select {
	case r = <-p.idle:
	default:
		return nil, nil
	}
	p.inUse.Add(1)
	rl := &releaser{pool: p, r: r}
	return r, rl.release
}

// refresh serializes the writer's current state and deserializes it into every
// replica.
//
// It first drains the idle channel (collecting all currently-idle replicas),
// then calls inUse.Wait() to block until every checked-out replica has been
// released by its caller. Only then does it call sqlite3_deserialize — at
// that point no goroutine holds a reference to any replica and no open cursor
// can exist on any of them.
//
// Once deserialization is complete all replicas are returned to the channel
// and normal reads resume. The blackout window is: channel drain latency
// (nanoseconds per replica) + inUse.Wait() (zero when no reads are in flight)
// + deserialize latency (in-process memcpy, microseconds).
//
// ctx is threaded into the underlying withRawConn calls so a cancellation
// (typically because the parent DB is shutting down) interrupts a refresh
// that is otherwise blocked waiting on a leaked *sql.Rows holding a
// replica's single connection (MaxOpenConns=1).
func (p *replicaPool) refresh(ctx context.Context, d *DB) error {
	// Fast-path: if the pool has already been seeded once AND no write
	// has occurred since the last successful refresh, every replica
	// already holds the correct bytes. Skip the drain / serialize /
	// deserialize cycle entirely. This is the common case in read-heavy
	// workloads where the writer is idle between refresh ticks.
	//
	// The seeded guard ensures the first refresh (called from
	// newReplicaPool during Open) always runs, even when writeGen is
	// still 0 — otherwise a DB opened with no InitSchema and no WAL
	// replay would leave every replica pointing at an empty page image
	// while the writer has a valid (but trivial) schema.
	//
	// Under concurrent writes the generation counter may be bumped
	// between the load here and the serialize step below — that is
	// harmless: the refresh will pick up the newer state, and
	// lastRefreshedGen is captured AFTER the load so the next tick
	// correctly re-evaluates. A rare spurious refresh is preferable to
	// a rare skipped refresh (which would leave replicas stale beyond
	// one tick).
	currentGen := d.writeGen.Load()
	if p.seeded && currentGen == p.lastRefreshedGen {
		return nil
	}

	// Signal checkout() to fall back to the writer for the duration of the
	// refresh, so released replicas land in the idle channel where this
	// goroutine can drain them rather than being immediately handed back
	// out to another reader.
	p.refreshing.Store(true)
	defer p.refreshing.Store(false)

	n := cap(p.idle)

	// Step 1 — Drain: collect every idle replica from the channel.
	// Replicas that are currently checked out are absent from the channel;
	// step 2 waits for them.
	replicas := make([]*sql.DB, 0, n)
	for len(replicas) < n {
		replicas = append(replicas, <-p.idle)
	}

	// Step 2 — Wait for every checked-out replica to be released.
	// After inUse.Wait() returns, all N replicas are in our local slice and
	// no goroutine holds a reference to any of them.
	p.inUse.Wait()

	// Step 3 — Serialize the writer.
	// All replicas are now idle and no cursor is open on any of them. The
	// writer is single-connection (MaxOpenConns=1) so no write is in flight.
	var data []byte
	if err := withRawConn(ctx, d.mem, func(conn *sqlite3.SQLiteConn) error {
		var err error
		data, err = conn.Serialize("main")
		return err
	}); err != nil {
		// Serialize failed — return all replicas to the channel unmodified so
		// the pool remains usable (reads will serve stale data, which is the
		// documented trade-off for a failed refresh).
		for _, r := range replicas {
			p.idle <- r
		}
		return fmt.Errorf("memdb: replica serialize: %w", err)
	}

	// Step 4 — Deserialize into each replica and return it to the channel
	// immediately after it is updated so reads can resume as quickly as possible.
	for i, r := range replicas {
		if err := withRawConn(ctx, r, func(conn *sqlite3.SQLiteConn) error {
			return conn.Deserialize(data, "main")
		}); err != nil {
			// Deserialize failed on replica i. Return all remaining replicas
			// (including i itself) to the channel before surfacing the error
			// so the pool stays fully populated.
			for _, rem := range replicas[i:] {
				p.idle <- rem
			}
			return fmt.Errorf("memdb: replica %d deserialize: %w", i, err)
		}
		p.idle <- r
	}

	// Record the generation observed at the top of the function. Using
	// the snapshotted value (not a fresh Load) avoids "losing" a write
	// that happened during the refresh itself — that write will bump
	// writeGen past currentGen and trigger a follow-up refresh on the
	// next tick, which is exactly what we want.
	p.lastRefreshedGen = currentGen
	p.seeded = true

	return nil
}

// close drains the idle channel and closes every idle replica connection.
// Replicas currently checked out by callers are closed when their release()
// runs, because the closed flag is set first.
func (p *replicaPool) close() {
	p.closed.Store(true)
	for {
		select {
		case r := <-p.idle:
			r.Close()
		default:
			return
		}
	}
}
