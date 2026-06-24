package memdb_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/voicetel/memdb"
)

// TestRegisterDriverConcurrentOpen reproduces the #476 driver-registration race:
// when many goroutines Open databases that resolve to the same driver key at
// once, none must hit database/sql's "unknown driver" error. Before the fix,
// registerDriver published the generated driver name into its shared map
// BEFORE calling sql.Register, so a racing Open could Load that name and pass
// it to sql.Open before registration completed.
//
// Each iteration uses a distinct CacheSize so the driver key is fresh (not
// already registered by a prior iteration or another test), forcing a
// first-time registration that exercises the publish/register ordering. The
// configs are built on the test goroutine — testConfig calls t.Fatal, which is
// only safe there — and only Open runs concurrently, released together via a
// start barrier to maximize contention.
func TestRegisterDriverConcurrentOpen(t *testing.T) {
	const iterations = 25
	const goroutines = 16

	for i := 0; i < iterations; i++ {
		cacheSize := -64000 - i // distinct key per iteration (PRAGMA cache_size)

		cfgs := make([]memdb.Config, goroutines)
		for g := range cfgs {
			cfgs[g] = testConfig(t)
			cfgs[g].CacheSize = cacheSize
		}

		start := make(chan struct{})
		errs := make(chan error, goroutines)
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(cfg memdb.Config) {
				defer wg.Done()
				<-start
				db, err := memdb.Open(cfg)
				if err != nil {
					errs <- err
					return
				}
				_ = db.Close()
			}(cfgs[g])
		}
		close(start)
		wg.Wait()
		close(errs)

		for err := range errs {
			if strings.Contains(err.Error(), "unknown driver") {
				t.Fatalf("iteration %d: register-before-publish race: %v", i, err)
			}
			t.Fatalf("iteration %d: concurrent Open failed: %v", i, err)
		}
	}
}
