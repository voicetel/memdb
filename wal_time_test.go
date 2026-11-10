package memdb_test

// A time.Time argument must end up as the same stored value whether the
// write was applied directly or recovered by replaying the WAL.

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/voicetel/memdb"
)

// TestWAL_Replay_TimeArgsStoreWhatTheDirectPathStores writes time.Time values
// three ways — to a database with no WAL, to a DurabilityWAL database, and by
// replaying that database's WAL after a crash — and compares what SQLite
// actually holds (type and text, not a parsed time).
//
// Times outside the int64-nanosecond range used to be logged as an overflowed
// UnixNano, so replay stored a different date than the direct write had: the
// zero time.Time came back as 1754-08-30 22:43:41.128654848+00:00.
func TestWAL_Replay_TimeArgsStoreWhatTheDirectPathStores(t *testing.T) {
	times := []time.Time{
		{}, // zero time
		time.Date(1, 6, 15, 12, 30, 45, 123456789, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
		time.Unix(0, math.MinInt64).UTC().Add(-1), // just outside the range
		time.Unix(0, math.MaxInt64).UTC().Add(1),
		time.Unix(0, math.MinInt64).UTC(), // the range's boundaries
		time.Unix(0, math.MaxInt64).UTC(),
		time.Unix(0, 0).UTC(),
		time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.UTC),
	}
	schema := func(db *memdb.DB) error {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS ts (id INTEGER PRIMARY KEY, v)`)
		return err
	}
	insert := func(db *memdb.DB) {
		t.Helper()
		for i, v := range times {
			if _, err := db.Exec(`INSERT INTO ts (id, v) VALUES (?, ?)`, i, v); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
	}
	stored := func(db *memdb.DB) []string {
		t.Helper()
		rows, err := db.Query(`SELECT typeof(v) || ':' || CAST(v AS TEXT) FROM ts ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(out) != len(times) {
			t.Fatalf("%d rows stored, want %d", len(out), len(times))
		}
		return out
	}

	// Reference: a direct write with no WAL involved.
	plainCfg := extraTestConfig(t)
	plainCfg.InitSchema = schema
	plain := mustOpen(t, plainCfg)
	insert(plain)
	direct := stored(plain)

	// The same writes under DurabilityWAL, then a crash before any snapshot.
	cfg := walTestConfig(t)
	cfg.InitSchema = schema
	walPath := cfg.FilePath + ".wal"
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	insert(db)
	live := stored(db)
	wal, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(cfg.FilePath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, wal, 0o600); err != nil {
		t.Fatal(err)
	}

	recovered, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = recovered.Close() }()
	replayed := stored(recovered)

	for i, v := range times {
		if live[i] != direct[i] {
			t.Errorf("%s: WAL-mode write stored %q, the direct path stores %q",
				v.Format(time.RFC3339Nano), live[i], direct[i])
		}
		if replayed[i] != direct[i] {
			t.Errorf("%s: replay stored %q, the direct path stores %q",
				v.Format(time.RFC3339Nano), replayed[i], direct[i])
		}
	}
	if want := "text:0001-01-01 00:00:00+00:00"; replayed[0] != want {
		t.Errorf("zero time replayed as %q, want %q", replayed[0], want)
	}
}
