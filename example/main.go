package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/voicetel/memdb"
)

func main() {
	db, err := memdb.Open(memdb.Config{
		FilePath:      "/tmp/example.db",
		FlushInterval: 10 * time.Second,
		Durability:    memdb.DurabilityWAL,
		OnFlushError: func(err error) {
			log.Printf("flush error: %v", err)
		},
		OnChange: func(e memdb.ChangeEvent) {
			log.Printf("change: %s %s rowid=%d", e.Op, e.Table, e.RowID)
		},
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`
				CREATE TABLE IF NOT EXISTS sessions (
					id    TEXT PRIMARY KEY,
					data  BLOB    NOT NULL,
					ts    INTEGER NOT NULL
				);
				CREATE INDEX IF NOT EXISTS sessions_ts ON sessions(ts);
			`)
			return err
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Write — hits memory only until next flush
	_, err = db.Exec(
		`INSERT OR REPLACE INTO sessions (id, data, ts) VALUES (?, ?, ?)`,
		"session-abc", []byte(`{"user_id":42}`), time.Now().Unix(),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Read — sub-millisecond, no disk I/O
	var data []byte
	err = db.QueryRow(
		`SELECT data FROM sessions WHERE id = ?`, "session-abc",
	).Scan(&data)
	if err == sql.ErrNoRows {
		log.Println("not found")
	} else if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("session data: %s", data)
	}

	// Transaction
	err = memdb.WithTx(context.Background(), db, func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE sessions SET ts = ? WHERE id = ?`,
			time.Now().Unix(), "session-abc",
		)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	// Manual flush for guaranteed durability
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Println("flushed to disk")
}
