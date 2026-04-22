package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/server"
)

func main() {
	serveCmd := flag.NewFlagSet("serve", flag.ExitOnError)
	serveFile := serveCmd.String("file", "memdb.db", "path to SQLite snapshot file")
	serveAddr := serveCmd.String("addr", "127.0.0.1:5433", "listen address (TCP or unix://path)")
	serveFlush := serveCmd.Duration("flush", 30*time.Second, "flush interval")

	snapCmd := flag.NewFlagSet("snapshot", flag.ExitOnError)
	snapFile := snapCmd.String("file", "memdb.db", "path to SQLite snapshot file")

	restoreCmd := flag.NewFlagSet("restore", flag.ExitOnError)
	restoreFrom := restoreCmd.String("from", "", "snapshot file to restore from")
	restoreTo := restoreCmd.String("to", "memdb.db", "destination file")

	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		if err := serveCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalf("serve: %v", err)
		}
		runServe(*serveFile, *serveAddr, *serveFlush)

	case "snapshot":
		if err := snapCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalf("snapshot: %v", err)
		}
		runSnapshot(*snapFile)

	case "restore":
		if err := restoreCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalf("restore: %v", err)
		}
		if *restoreFrom == "" {
			log.Fatal("--from is required")
		}
		runRestore(*restoreFrom, *restoreTo)

	default:
		usage()
		os.Exit(1)
	}
}

func runServe(file, addr string, flush time.Duration) {
	db, err := memdb.Open(memdb.Config{
		FilePath:      file,
		FlushInterval: flush,
		Durability:    memdb.DurabilityWAL,
		OnFlushError: func(err error) {
			log.Printf("ERROR flush: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	srv := server.New(db, server.Config{ListenAddr: addr})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		srv.Stop()
	}()

	log.Printf("memdb listening on %s  file=%s  flush=%s", addr, file, flush)
	if err := srv.ListenAndServe(); err != nil {
		log.Printf("server: %v", err)
	}
}

func runSnapshot(file string) {
	db, err := memdb.Open(memdb.Config{
		FilePath:      file,
		FlushInterval: -1, // no background flush
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		log.Printf("close: %v", err)
	}
	log.Printf("snapshot written to %s", file)
}

func runRestore(from, to string) {
	if _, err := os.Stat(from); err != nil {
		log.Fatalf("source not found: %v", err)
	}
	if err := copyFileAtomic(from, to); err != nil {
		log.Fatalf("restore: %v", err)
	}
	log.Printf("restored %s → %s", from, to)
}

// copyFileAtomic copies src to dst using a temp file + rename for atomicity.
// This avoids loading the entire file into memory and prevents a corrupt
// destination if the process is killed mid-write.
func copyFileAtomic(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer in.Close()

	dir := filepath.Dir(dst)
	tmp, err := os.CreateTemp(dir, ".memdb-restore-*.db")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("copy: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync: %w", err)
	}
	tmp.Close()

	if err := os.Rename(tmpName, dst); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: memdb <command> [flags]

Commands:
  serve       Start the PostgreSQL wire-protocol server
  snapshot    Force a snapshot flush of the in-memory DB to disk
  restore     Copy a snapshot file to a new location

Run 'memdb <command> -h' for flag details.
`)
}
