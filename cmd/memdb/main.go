package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/logging"
	"github.com/voicetel/memdb/profiling"
	"github.com/voicetel/memdb/server"
)

func main() {
	// Configure slog to write to syslog, falling back to stderr text logging.
	if logger, err := logging.NewSyslogHandler("memdb", slog.LevelInfo); err == nil {
		slog.SetDefault(logger)
	} else {
		slog.SetDefault(logging.NewTextHandler(os.Stderr, slog.LevelInfo))
	}

	serveCmd := flag.NewFlagSet("serve", flag.ExitOnError)
	serveFile := serveCmd.String("file", "memdb.db", "path to SQLite snapshot file")
	serveAddr := serveCmd.String("addr", "127.0.0.1:5433", "listen address (TCP or unix://path)")
	serveFlush := serveCmd.Duration("flush", 30*time.Second, "flush interval")
	servePprof := serveCmd.String("pprof", "",
		"enable net/http/pprof on the given address (e.g. 127.0.0.1:6060); empty disables")
	servePprofMutex := serveCmd.Int("pprof-mutex-fraction", 0,
		"mutex profile sampling fraction (see runtime.SetMutexProfileFraction); 0 disables")
	servePprofBlock := serveCmd.Int("pprof-block-rate", 0,
		"block profile sampling rate in nanoseconds (see runtime.SetBlockProfileRate); 0 disables")

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
			slog.Error("serve parse flags", "error", err)
			os.Exit(1)
		}
		runServe(*serveFile, *serveAddr, *serveFlush,
			*servePprof, *servePprofMutex, *servePprofBlock)

	case "snapshot":
		if err := snapCmd.Parse(os.Args[2:]); err != nil {
			slog.Error("snapshot parse flags", "error", err)
			os.Exit(1)
		}
		runSnapshot(*snapFile)

	case "restore":
		if err := restoreCmd.Parse(os.Args[2:]); err != nil {
			slog.Error("restore parse flags", "error", err)
			os.Exit(1)
		}
		if *restoreFrom == "" {
			slog.Error("--from is required")
			os.Exit(1)
		}
		runRestore(*restoreFrom, *restoreTo)

	default:
		usage()
		os.Exit(1)
	}
}

func runServe(file, addr string, flush time.Duration,
	pprofAddr string, pprofMutex, pprofBlock int) {
	// If using a Unix socket, remove any stale socket file from a previous run.
	if strings.HasPrefix(addr, "unix://") {
		path := strings.TrimPrefix(addr, "unix://")
		if fi, err := os.Stat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
			// Existing socket — likely stale from a previous crash.
			os.Remove(path)
		}
	}

	// Optional profiling HTTP server. Bound to a loopback address by default
	// because /debug/pprof/heap exposes full process memory — see profiling
	// package docs for security notes.
	var profSrv *profiling.Server
	if pprofAddr != "" {
		var err error
		profSrv, err = profiling.StartServer(profiling.Config{
			Addr:                 pprofAddr,
			MutexProfileFraction: pprofMutex,
			BlockProfileRate:     pprofBlock,
		})
		if err != nil {
			slog.Error("pprof server failed to start", "addr", pprofAddr, "error", err)
			os.Exit(1)
		}
		slog.Info("pprof listening", "addr", profSrv.Addr())
		defer func() {
			if err := profSrv.Close(); err != nil {
				slog.Warn("pprof shutdown error", "error", err)
			}
		}()
	}

	db, err := memdb.Open(memdb.Config{
		FilePath:      file,
		FlushInterval: flush,
		Durability:    memdb.DurabilityWAL,
		OnFlushError: func(err error) {
			slog.Error("flush error", "error", err)
		},
	})
	if err != nil {
		slog.Error("open failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	srv := server.New(db, server.Config{ListenAddr: addr})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	go func() {
		<-quit
		srv.Stop()
	}()

	slog.Info("memdb listening", "addr", addr, "file", file, "flush", flush)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server stopped unexpectedly", "error", err)
	}
}

func runSnapshot(file string) {
	db, err := memdb.Open(memdb.Config{
		FilePath:      file,
		FlushInterval: -1, // no background flush
	})
	if err != nil {
		slog.Error("open failed", "error", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		slog.Error("flush failed", "error", err)
		os.Exit(1)
	}
	if err := db.Close(); err != nil {
		slog.Warn("close error", "error", err)
	}
	slog.Info("snapshot written", "file", file)
}

func runRestore(from, to string) {
	if _, err := os.Stat(from); err != nil {
		slog.Error("source not found", "error", err)
		os.Exit(1)
	}
	if err := copyFileAtomic(from, to); err != nil {
		slog.Error("restore failed", "error", err)
		os.Exit(1)
	}
	slog.Info("snapshot restored", "from", from, "to", to)
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
	// fsync the parent directory so the rename is durable.
	if dir, err := os.Open(filepath.Dir(dst)); err == nil {
		_ = dir.Sync()
		dir.Close()
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
