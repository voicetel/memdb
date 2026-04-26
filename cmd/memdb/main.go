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
	// Configure slog. Always fall back to stderr; mirror to syslog in
	// production so log aggregators receive events too.
	if syslogLogger, err := logging.NewSyslogHandler("memdb", slog.LevelInfo); err == nil {
		slog.SetDefault(syslogLogger)
	} else {
		slog.SetDefault(logging.NewTextHandler(os.Stderr, slog.LevelInfo))
	}

	serveCmd := flag.NewFlagSet("serve", flag.ExitOnError)
	serveFile := serveCmd.String("file", "memdb.db", "path to SQLite snapshot file")
	serveAddr := serveCmd.String("addr", "127.0.0.1:5433", "listen address (TCP or unix://path)")
	serveFlush := serveCmd.Duration("flush", 30*time.Second, "flush interval")
	serveDurability := serveCmd.String("durability", "",
		"durability mode: none|wal|sync. Default: wal standalone, none with raft "+
			"(raft's log is already durable, so memdb's WAL is redundant)")
	servePprof := serveCmd.String("pprof", "",
		"enable net/http/pprof on the given address (e.g. 127.0.0.1:6060); empty disables")
	servePprofMutex := serveCmd.Int("pprof-mutex-fraction", 0,
		"mutex profile sampling fraction (see runtime.SetMutexProfileFraction); 0 disables")
	servePprofBlock := serveCmd.Int("pprof-block-rate", 0,
		"block profile sampling rate in nanoseconds (see runtime.SetBlockProfileRate); 0 disables")
	serveAuthUser := serveCmd.String("auth-user", "",
		"username required for client connections; empty disables auth")
	serveAuthPassword := serveCmd.String("auth-password", "",
		"password for -auth-user (also reads MEMDB_AUTH_PASSWORD env var if flag is empty)")
	serveAuthMethod := serveCmd.String("auth-method", "scram",
		"auth method: scram (SCRAM-SHA-256, recommended) or cleartext")
	serveRaft := registerRaftFlags(serveCmd)

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
		runServe(*serveFile, *serveAddr, *serveFlush, *serveDurability,
			*servePprof, *servePprofMutex, *servePprofBlock,
			*serveAuthUser, *serveAuthPassword, *serveAuthMethod,
			serveRaft)

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

// buildAuthenticator constructs the server.Authenticator from the
// -auth-* flags. Returns nil when -auth-user is empty (auth disabled).
//
// The password is read from MEMDB_AUTH_PASSWORD when -auth-password is
// empty, so deployments can avoid leaking it through `ps`/`/proc/<pid>/cmdline`
// while still configuring it declaratively (e.g. via systemd EnvironmentFile).
func buildAuthenticator(user, password, method string) (server.Authenticator, error) {
	if user == "" {
		return nil, nil
	}
	if password == "" {
		password = os.Getenv("MEMDB_AUTH_PASSWORD")
	}
	if password == "" {
		return nil, fmt.Errorf("auth-user %q requires -auth-password or MEMDB_AUTH_PASSWORD env var", user)
	}
	switch strings.ToLower(method) {
	case "scram", "":
		return server.NewScramAuth(user, password), nil
	case "cleartext":
		return server.BasicAuth{Username: user, Password: password}, nil
	default:
		return nil, fmt.Errorf("invalid -auth-method %q (want scram|cleartext)", method)
	}
}

// resolveDurability maps the -durability CLI flag to a memdb.DurabilityMode.
// An empty flag selects DurabilityNone in raft mode (Raft's log is already
// the durability layer; memdb's WAL would be a redundant per-write fsync)
// and DurabilityWAL standalone (the safer default for a single-node DB).
func resolveDurability(flag string, raftEnabled bool) (memdb.DurabilityMode, error) {
	switch strings.ToLower(flag) {
	case "":
		if raftEnabled {
			return memdb.DurabilityNone, nil
		}
		return memdb.DurabilityWAL, nil
	case "none":
		return memdb.DurabilityNone, nil
	case "wal":
		return memdb.DurabilityWAL, nil
	case "sync":
		return memdb.DurabilitySync, nil
	default:
		return 0, fmt.Errorf("invalid -durability %q (want none|wal|sync)", flag)
	}
}

func durabilityName(m memdb.DurabilityMode) string {
	switch m {
	case memdb.DurabilityNone:
		return "none"
	case memdb.DurabilityWAL:
		return "wal"
	case memdb.DurabilitySync:
		return "sync"
	default:
		return "unknown"
	}
}

func runServe(file, addr string, flush time.Duration, durability string,
	pprofAddr string, pprofMutex, pprofBlock int,
	authUser, authPassword, authMethod string,
	raftCfg *raftFlags) {
	if err := raftCfg.validate(); err != nil {
		slog.Error("raft flags", "error", err)
		os.Exit(1)
	}

	durabilityMode, err := resolveDurability(durability, raftCfg.enabled())
	if err != nil {
		slog.Error("durability flag", "error", err)
		os.Exit(1)
	}

	authenticator, err := buildAuthenticator(authUser, authPassword, authMethod)
	if err != nil {
		slog.Error("auth flags", "error", err)
		os.Exit(1)
	}

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

	// nodeExec is assigned after the Raft node is built. The OnExec closure
	// captures it by reference so memdb.Open can be called before the node
	// exists — ListenAndServe only starts after assignment, so the nil
	// window is never observable to a concurrent caller.
	var nodeExec func(sql string, args ...any) error
	memCfg := memdb.Config{
		FilePath:      file,
		FlushInterval: flush,
		Durability:    durabilityMode,
		OnFlushError: func(err error) {
			slog.Error("flush error", "error", err)
		},
	}
	if raftCfg.enabled() {
		memCfg.OnExec = func(sql string, args []any) error {
			return nodeExec(sql, args...)
		}
	}

	db, err := memdb.Open(memCfg)
	if err != nil {
		slog.Error("open failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if raftCfg.enabled() {
		node, err := buildRaftNode(db, *raftCfg, slog.Default())
		if err != nil {
			slog.Error("raft start failed", "error", err)
			os.Exit(1)
		}
		nodeExec = node.Exec
		defer func() {
			if err := node.Shutdown(); err != nil {
				slog.Warn("raft shutdown error", "error", err)
			}
		}()
		slog.Info("memdb raft enabled",
			"nodeID", raftCfg.NodeID,
			"bind", raftCfg.BindAddr,
			"forwardBind", raftCfg.ForwardBindAddr,
			"peers", raftCfg.Peers,
		)
	}

	srv := server.New(db, server.Config{ListenAddr: addr, Auth: authenticator})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	go func() {
		<-quit
		srv.Stop()
	}()

	slog.Info("memdb listening",
		"addr", addr, "file", file, "flush", flush,
		"durability", durabilityName(durabilityMode))
	if err := srv.ListenAndServe(); err != nil {
		// Write directly to stderr so a port-conflict or bind error is always
		// visible in the terminal, even when syslog is the default handler.
		// Without this the operator sees a silent zero-exit with no indication
		// of the cause (e.g. "address already in use").
		fmt.Fprintf(os.Stderr, "memdb: server error: %v\n", err)
		slog.Error("server stopped unexpectedly", "error", err)
		os.Exit(1)
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

Replication: pass -raft-node-id (and the other -raft-* flags) to 'serve'
to join a Raft cluster. See 'memdb serve -h' for the full list.
`)
}
