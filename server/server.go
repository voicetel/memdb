package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/voicetel/memdb"
)

// Config holds options for the memdb wire-protocol server.
type Config struct {
	// ListenAddr is a TCP address or unix socket path.
	// Prefix with "unix://" for a Unix domain socket.
	// Example: "127.0.0.1:5433" or "unix:///var/run/memdb/memdb.sock"
	ListenAddr string

	// TLSConfig enables TLS on TCP listeners. Ignored for Unix sockets.
	TLSConfig *tls.Config

	// Auth is optional. If nil, no authentication is required.
	Auth Authenticator
}

// Authenticator validates a username/password pair.
type Authenticator interface {
	Authenticate(username, password string) bool
}

// BasicAuth is a simple static credential authenticator.
type BasicAuth struct {
	Username string
	Password string
}

func (a BasicAuth) Authenticate(username, password string) bool {
	return username == a.Username && password == a.Password
}

// Server is a PostgreSQL wire-protocol server backed by a memdb.DB.
type Server struct {
	db       *memdb.DB
	cfg      Config
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
}

// New creates a new Server. Call ListenAndServe to start accepting connections.
func New(db *memdb.DB, cfg Config) *Server {
	return &Server{
		db:   db,
		cfg:  cfg,
		quit: make(chan struct{}),
	}
}

// ListenAndServe starts the server. Blocks until Stop is called.
func (s *Server) ListenAndServe() error {
	l, err := s.listen()
	if err != nil {
		return err
	}
	s.listener = l

	for {
		conn, err := l.Accept()
		select {
		case <-s.quit:
			return nil
		default:
		}
		if err != nil {
			return fmt.Errorf("server: accept: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			newHandler(s.db, s.cfg, conn).serve()
		}()
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

func (s *Server) listen() (net.Listener, error) {
	addr := s.cfg.ListenAddr
	if strings.HasPrefix(addr, "unix://") {
		return net.Listen("unix", strings.TrimPrefix(addr, "unix://"))
	}
	if s.cfg.TLSConfig != nil {
		return tls.Listen("tcp", addr, s.cfg.TLSConfig)
	}
	return net.Listen("tcp", addr)
}
