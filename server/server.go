package server

import (
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/voicetel/memdb"
)

// Config holds options for the memdb wire-protocol server.
type Config struct {
	// ListenAddr is a TCP address or unix socket path.
	// Prefix with "unix://" for a Unix domain socket.
	// Example: "127.0.0.1:5433" or "unix:///var/run/memdb/memdb.sock"
	ListenAddr string

	// TLSConfig enables TLS on TCP listeners. Ignored for Unix sockets.
	//
	// Performance note: Go 1.24+ enables a hybrid X25519+MLKEM768
	// post-quantum key exchange by default. Connect-cycle pprof showed
	// the MLKEM and SHA-3 (Keccak) functions accounting for ~7% of CPU
	// per handshake; combined with EC field arithmetic the full handshake
	// runs ~3× slower than plain TCP for short-lived connections (~4.3k
	// vs ~13k cycles/s on the test hardware). For deployments without a
	// PQC requirement, set CurvePreferences to []tls.CurveID{tls.X25519,
	// tls.CurveP256} to recover most of that. For deployments that DO
	// need PQC, the cleanest mitigation is a connection pool on the
	// client so the handshake amortises across many queries.
	TLSConfig *tls.Config

	// Auth is optional. If nil, no authentication is required.
	Auth Authenticator

	// Logger is used for structured log output from the server (connection
	// errors, recovered panics). If nil, slog.Default() is used.
	Logger *slog.Logger
}

// logger returns the configured logger or slog.Default().
func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// Authenticator validates a username/password pair.
type Authenticator interface {
	Authenticate(username, password string) bool
}

// BasicAuth is a simple static credential authenticator.
//
// Username and Password are compared in constant time via
// crypto/subtle.ConstantTimeCompare so that an attacker cannot use
// response-timing differences to recover the credentials one character at
// a time. Go's native == on strings short-circuits at the first differing
// byte, which is measurable over a network.
type BasicAuth struct {
	Username string
	Password string
}

// Authenticate returns true when both username and password match the
// configured values. Both comparisons run in constant time relative to
// the lengths of the inputs, and the boolean results are AND'ed with a
// bitwise-and (not &&) so the second comparison is always evaluated
// regardless of the first result. Length mismatch between the inputs and
// the configured values short-circuits to false but does not leak the
// configured length — ConstantTimeCompare returns 0 on any length
// mismatch without revealing which side was longer.
func (a BasicAuth) Authenticate(username, password string) bool {
	userOK := subtle.ConstantTimeCompare([]byte(username), []byte(a.Username))
	passOK := subtle.ConstantTimeCompare([]byte(password), []byte(a.Password))
	return userOK&passOK == 1
}

// Server is a PostgreSQL wire-protocol server backed by a memdb.DB.
type Server struct {
	db       *memdb.DB
	cfg      Config
	mu       sync.Mutex // protects listener
	listener net.Listener
	wg       sync.WaitGroup
	stopOnce sync.Once
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
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

	for {
		conn, err := l.Accept()
		if err != nil {
			// Listener deliberately closed via Stop.
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			select {
			case <-s.quit:
				return nil
			default:
			}
			// Transient errors (EMFILE, ENFILE, etc.) should not kill the
			// server — but returning immediately can produce a tight busy
			// loop. Back off briefly before retrying.
			time.Sleep(10 * time.Millisecond)
			continue
		}
		select {
		case <-s.quit:
			conn.Close()
			return nil
		default:
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					// A panicking handler must not crash the server process.
					// Log the panic and close the connection so the client
					// gets a clean EOF rather than a half-written response.
					// Other connections are unaffected.
					conn.Close()
					s.cfg.logger().Error("memdb server: handler panic recovered",
						"panic", r,
						"remote", conn.RemoteAddr(),
					)
				}
			}()
			newHandler(s.db, s.cfg, conn).serve()
		}()
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.quit)
		s.mu.Lock()
		if s.listener != nil {
			s.listener.Close()
		}
		s.mu.Unlock()
	})
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
