package memdb

import "errors"

var (
	ErrFlushFailed             = errors.New("memdb: flush to disk failed")
	ErrRestoreFailed           = errors.New("memdb: restore from disk failed")
	ErrClosed                  = errors.New("memdb: database is closed")
	ErrTransactionNotSupported = errors.New("memdb: transactions not supported in Raft mode")
)
