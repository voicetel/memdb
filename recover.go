package memdb

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// safeDo calls fn synchronously. If fn panics the panic is recovered,
// logged, and — when onErr is non-nil — forwarded to the error handler.
// The caller never sees the panic propagate.
//
// Use safeDo when you need panic-safe execution on the current goroutine
// (e.g. calling user-supplied callbacks such as OnChange, OnFlushError,
// OnFlushComplete, or InitSchema where a misbehaving callback must not
// take down the entire process).
//
// Returned errors from fn are NOT captured here — the caller receives them
// normally via the return value. safeDo only handles panics.
func safeDo(log *slog.Logger, onErr func(error), fn func()) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}

		// Capture the stack trace at the point of the panic so the log
		// entry is immediately actionable without needing a core dump.
		stack := debug.Stack()

		var err error
		switch v := r.(type) {
		case error:
			err = fmt.Errorf("memdb: recovered panic: %w\n%s", v, stack)
		default:
			err = fmt.Errorf("memdb: recovered panic: %v\n%s", v, stack)
		}

		if log == nil {
			log = slog.Default()
		}
		log.Error("memdb: panic recovered", "error", err)

		if onErr != nil {
			// Call the error handler inside a nested safeDo so a
			// panicking error handler cannot escape back to the caller.
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						log.Error("memdb: panic in OnFlushError handler",
							"panic", fmt.Sprintf("%v", r2),
						)
					}
				}()
				onErr(err)
			}()
		}
	}()

	fn()
}

// safeCallback calls a user-supplied callback fn with the given argument,
// recovering any panic and logging it. Returns the callback's error (or nil
// when fn is nil). Panics from fn are never propagated — they are logged and
// optionally forwarded to onErr, then the function returns nil (the panic is
// treated as a failed-but-non-fatal callback invocation so the caller can
// continue operating).
//
// This is the correct wrapper for OnChange and similar fire-and-forget
// callbacks that do not return errors themselves.
func safeCallback[T any](log *slog.Logger, onErr func(error), fn func(T), arg T) {
	if fn == nil {
		return
	}
	safeDo(log, onErr, func() { fn(arg) })
}
