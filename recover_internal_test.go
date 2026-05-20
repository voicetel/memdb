package memdb

// White-box tests for safeDo / safeCallback panic recovery. These are
// difficult to drive end-to-end through public callbacks (OnFlushError
// etc.) because the panic paths are deep inside flush goroutines, but
// the helpers are pure functions and trivial to exercise directly.

import (
	"bytes"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
)

func TestSafeDo_NoPanic(t *testing.T) {
	var ran bool
	var seenErr atomic.Pointer[error]
	safeDo(slog.Default(), func(err error) { seenErr.Store(&err) }, func() {
		ran = true
	})
	if !ran {
		t.Error("fn did not run")
	}
	if seenErr.Load() != nil {
		t.Error("onErr called when fn did not panic")
	}
}

func TestSafeDo_RecoversErrorPanic(t *testing.T) {
	wantPanic := errors.New("err panic")
	var seenErr atomic.Pointer[error]
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped safeDo: %v", r)
		}
	}()
	safeDo(slog.Default(), func(err error) { seenErr.Store(&err) }, func() {
		panic(wantPanic)
	})
	got := seenErr.Load()
	if got == nil {
		t.Fatal("onErr was not called")
	}
	if !errors.Is(*got, wantPanic) {
		t.Errorf("error chain missing wantPanic: %v", *got)
	}
}

func TestSafeDo_RecoversValuePanic(t *testing.T) {
	var seenErr atomic.Pointer[error]
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped safeDo: %v", r)
		}
	}()
	safeDo(slog.Default(), func(err error) { seenErr.Store(&err) }, func() {
		panic("string panic")
	})
	got := seenErr.Load()
	if got == nil {
		t.Fatal("onErr was not called")
	}
	if !contains((*got).Error(), "string panic") {
		t.Errorf("error message missing panic value: %q", (*got).Error())
	}
}

func TestSafeDo_NilLoggerUsesDefault(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped: %v", r)
		}
	}()
	safeDo(nil, nil, func() { panic("nil-logger panic") })
}

func TestSafeDo_NilOnErrIsSafe(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped: %v", r)
		}
	}()
	safeDo(slog.Default(), nil, func() { panic("nil-onErr panic") })
}

func TestSafeDo_OnErrPanicIsContained(t *testing.T) {
	// Capture log output to confirm the nested-handler panic was
	// logged (the failure mode is "outer panic recovered but handler
	// panic propagated").
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelError}))

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped safeDo: %v", r)
		}
	}()
	safeDo(logger, func(error) { panic("handler panic") }, func() {
		panic("inner panic")
	})

	logged := logBuf.String()
	if !contains(logged, "panic recovered") {
		t.Errorf("missing 'panic recovered' log: %q", logged)
	}
	if !contains(logged, "OnFlushError") {
		t.Errorf("missing nested-handler log: %q", logged)
	}
}

func TestSafeCallback_NilCallbackIsNoop(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped: %v", r)
		}
	}()
	safeCallback[int](slog.Default(), nil, nil, 42)
}

func TestSafeCallback_PassesArg(t *testing.T) {
	var got int
	safeCallback(slog.Default(), nil, func(v int) { got = v }, 7)
	if got != 7 {
		t.Errorf("got %d, want 7", got)
	}
}

func TestSafeCallback_RecoversPanic(t *testing.T) {
	var seenErr atomic.Pointer[error]
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped: %v", r)
		}
	}()
	safeCallback(slog.Default(), func(err error) { seenErr.Store(&err) },
		func(string) { panic("cb panic") }, "x")
	if seenErr.Load() == nil {
		t.Error("onErr was not called")
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
