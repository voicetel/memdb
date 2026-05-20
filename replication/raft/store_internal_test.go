package raft

// White-box tests for the package-private fileLogStore and fileStableStore.
// These exist because the live Raft cluster integration tests don't trigger
// every branch (truncation, deletion ranges, corrupt-state rejection), and
// the constructors / inner methods are unexported so a black-box test
// cannot reach them.

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	hraft "github.com/hashicorp/raft"
)

func TestFileLogStore_EmptyStore(t *testing.T) {
	dir := t.TempDir()
	store, err := newLogStore(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("newLogStore: %v", err)
	}
	t.Cleanup(func() { _ = store.(*fileLogStore).Close() })

	first, err := store.FirstIndex()
	if err != nil || first != 0 {
		t.Errorf("FirstIndex on empty store = %d, %v; want 0, nil", first, err)
	}
	last, err := store.LastIndex()
	if err != nil || last != 0 {
		t.Errorf("LastIndex on empty store = %d, %v; want 0, nil", last, err)
	}
	var log hraft.Log
	if err := store.GetLog(1, &log); !errors.Is(err, hraft.ErrLogNotFound) {
		t.Errorf("GetLog on empty store = %v, want ErrLogNotFound", err)
	}
}

func TestFileLogStore_StoreAndGet(t *testing.T) {
	dir := t.TempDir()
	store, err := newLogStore(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.(*fileLogStore).Close() })

	logs := []*hraft.Log{
		{Index: 1, Term: 1, Data: []byte("entry-1")},
		{Index: 2, Term: 1, Data: []byte("entry-2")},
		{Index: 3, Term: 2, Data: []byte("entry-3")},
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 1 || last != 3 {
		t.Errorf("First/Last = %d/%d, want 1/3", first, last)
	}

	for _, want := range logs {
		var got hraft.Log
		if err := store.GetLog(want.Index, &got); err != nil {
			t.Fatalf("GetLog %d: %v", want.Index, err)
		}
		if got.Index != want.Index || got.Term != want.Term || string(got.Data) != string(want.Data) {
			t.Errorf("GetLog %d: got %+v, want %+v", want.Index, got, want)
		}
	}
}

func TestFileLogStore_StoreLog_SingleEntry(t *testing.T) {
	dir := t.TempDir()
	store, err := newLogStore(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.(*fileLogStore).Close() })

	if err := store.StoreLog(&hraft.Log{Index: 7, Term: 1, Data: []byte("solo")}); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 7 || last != 7 {
		t.Errorf("Single-entry First/Last = %d/%d, want 7/7", first, last)
	}
}

func TestFileLogStore_DeleteRange_Middle(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "log")
	store, err := newLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.(*fileLogStore).Close()

	for i := uint64(1); i <= 5; i++ {
		if err := store.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteRange(2, 4); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 1 || last != 5 {
		t.Errorf("After DeleteRange(2,4) First/Last = %d/%d, want 1/5", first, last)
	}

	// Surviving entries 1 and 5 must still be readable; the deleted ones
	// must report ErrLogNotFound.
	for _, idx := range []uint64{1, 5} {
		var l hraft.Log
		if err := store.GetLog(idx, &l); err != nil {
			t.Errorf("GetLog %d after delete: %v", idx, err)
		}
	}
	for _, idx := range []uint64{2, 3, 4} {
		var l hraft.Log
		if err := store.GetLog(idx, &l); !errors.Is(err, hraft.ErrLogNotFound) {
			t.Errorf("GetLog %d after delete: got %v, want ErrLogNotFound", idx, err)
		}
	}
}

func TestFileLogStore_DeleteRange_All(t *testing.T) {
	dir := t.TempDir()
	store, err := newLogStore(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.(*fileLogStore).Close()

	for i := uint64(1); i <= 3; i++ {
		_ = store.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")})
	}
	if err := store.DeleteRange(1, 3); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 0 || last != 0 {
		t.Errorf("Empty after DeleteRange(1,3): First/Last = %d/%d, want 0/0", first, last)
	}
}

func TestFileLogStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "log")

	store, err := newLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		_ = store.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte{byte(i)}})
	}
	_ = store.(*fileLogStore).Close()

	// Reopen — the load() path scans the file and rebuilds the index.
	reopened, err := newLogStore(logPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.(*fileLogStore).Close() })

	first, _ := reopened.FirstIndex()
	last, _ := reopened.LastIndex()
	if first != 1 || last != 4 {
		t.Errorf("Reopened First/Last = %d/%d, want 1/4", first, last)
	}
	indices := make([]uint64, 0, 4)
	for i := uint64(1); i <= 4; i++ {
		var l hraft.Log
		if err := reopened.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog %d: %v", i, err)
		}
		indices = append(indices, l.Index)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	for i, want := range []uint64{1, 2, 3, 4} {
		if indices[i] != want {
			t.Errorf("indices[%d]=%d, want %d", i, indices[i], want)
		}
	}
}

func TestFileLogStore_LoadStopsAtTruncation(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "log")

	store, err := newLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		_ = store.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte{byte(i)}})
	}
	_ = store.(*fileLogStore).Close()

	// Append a malformed length prefix so load() stops before reading it.
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("garbage")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	reopened, err := newLogStore(logPath)
	if err != nil {
		t.Fatalf("reopen with bad tail: %v", err)
	}
	t.Cleanup(func() { _ = reopened.(*fileLogStore).Close() })

	last, _ := reopened.LastIndex()
	if last != 3 {
		t.Errorf("Last after truncation = %d, want 3", last)
	}
}

func TestFileLogStore_GetLogInvalidIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := newLogStore(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.(*fileLogStore).Close() })

	_ = store.StoreLog(&hraft.Log{Index: 1, Term: 1})
	var l hraft.Log
	if err := store.GetLog(99, &l); !errors.Is(err, hraft.ErrLogNotFound) {
		t.Errorf("GetLog(99): %v, want ErrLogNotFound", err)
	}
}

// ── Stable store ──────────────────────────────────────────────────────────────

func TestFileStableStore_SetGet(t *testing.T) {
	dir := t.TempDir()
	store, err := newStableStore(filepath.Join(dir, "stable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := store.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v1" {
		t.Errorf("got %q, want v1", got)
	}
}

func TestFileStableStore_GetMissingKey(t *testing.T) {
	dir := t.TempDir()
	store, err := newStableStore(filepath.Join(dir, "stable"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get([]byte("missing"))
	if !errors.Is(err, errStableKeyNotFound) {
		t.Errorf("Get missing: %v, want errStableKeyNotFound", err)
	}
}

func TestFileStableStore_Uint64(t *testing.T) {
	dir := t.TempDir()
	store, err := newStableStore(filepath.Join(dir, "stable"))
	if err != nil {
		t.Fatal(err)
	}
	const want uint64 = 0x1122334455667788
	if err := store.SetUint64([]byte("term"), want); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetUint64([]byte("term"))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("GetUint64=%x, want %x", got, want)
	}
}

func TestFileStableStore_GetUint64MissingReturnsZero(t *testing.T) {
	dir := t.TempDir()
	store, err := newStableStore(filepath.Join(dir, "stable"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.GetUint64([]byte("nope"))
	if err != nil || got != 0 {
		t.Errorf("missing GetUint64=%d,%v; want 0,nil", got, err)
	}
}

func TestFileStableStore_GetUint64CorruptValue(t *testing.T) {
	dir := t.TempDir()
	store, err := newStableStore(filepath.Join(dir, "stable"))
	if err != nil {
		t.Fatal(err)
	}
	// Set with a non-8-byte payload so GetUint64 surfaces the corruption.
	if err := store.Set([]byte("term"), []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetUint64([]byte("term")); err == nil {
		t.Error("expected error for short value, got nil")
	}
}

func TestFileStableStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stable")

	store, err := newStableStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Set([]byte("a"), []byte("alpha"))
	_ = store.SetUint64([]byte("b"), 42)
	_ = store.(*fileStableStore).Close()

	reopened, err := newStableStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, _ := reopened.Get([]byte("a"))
	if string(got) != "alpha" {
		t.Errorf("Get a=%q, want alpha", got)
	}
	gn, _ := reopened.GetUint64([]byte("b"))
	if gn != 42 {
		t.Errorf("GetUint64 b=%d, want 42", gn)
	}
}

func TestFileStableStore_RejectsCorruptStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stable")
	if err := os.WriteFile(path, []byte("not valid json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := newStableStore(path)
	if err == nil {
		t.Fatal("expected error for corrupt stable store, got nil")
	}
}
