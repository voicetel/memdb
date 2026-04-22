//go:build !purego

package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	hraft "github.com/hashicorp/raft"
)

// ── Log Store ─────────────────────────────────────────────────────────────────
//
// fileLogStore implements hraft.LogStore using a simple append-only flat file.
// Each record is:
//
//	[8 bytes big-endian uint64: record length][record length bytes: JSON-encoded hraft.Log]
//
// On open, all records are scanned into an in-memory index (map[uint64]int64)
// that maps log index → file offset. Appends go to the end of the file.
// DeleteRange truncates or rewrites as needed. The design prioritises
// correctness and crash-safety (every Append calls fsync) over throughput.

type fileLogStore struct {
	mu      sync.RWMutex
	f       *os.File
	path    string
	index   map[uint64]int64 // log index → byte offset of the length prefix
	first   uint64
	last    uint64
	hasData bool
}

func newLogStore(path string) (hraft.LogStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("log store: open %s: %w", path, err)
	}
	s := &fileLogStore{
		f:     f,
		path:  path,
		index: make(map[uint64]int64),
	}
	if err := s.load(); err != nil {
		f.Close()
		return nil, fmt.Errorf("log store: load: %w", err)
	}
	return s, nil
}

// load scans the entire file and rebuilds the in-memory index.
// All reads use ReadAt so no seek state is mutated.
func (s *fileLogStore) load() error {
	var offset int64
	for {
		// Read the 8-byte length prefix at the current offset.
		var lenBuf [8]byte
		n, err := s.f.ReadAt(lenBuf[:], offset)
		if n == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
			break
		}
		if err != nil && err != io.EOF {
			if n < 8 {
				break // truncated length prefix — stop
			}
			return err
		}
		if n < 8 {
			break
		}
		length := binary.BigEndian.Uint64(lenBuf[:])
		if length == 0 || length > 64*1024*1024 {
			break // corrupt or zero-length record
		}

		data := make([]byte, length)
		n2, err2 := s.f.ReadAt(data, offset+8)
		if uint64(n2) < length || (err2 != nil && err2 != io.EOF) {
			break // truncated body — stop
		}

		var log hraft.Log
		if err := json.Unmarshal(data, &log); err != nil {
			break // corrupt record — stop
		}

		s.index[log.Index] = offset
		if !s.hasData || log.Index < s.first {
			s.first = log.Index
		}
		if !s.hasData || log.Index > s.last {
			s.last = log.Index
		}
		s.hasData = true
		offset += 8 + int64(length)
	}
	return nil
}

func (s *fileLogStore) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.hasData {
		return 0, nil
	}
	return s.first, nil
}

func (s *fileLogStore) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.hasData {
		return 0, nil
	}
	return s.last, nil
}

func (s *fileLogStore) GetLog(idx uint64, out *hraft.Log) error {
	s.mu.RLock()
	offset, ok := s.index[idx]
	s.mu.RUnlock()
	if !ok {
		return hraft.ErrLogNotFound
	}

	// Read the length prefix using ReadAt — no seek, safe for concurrent use.
	var lenBuf [8]byte
	if _, err := s.f.ReadAt(lenBuf[:], offset); err != nil {
		return fmt.Errorf("log store: read length at %d: %w", offset, err)
	}
	length := binary.BigEndian.Uint64(lenBuf[:])
	if length == 0 || length > 64*1024*1024 {
		return fmt.Errorf("log store: invalid record length %d at offset %d", length, offset)
	}

	data := make([]byte, length)
	if _, err := s.f.ReadAt(data, offset+8); err != nil {
		return fmt.Errorf("log store: read body at %d: %w", offset+8, err)
	}
	return json.Unmarshal(data, out)
}

func (s *fileLogStore) StoreLog(log *hraft.Log) error {
	return s.StoreLogs([]*hraft.Log{log})
}

func (s *fileLogStore) StoreLogs(logs []*hraft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Seek to end once to get the current append position.
	appendOffset, err := s.f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("log store: seek end: %w", err)
	}

	for _, log := range logs {
		data, err := json.Marshal(log)
		if err != nil {
			return fmt.Errorf("log store: marshal: %w", err)
		}
		length := uint64(len(data))

		// Build the record in a single buffer: [8-byte length][data].
		record := make([]byte, 8+len(data))
		binary.BigEndian.PutUint64(record[:8], length)
		copy(record[8:], data)

		// WriteAt is positionally explicit — no implicit seek state used.
		if _, err := s.f.WriteAt(record, appendOffset); err != nil {
			return fmt.Errorf("log store: write at %d: %w", appendOffset, err)
		}

		s.index[log.Index] = appendOffset
		appendOffset += int64(len(record))

		if !s.hasData || log.Index < s.first {
			s.first = log.Index
		}
		if !s.hasData || log.Index > s.last {
			s.last = log.Index
		}
		s.hasData = true
	}
	// fsync once after all records are written.
	return s.f.Sync()
}

// DeleteRange removes log entries with indices in [min, max].
// We rewrite the file keeping only entries outside the deleted range,
// using ReadAt so there is no seek-state contention.
func (s *fileLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect surviving entries in index order.
	type entry struct {
		idx  uint64
		data []byte
	}
	var keep []entry

	for idx, offset := range s.index {
		if idx >= min && idx <= max {
			continue
		}
		// Read length prefix via ReadAt.
		var lenBuf [8]byte
		if _, err := s.f.ReadAt(lenBuf[:], offset); err != nil {
			return fmt.Errorf("log store: delete range read length: %w", err)
		}
		length := binary.BigEndian.Uint64(lenBuf[:])
		if length == 0 || length > 64*1024*1024 {
			return fmt.Errorf("log store: delete range invalid length %d", length)
		}
		data := make([]byte, length)
		if _, err := s.f.ReadAt(data, offset+8); err != nil {
			return fmt.Errorf("log store: delete range read body: %w", err)
		}
		keep = append(keep, entry{idx: idx, data: data})
	}

	// Sort survivors by log index so the file is written in order.
	sort.Slice(keep, func(i, j int) bool { return keep[i].idx < keep[j].idx })

	// Rewrite the file atomically via a temp file + rename.
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".logstore-tmp-*")
	if err != nil {
		return fmt.Errorf("log store: delete range temp: %w", err)
	}
	tmpName := tmp.Name()

	newIndex := make(map[uint64]int64, len(keep))
	var newFirst, newLast uint64
	hasData := false
	var writeOffset int64

	for _, e := range keep {
		length := uint64(len(e.data))
		record := make([]byte, 8+len(e.data))
		binary.BigEndian.PutUint64(record[:8], length)
		copy(record[8:], e.data)

		if _, err := tmp.WriteAt(record, writeOffset); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return fmt.Errorf("log store: delete range write: %w", err)
		}
		newIndex[e.idx] = writeOffset
		writeOffset += int64(len(record))

		if !hasData || e.idx < newFirst {
			newFirst = e.idx
		}
		if !hasData || e.idx > newLast {
			newLast = e.idx
		}
		hasData = true
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	tmp.Close()

	if err := s.f.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return err
	}

	f, err := os.OpenFile(s.path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	s.f = f
	s.index = newIndex
	s.first = newFirst
	s.last = newLast
	s.hasData = hasData
	return nil
}

// ── Stable Store ──────────────────────────────────────────────────────────────
//
// fileStableStore implements hraft.StableStore using a JSON file that is
// rewritten atomically on every Set/SetUint64. Raft only calls Set/SetUint64
// for a small number of keys (CurrentTerm, LastVoteTerm, LastVoteCand) so
// the full-rewrite approach is safe and simple.

type fileStableStore struct {
	mu   sync.Mutex
	path string
	kv   map[string][]byte
}

func newStableStore(path string) (hraft.StableStore, error) {
	s := &fileStableStore{
		path: path,
		kv:   make(map[string][]byte),
	}
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("stable store: read %s: %w", path, err)
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &s.kv); err != nil {
			// Corrupt file — start fresh. Raft will re-issue an election.
			s.kv = make(map[string][]byte)
		}
	}
	return s, nil
}

func (s *fileStableStore) save() error {
	data, err := json.Marshal(s.kv)
	if err != nil {
		return err
	}
	// Atomic write: temp file in same directory + rename.
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".stable-tmp-*")
	if err != nil {
		// Fallback: write directly if temp dir unavailable.
		return os.WriteFile(s.path, data, 0o600)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	tmp.Close()
	return os.Rename(tmpName, s.path)
}

func (s *fileStableStore) Set(key []byte, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[string(key)] = val
	return s.save()
}

func (s *fileStableStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kv[string(key)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return v, nil
}

func (s *fileStableStore) SetUint64(key []byte, val uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	s.kv[string(key)] = buf
	return s.save()
}

func (s *fileStableStore) GetUint64(key []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kv[string(key)]
	if !ok {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("stable store: corrupt value for key %q", key)
	}
	return binary.BigEndian.Uint64(v), nil
}
