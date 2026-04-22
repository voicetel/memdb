//go:build purego

package raft

import hraft "github.com/hashicorp/raft"

// newLogStore returns an in-memory LogStore. In purego builds this is the
// only available implementation (no file-backed alternative).
func newLogStore(_ string) (hraft.LogStore, error) {
	return hraft.NewInmemStore(), nil
}

// newStableStore returns an in-memory StableStore. In purego builds this is
// the only available implementation (no file-backed alternative).
func newStableStore(_ string) (hraft.StableStore, error) {
	return hraft.NewInmemStore(), nil
}
