package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
)

type entry struct {
	version int64
	data    []byte
}

// MemoryStore is a thread-safe in-memory Store for testing.
type MemoryStore struct {
	mu      sync.RWMutex
	streams map[string][]entry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		streams: make(map[string][]entry),
	}
}

func (m *MemoryStore) Append(
	ctx context.Context,
	aggregateID string,
	version int64,
	data []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	entries := m.streams[aggregateID]
	idx, found := slices.BinarySearchFunc(
		entries,
		version,
		func(e entry, v int64) int {
			return cmp.Compare(e.version, v)
		},
	)

	if found {
		return fmt.Errorf("store: version conflict: (%s, %d) already exists", aggregateID, version)
	}

	m.streams[aggregateID] = slices.Insert(
		entries,
		idx,
		entry{
			version: version,
			data:    data,
		},
	)
	return nil
}

func (m *MemoryStore) ReadFrom(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
) (result [][]byte, err error) {
	if err = ctx.Err(); err != nil {
		return result, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if err = ctx.Err(); err != nil {
		return result, err
	}

	result = toBlobs(m.entriesFrom(aggregateID, fromVersion))
	return result, nil
}

func (m *MemoryStore) ReadRange(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	count int64,
) (result [][]byte, err error) {
	if err = ctx.Err(); err != nil {
		return result, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if err = ctx.Err(); err != nil {
		return result, err
	}

	entries := m.entriesFrom(aggregateID, fromVersion)
	if len(entries) > int(count) {
		entries = entries[:int(count)]
	}

	result = toBlobs(entries)
	return result, nil
}

func (m *MemoryStore) entriesFrom(
	aggregateID string,
	fromVersion int64,
) []entry {
	entries := m.streams[aggregateID]
	if len(entries) == 0 {
		return nil
	}

	startIdx, _ := slices.BinarySearchFunc(entries, fromVersion, func(e entry, v int64) int {
		return cmp.Compare(e.version, v)
	})

	return entries[startIdx:]
}

func toBlobs(entries []entry) [][]byte {
	result := make([][]byte, len(entries))
	for i, e := range entries {
		result[i] = e.data
	}
	return result
}
