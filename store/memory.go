// Package store provides an in-memory implementation of models.Store,
// intended for tests and examples. Data is not persisted across restarts;
// production deployments should implement models.Store over a durable backend.
package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/char2cs/asynx/models"
)

// Memory is a thread-safe in-memory event store for tests and examples.
// It satisfies models.Store and adds failure injection via SetError.
type Memory interface {
	models.Store

	// SetError schedules a one-shot error for the next Append call on
	// aggregateID. The error is consumed and removed after the first
	// matching Append.
	SetError(
		aggregateID string,
		err error,
	)
}

type memory struct {
	mu      sync.RWMutex
	streams map[string][]entry
	errOn   map[string]error
}

type entry struct {
	version int64
	data    []byte
}

// New returns an empty in-memory store.
func New() Memory {
	return &memory{
		streams: make(map[string][]entry),
		errOn:   make(map[string]error),
	}
}

func (s *memory) SetError(aggregateID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[aggregateID] = err
}

func (s *memory) Append(ctx context.Context, aggregateID string, version int64, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := s.errOn[aggregateID]; ok {
		delete(s.errOn, aggregateID)
		return err
	}

	entries := s.streams[aggregateID]
	idx, found := slices.BinarySearchFunc(entries, version, func(e entry, v int64) int {
		return cmp.Compare(e.version, v)
	})
	if found {
		return fmt.Errorf("%w: version conflict (%s, v%d)", models.ErrPipelineFailed, aggregateID, version)
	}

	s.streams[aggregateID] = slices.Insert(entries, idx, entry{version: version, data: data})
	return nil
}

func (s *memory) ReadFrom(ctx context.Context, aggregateID string, fromVersion int64) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return toBlobs(s.entriesFrom(aggregateID, fromVersion)), nil
}

func (s *memory) ReadRange(ctx context.Context, aggregateID string, fromVersion, count int64) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.entriesFrom(aggregateID, fromVersion)
	if int64(len(entries)) > count {
		entries = entries[:count]
	}

	return toBlobs(entries), nil
}

func (s *memory) entriesFrom(aggregateID string, fromVersion int64) []entry {
	entries := s.streams[aggregateID]
	if len(entries) == 0 {
		return nil
	}
	startIdx, _ := slices.BinarySearchFunc(entries, fromVersion, func(e entry, v int64) int {
		return cmp.Compare(e.version, v)
	})
	return entries[startIdx:]
}

func (s *memory) Count(ctx context.Context, aggregateID string, fromVersion int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.entriesFrom(aggregateID, fromVersion))), nil
}

func (s *memory) Delete(ctx context.Context, aggregateID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.streams, aggregateID)
	return nil
}

func toBlobs(entries []entry) [][]byte {
	result := make([][]byte, len(entries))
	for i, e := range entries {
		result[i] = e.data
	}
	return result
}
