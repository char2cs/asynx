// Package store provides in-memory implementations of models.Store for use in tests.
package store

import (
	"context"
	"sync"

	"github.com/char2cs/asynx/models"
)

// Memory is a thread-safe in-memory implementation of models.Store.
// Only for testing.
type Memory struct {
	mu    sync.RWMutex
	blobs map[string][]blob
	errOn map[string]error
}

type blob struct {
	version int64
	data    []byte
}

// New returns an empty Memory store.
func New() *Memory {
	return &Memory{
		blobs: make(map[string][]blob),
		errOn: make(map[string]error),
	}
}

// SetError schedules a one-shot error for the next Append call on aggregateID.
// The error is consumed and removed after the first matching Append.
func (s *Memory) SetError(aggregateID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[aggregateID] = err
}

func (s *Memory) Append(_ context.Context, aggregateID string, version int64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.errOn[aggregateID]; ok {
		delete(s.errOn, aggregateID)
		return err
	}
	for _, b := range s.blobs[aggregateID] {
		if b.version == version {
			return models.ErrPipelineFailed
		}
	}
	s.blobs[aggregateID] = append(s.blobs[aggregateID], blob{version: version, data: data})
	return nil
}

func (s *Memory) ReadFrom(_ context.Context, aggregateID string, fromVersion int64) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out [][]byte
	for _, b := range s.blobs[aggregateID] {
		if b.version >= fromVersion {
			out = append(out, b.data)
		}
	}
	return out, nil
}

func (s *Memory) ReadRange(_ context.Context, aggregateID string, fromVersion, count int64) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out [][]byte
	for _, b := range s.blobs[aggregateID] {
		if b.version >= fromVersion {
			out = append(out, b.data)
			if int64(len(out)) >= count {
				break
			}
		}
	}
	return out, nil
}
