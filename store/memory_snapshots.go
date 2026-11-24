package store

import (
	"context"
	"sync"
)

type memorySnapshots struct {
	mu    sync.RWMutex
	snaps map[string][]byte
	errOn map[string]error
}

// NewSnapshots returns an empty in-memory snapshot store.
func NewSnapshots() SnapshotMemory {
	return &memorySnapshots{
		snaps: make(map[string][]byte),
		errOn: make(map[string]error),
	}
}

// SetError schedules a one-shot error for the next Put on aggregateID.
func (s *memorySnapshots) SetError(
	aggregateID string,
	err error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[aggregateID] = err
}

// Put replaces the snapshot for aggregateID. The version is accepted to
// satisfy models.SnapshotStore but is not retained: nothing reads it back,
// and this store does not implement the optional monotonicity guard.
func (s *memorySnapshots) Put(
	ctx context.Context,
	aggregateID string,
	_ int64,
	data []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := s.errOn[aggregateID]; ok {
		delete(
			s.errOn,
			aggregateID,
		)
		return err
	}

	s.snaps[aggregateID] = data
	return nil
}

// Get returns the snapshot for aggregateID, if any.
func (s *memorySnapshots) Get(
	ctx context.Context,
	aggregateID string,
) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil,
			false,
			err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.snaps[aggregateID]
	if !ok {
		return nil,
			false,
			nil
	}

	return data,
		true,
		nil
}

// Delete removes the snapshot for aggregateID.
func (s *memorySnapshots) Delete(
	ctx context.Context,
	aggregateID string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(
		s.snaps,
		aggregateID,
	)
	return nil
}
