package store

import (
	"context"
	"sync"

	"github.com/char2cs/asynx/models"
)

// SnapshotMemory is a thread-safe in-memory snapshot store for tests and
// examples. It satisfies models.SnapshotStore and adds failure injection via
// SetError. Data is not persisted across restarts; production deployments
// should implement models.SnapshotStore over a durable backend.
type SnapshotMemory interface {
	models.SnapshotStore

	// SetError schedules a one-shot error for the next Put call on
	// aggregateID. The error is consumed and removed after the first
	// matching Put.
	SetError(
		aggregateID string,
		err error,
	)
}

type snapshotMemory struct {
	mu    sync.RWMutex
	snaps map[string][]byte
	errOn map[string]error
}

// NewSnapshots returns an empty in-memory snapshot store.
func NewSnapshots() SnapshotMemory {
	return &snapshotMemory{
		snaps: make(map[string][]byte),
		errOn: make(map[string]error),
	}
}

func (s *snapshotMemory) SetError(aggregateID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[aggregateID] = err
}

// Put replaces the snapshot for aggregateID. The version is accepted to
// satisfy models.SnapshotStore but is not retained: nothing reads it back,
// and this store does not implement the optional monotonicity guard.
func (s *snapshotMemory) Put(ctx context.Context, aggregateID string, _ int64, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := s.errOn[aggregateID]; ok {
		delete(s.errOn, aggregateID)
		return err
	}

	s.snaps[aggregateID] = data
	return nil
}

func (s *snapshotMemory) Get(ctx context.Context, aggregateID string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.snaps[aggregateID]
	if !ok {
		return nil, false, nil
	}

	return data, true, nil
}

func (s *snapshotMemory) Delete(ctx context.Context, aggregateID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.snaps, aggregateID)
	return nil
}
