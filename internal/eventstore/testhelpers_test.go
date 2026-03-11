package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- Test types ---

type order struct {
	ID     string
	Status string
	Total  float64
}

// --- In-memory store for testing ---

// memStore is a simple in-memory Store that enforces (aggregateID, version)
// uniqueness, matching the contract of the real Store interface.
type memStore struct {
	mu     sync.RWMutex
	events map[string][]storedEvent // aggregateID → ordered events
	errOn  map[string]error         // aggregateID → error to return on next write
}

type storedEvent struct {
	version int64
	data    []byte
}

func newMemStore() *memStore {
	return &memStore{
		events: make(map[string][]storedEvent),
		errOn:  make(map[string]error),
	}
}

// setError makes the next call to Append for aggregateID return err.
func (s *memStore) setError(aggregateID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[aggregateID] = err
}

func (s *memStore) Append(
	ctx context.Context,
	aggregateID string,
	version int64,
	data []byte,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := s.errOn[aggregateID]; ok {
		delete(s.errOn, aggregateID)
		return err
	}

	for _, e := range s.events[aggregateID] {
		if e.version == version {
			return asynxmd.ErrPipelineFailed
		}
	}

	s.events[aggregateID] = append(s.events[aggregateID], storedEvent{
		version: version,
		data:    data,
	})

	return nil
}

func (s *memStore) ReadFrom(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out [][]byte
	for _, e := range s.events[aggregateID] {
		if e.version >= fromVersion {
			out = append(out, e.data)
		}
	}

	return out, nil
}

func (s *memStore) ReadRange(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	count int64,
) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out [][]byte
	for _, e := range s.events[aggregateID] {
		if e.version >= fromVersion {
			out = append(out, e.data)
			if int64(len(out)) >= count {
				break
			}
		}
	}

	return out, nil
}

// --- errStore always returns an error ---

type errStore struct{ err error }

func (e *errStore) Append(_ context.Context, _ string, _ int64, _ []byte) error {
	return e.err
}

func (e *errStore) ReadFrom(_ context.Context, _ string, _ int64) ([][]byte, error) {
	return nil, e.err
}

func (e *errStore) ReadRange(_ context.Context, _ string, _ int64, _ int64) ([][]byte, error) {
	return nil, e.err
}

// --- Helpers ---

var storageErr = errors.New("storage failure")

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("mustMarshal: %v", err)
	}
	return b
}

// makeInternalEventBlob creates a serialized internalEvent blob for seeding
// a memStore in tests.
func makeInternalEventBlob(t *testing.T, id string, eventName string, version int64, schemaVersion int, patches json.RawMessage) []byte {
	t.Helper()
	evt := internalEvent{
		ID:            id,
		EventName:     eventName,
		Version:       version,
		SchemaVersion: schemaVersion,
		OccurredAt:    time.Now().UTC(),
		Patches:       patches,
	}
	return mustMarshal(t, evt)
}

// makeSnapshotBlob creates a serialized snapshotBlob for seeding the snapshot store.
func makeSnapshotBlob(t *testing.T, version int64, schemaVersion int, state any) []byte {
	t.Helper()
	stateBytes := mustMarshal(t, state)
	snap := snapshotBlob{
		Version:       version,
		SchemaVersion: schemaVersion,
		State:         stateBytes,
	}
	return mustMarshal(t, snap)
}

// newTestReplayer builds a Replayer with the given stores and options.
func newTestReplayer(eventStore, snapshotStore asynxmd.Store, upcasters map[int]asynxmd.Upcaster, schemaVersion int) *Replayer[order] {
	if upcasters == nil {
		upcasters = make(map[int]asynxmd.Upcaster)
	}
	return &Replayer[order]{
		eventStore:           eventStore,
		snapshotStore:        snapshotStore,
		upcasters:            upcasters,
		currentSchemaVersion: schemaVersion,
	}
}

// newTestReader builds a Reader backed by the provided stores and replayer.
func newTestReader(eventStore, snapshotStore asynxmd.Store, replayer *Replayer[order]) *Reader[order] {
	return &Reader[order]{
		eventStore:    eventStore,
		snapshotStore: snapshotStore,
		replayer:      replayer,
	}
}

// newTestWriter builds a Writer backed by the provided stores.
func newTestWriter(eventStore, snapshotStore asynxmd.Store, schemaVersion int) *Writer[order] {
	return &Writer[order]{
		eventStore:           eventStore,
		snapshotStore:        snapshotStore,
		currentSchemaVersion: schemaVersion,
	}
}
