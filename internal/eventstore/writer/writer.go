package writer

import (
	"context"
	"encoding/json"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxutils "github.com/char2cs/asynx/internal/utils"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/wI2L/jsondiff"
)

// Writer computes RFC 6902 diffs between state transitions and durably appends
// them as InternalEvents to the event stream. It optionally writes a snapshot
// when the command requests one.
//
// Writer is stateless and safe for concurrent use.
type Writer[T any] struct {
	eventStore           asynxmd.Store
	snapshotStore        asynxmd.Store
	currentSchemaVersion int
}

// New constructs a Writer.
func New[T any](es, ss asynxmd.Store, schemaVersion int) *Writer[T] {
	return &Writer[T]{
		eventStore:           es,
		snapshotStore:        ss,
		currentSchemaVersion: schemaVersion,
	}
}

// Write appends the event at expectedVersion+1, where expectedVersion is the
// version of the state the command was validated against (0 for a brand-new
// aggregate). Appending at the validated version is what makes the store's
// (aggregateID, version) uniqueness act as optimistic-concurrency control: two
// writers that validated against the same state target the same version, so one
// wins and the other gets ErrPipelineFailed.
//
// It computes the RFC 6902 diff from previousState to newState, serializes it as
// an InternalEvent, and appends it to the event stream. A snapshot is written if
// shouldSnapshot is true.
//
// The event stream append is the save point: once it succeeds, the event is
// durable in the store. If snapshot write fails, the append is not rolled back
// (snapshot failures don't prevent durability of the event), but the error is
// still returned to the caller.
//
// Returns the public Event[T] (for bus publish) or an error.
func (w *Writer[T]) Write(
	ctx context.Context,
	aggregateID string,
	eventName string,
	expectedVersion int64,
	previousState T,
	newState T,
	shouldSnapshot bool,
) (asynxmd.Event[T], error) {
	version := expectedVersion + 1

	patch, err := jsondiff.Compare(previousState, newState)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	eventID := asynxutils.NewID()
	now := time.Now().UTC()

	evt := esmodels.InternalEvent{
		ID:            eventID,
		EventName:     eventName,
		Version:       version,
		SchemaVersion: w.currentSchemaVersion,
		OccurredAt:    now,
		Patches:       json.RawMessage(patchJSON),
	}

	evtJSON, err := json.Marshal(evt)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}
	if err := w.eventStore.Append(ctx, "events:"+aggregateID, version, evtJSON); err != nil {
		return asynxmd.Event[T]{}, err
	}

	if shouldSnapshot {
		if err := w.writeSnapshot(ctx, aggregateID, version, newState); err != nil {
			return asynxmd.Event[T]{}, err
		}
	}

	return asynxmd.Event[T]{
		ID:                eventID,
		AggregateID:       aggregateID,
		EventName:         eventName,
		Version:           version,
		SchemaVersion:     w.currentSchemaVersion,
		OccurredAt:        now,
		Aggregate:         newState,
		PreviousAggregate: previousState,
	}, nil
}

// EventStore returns the backing event store.
func (w *Writer[T]) EventStore() asynxmd.Store {
	return w.eventStore
}

// SnapshotStore returns the backing snapshot store.
func (w *Writer[T]) SnapshotStore() asynxmd.Store {
	return w.snapshotStore
}

func (w *Writer[T]) writeSnapshot(ctx context.Context, aggregateID string, version int64, state T) error {
	snap := esmodels.SnapshotBlob[T]{
		Version:       version,
		SchemaVersion: w.currentSchemaVersion,
		State:         state,
	}
	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return w.snapshotStore.Append(ctx, "snapshots:"+aggregateID, version, snapJSON)
}
