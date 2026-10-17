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
	snapshotStore        asynxmd.SnapshotStore
	currentSchemaVersion int
}

// New constructs a Writer.
func New[T any](es asynxmd.Store, ss asynxmd.SnapshotStore, schemaVersion int) *Writer[T] {
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
// The append runs in its own transaction; a snapshot is written afterwards if
// shouldSnapshot is true. If the snapshot write fails, the append is not rolled
// back (the event is durable) but the error is returned to the caller. The
// command executor does not use this method: it composes AppendTx and Snapshot
// itself so it can enlist projections and treat snapshot failures as non-fatal.
func (w *Writer[T]) Write(
	ctx context.Context,
	aggregateID string,
	eventName string,
	expectedVersion int64,
	previousState T,
	newState T,
	shouldSnapshot bool,
) (asynxmd.Event[T], error) {
	var evt asynxmd.Event[T]
	err := w.eventStore.Atomically(ctx, func(tx asynxmd.Tx) error {
		var err error
		evt, err = w.AppendTx(ctx, tx, aggregateID, eventName, expectedVersion, previousState, newState)
		return err
	})
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	if shouldSnapshot {
		if err := w.Snapshot(ctx, evt); err != nil {
			return asynxmd.Event[T]{}, err
		}
	}

	return evt, nil
}

// AppendTx computes the RFC 6902 diff from previousState to newState,
// serializes it as an InternalEvent and appends it at expectedVersion+1 inside
// tx. It returns the public Event[T].
func (w *Writer[T]) AppendTx(
	ctx context.Context,
	tx asynxmd.Tx,
	aggregateID string,
	eventName string,
	expectedVersion int64,
	previousState T,
	newState T,
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
	if err := tx.Append(ctx, "events:"+aggregateID, version, evtJSON); err != nil {
		return asynxmd.Event[T]{}, err
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

// Snapshot upserts the snapshot of evt.Aggregate at evt.Version.
func (w *Writer[T]) Snapshot(
	ctx context.Context,
	evt asynxmd.Event[T],
) error {
	return w.writeSnapshot(ctx, evt.AggregateID, evt.Version, evt.Aggregate)
}

// EventStore returns the backing event store.
func (w *Writer[T]) EventStore() asynxmd.Store {
	return w.eventStore
}

// SnapshotStore returns the backing snapshot store.
func (w *Writer[T]) SnapshotStore() asynxmd.SnapshotStore {
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
	return w.snapshotStore.Put(ctx, aggregateID, version, snapJSON)
}
