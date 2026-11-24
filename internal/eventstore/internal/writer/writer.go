package writer

import (
	"context"
	"encoding/json"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/ids"
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

// New constructs a Writer over the event store it appends to and the snapshot
// store it upserts into. schemaVersion is stamped on every event and snapshot it
// writes, so readers know which upcasts they still need.
func New[T any](
	es asynxmd.Store,
	ss asynxmd.SnapshotStore,
	schemaVersion int,
) *Writer[T] {
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
// The append runs in its own transaction and no snapshot is written; see
// WriteSnapshotted. The command executor does not use this method: it composes
// AppendTx and Snapshot itself so it can enlist projections and treat snapshot
// failures as non-fatal.
func (w *Writer[T]) Write(
	ctx context.Context,
	aggregateID string,
	eventName string,
	expectedVersion int64,
	previousState T,
	newState T,
) (evt asynxmd.Event[T], err error) {
	appendEvent := func(
		tx asynxmd.Tx,
	) (txErr error) {
		evt, txErr = w.AppendTx(
			ctx,
			tx,
			aggregateID,
			eventName,
			expectedVersion,
			previousState,
			newState,
		)
		return txErr
	}

	err = w.eventStore.Atomically(
		ctx,
		appendEvent,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	return evt,
		nil
}

// WriteSnapshotted is Write followed by a snapshot of the new state. If the
// snapshot write fails, the append is not rolled back (the event is durable)
// but the error is returned to the caller.
func (w *Writer[T]) WriteSnapshotted(
	ctx context.Context,
	aggregateID string,
	eventName string,
	expectedVersion int64,
	previousState T,
	newState T,
) (asynxmd.Event[T], error) {
	evt, err := w.Write(
		ctx,
		aggregateID,
		eventName,
		expectedVersion,
		previousState,
		newState,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	err = w.Snapshot(
		ctx,
		evt,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	return evt,
		nil
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
	patchJSON, err := diffJSON(
		previousState,
		newState,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	evt := w.newInternalEvent(
		eventName,
		expectedVersion+1,
		patchJSON,
	)
	err = appendInternal(
		ctx,
		tx,
		aggregateID,
		evt,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	return publicEvent(
			aggregateID,
			evt,
			previousState,
			newState,
		),
		nil
}

func (w *Writer[T]) newInternalEvent(
	eventName string,
	version int64,
	patchJSON []byte,
) esmodels.InternalEvent {
	return esmodels.InternalEvent{
		ID:            ids.New(),
		EventName:     eventName,
		Version:       version,
		SchemaVersion: w.currentSchemaVersion,
		OccurredAt:    time.Now().UTC(),
		Patches:       json.RawMessage(patchJSON),
	}
}

func publicEvent[T any](
	aggregateID string,
	evt esmodels.InternalEvent,
	previousState T,
	newState T,
) asynxmd.Event[T] {
	return asynxmd.Event[T]{
		ID:                evt.ID,
		AggregateID:       aggregateID,
		EventName:         evt.EventName,
		Version:           evt.Version,
		SchemaVersion:     evt.SchemaVersion,
		OccurredAt:        evt.OccurredAt,
		Aggregate:         newState,
		PreviousAggregate: previousState,
	}
}

func diffJSON[T any](
	previousState T,
	newState T,
) ([]byte, error) {
	patch, err := jsondiff.Compare(
		previousState,
		newState,
	)
	if err != nil {
		return nil,
			err
	}

	return json.Marshal(patch)
}

func appendInternal(
	ctx context.Context,
	tx asynxmd.Tx,
	aggregateID string,
	evt esmodels.InternalEvent,
) error {
	evtJSON, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	return tx.Append(
		ctx,
		"events:"+aggregateID,
		evt.Version,
		evtJSON,
	)
}

// Snapshot upserts the snapshot of evt.Aggregate at evt.Version in the snapshot
// store, replacing the aggregate's earlier snapshot and stamping it with the
// current schema version. It runs outside any event transaction and is safe to
// retry. A failure is returned unchanged and leaves events untouched.
func (w *Writer[T]) Snapshot(
	ctx context.Context,
	evt asynxmd.Event[T],
) error {
	return w.writeSnapshot(
		ctx,
		evt.AggregateID,
		evt.Version,
		evt.Aggregate,
	)
}

// EventStore returns the event store Writer appends to, so the caller can open
// its own transaction on it (see AppendTx). The same instance every call.
func (w *Writer[T]) EventStore() asynxmd.Store {
	return w.eventStore
}

// SnapshotStore returns the snapshot store Writer upserts into, which callers
// use to delete a snapshot. The same instance every call.
func (w *Writer[T]) SnapshotStore() asynxmd.SnapshotStore {
	return w.snapshotStore
}

func (w *Writer[T]) writeSnapshot(
	ctx context.Context,
	aggregateID string,
	version int64,
	state T,
) error {
	snap := esmodels.SnapshotBlob[T]{
		Version:       version,
		SchemaVersion: w.currentSchemaVersion,
		State:         state,
	}
	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	return w.snapshotStore.Put(
		ctx,
		aggregateID,
		version,
		snapJSON,
	)
}
