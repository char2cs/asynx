// Package eventstore provides the persistence layer for Asynx aggregates.
//
// EventStore[T] coordinates three sub-modules:
//   - reader   — optimistic snapshot + delta warm path, full replay cold path
//   - writer   — RFC 6902 full-state diffs, conditional snapshots
//   - replayer — sequential patch application, schema upcasting, auto-snapshots
//
// All operations are stateless (no in-memory cache). Storage is the single
// source of truth; operators may layer caching (Redis, Memcached) on top.
//
// Events and snapshots use different contracts: models.Store is an append-only
// versioned stream, models.SnapshotStore is a single upserted cell per
// aggregate holding only the newest snapshot.
package eventstore

import (
	"context"

	"github.com/char2cs/asynx/internal/eventstore/reader"
	"github.com/char2cs/asynx/internal/eventstore/replayer"
	"github.com/char2cs/asynx/internal/eventstore/writer"
	asynxmd "github.com/char2cs/asynx/models"
)

// EventStore[T] is the public API for durable aggregate persistence.
// Construct it with New; all fields are set up by the constructor.
type EventStore[T any] struct {
	reader   *reader.Reader[T]
	writer   *writer.Writer[T]
	replayer *replayer.Replayer[T]
}

// New builds a fully-configured EventStore. eventStore and snapshotStore use
// different contracts (models.Store vs models.SnapshotStore) and cannot be
// the same instance. upcasters maps SchemaVersion → migration func;
// schemaVersion is the current (target) schema version for all new events.
// onCorruption is called when a snapshot cannot be deserialized; pass nil to
// silently fall back to the cold replay path.
func New[T any](
	eventStore asynxmd.Store,
	snapshotStore asynxmd.SnapshotStore,
	upcasters map[int]asynxmd.Upcaster,
	schemaVersion int,
	onCorruption func(error),
) *EventStore[T] {
	var zero T

	r := replayer.New[T](eventStore, upcasters, schemaVersion, zero)
	w := writer.New[T](eventStore, snapshotStore, schemaVersion)
	rd := reader.New[T](eventStore, snapshotStore, r, schemaVersion, zero, onCorruption)

	return &EventStore[T]{
		reader:   rd,
		writer:   w,
		replayer: r,
	}
}

// Get returns the current aggregate state using the optimal path (warm or
// cold). Returns ErrNotFound if the aggregate has never been written.
func (es *EventStore[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	return es.reader.Get(ctx, aggregateID)
}

// Exists returns true if the aggregate has at least one event in the store.
// It is faster than Get as it issues a minimal single-event ReadRange query.
func (es *EventStore[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	return es.reader.Exists(ctx, aggregateID)
}

// Preload pays the cold-path cost offline by triggering a full Get.
// ErrNotFound is silently ignored — the aggregate may not exist yet.
func (es *EventStore[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	return es.reader.Preload(ctx, aggregateID)
}

// TxHook runs inside the append's transaction, after the event is appended.
// Returning an error rolls the transaction back, discarding the event.
type TxHook[T any] func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error

// Write loads the aggregate state, validates the command, computes the new state
// via cmd.EmitEvent, and durably appends the event. It writes a snapshot if
// cmd.ShouldSnapshot() returns true and returns the snapshot error if that
// fails; the command executor uses WriteTx and Snapshot instead.
//
// Returns ErrValidation if the command fails validation against the current state.
//
// If a concurrent writer already appended at the same version, the append
// fails with ErrPipelineFailed semantics — the caller should re-read state
// and retry.
func (es *EventStore[T]) Write(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	evt, err := es.WriteTx(ctx, cmd, nil)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	if cmd.ShouldSnapshot() {
		if err := es.Snapshot(ctx, evt); err != nil {
			return asynxmd.Event[T]{}, err
		}
	}

	return evt, nil
}

// WriteTx loads the aggregate state, validates the command, and in one
// transaction appends the resulting event and runs hook (when non-nil). It
// never writes a snapshot.
func (es *EventStore[T]) WriteTx(
	ctx context.Context,
	cmd asynxmd.Command[T],
	hook TxHook[T],
) (asynxmd.Event[T], error) {
	// Load state and its version together so the command is validated and the
	// event is appended against the same version. ErrNotFound yields version 0,
	// so a first write appends at version 1.
	current, loadedVersion, err := es.reader.Load(ctx, cmd.AggregateID())
	if err != nil && err != asynxmd.ErrNotFound {
		return asynxmd.Event[T]{}, err
	}

	var currentPtr *T
	if err != asynxmd.ErrNotFound {
		currentPtr = &current
	}

	if err := cmd.Validate(currentPtr); err != nil {
		return asynxmd.Event[T]{}, err
	}

	newState := cmd.EmitEvent(currentPtr)

	var previousState T
	if currentPtr != nil {
		previousState = *currentPtr
	}

	var evt asynxmd.Event[T]
	err = es.writer.EventStore().Atomically(ctx, func(tx asynxmd.Tx) error {
		var err error
		evt, err = es.writer.AppendTx(
			ctx,
			tx,
			cmd.AggregateID(),
			cmd.EventName(),
			loadedVersion,
			previousState,
			newState,
		)
		if err != nil {
			return err
		}
		if hook != nil {
			return hook(ctx, tx, evt)
		}
		return nil
	})
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	return evt, nil
}

// Snapshot upserts the snapshot for evt.Aggregate at evt.Version.
func (es *EventStore[T]) Snapshot(
	ctx context.Context,
	evt asynxmd.Event[T],
) error {
	return es.writer.Snapshot(ctx, evt)
}

// Store returns the backing event store.
func (es *EventStore[T]) Store() asynxmd.Store {
	return es.writer.EventStore()
}

// DeleteSnapshot removes the aggregate's snapshot. Idempotent.
func (es *EventStore[T]) DeleteSnapshot(
	ctx context.Context,
	aggregateID string,
) error {
	return es.writer.SnapshotStore().Delete(ctx, aggregateID)
}

// Delete removes all events and snapshots for the aggregate from the backing stores.
// Idempotent — deleting a non-existent aggregate is not an error.
func (es *EventStore[T]) Delete(
	ctx context.Context,
	aggregateID string,
) error {
	err := es.writer.EventStore().Atomically(ctx, func(tx asynxmd.Tx) error {
		return tx.DeleteStream(ctx, "events:"+aggregateID)
	})
	if err != nil {
		return err
	}
	return es.DeleteSnapshot(ctx, aggregateID)
}

// Replay iterates events in version order, upcasting each to the current
// schema version, and calls fn with the public Event[T] containing both the
// new and previous aggregate states. Replay is read-only and never writes
// auto-snapshots.
//
// fromVersion=1 starts from the beginning. toVersion=0 reads to the end.
func (es *EventStore[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn asynxmd.ProjectionHandler[T],
) error {
	return es.replayer.Replay(ctx, aggregateID, fromVersion, toVersion, fn)
}
