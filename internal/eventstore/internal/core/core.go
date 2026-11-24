package core

import (
	"context"

	"github.com/char2cs/asynx/internal/eventstore/internal/reader"
	"github.com/char2cs/asynx/internal/eventstore/internal/replayer"
	"github.com/char2cs/asynx/internal/eventstore/internal/writer"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
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

	r := replayer.New[T](
		eventStore,
		upcasters,
		schemaVersion,
		zero,
	)
	w := writer.New[T](
		eventStore,
		snapshotStore,
		schemaVersion,
	)
	rd := reader.New[T](
		eventStore,
		snapshotStore,
		r,
		schemaVersion,
		zero,
		onCorruption,
	)

	return &EventStore[T]{
		reader:   rd,
		writer:   w,
		replayer: r,
	}
}

// GuardAutoSnapshots installs the guard under which a lock-free Get or Preload
// writes the auto-snapshot of an upcast state: it must run its function while
// no command or Forget runs for the aggregate, and return false without running
// it when that cannot be arranged. See models.SnapshotGuard.
func (es *EventStore[T]) GuardAutoSnapshots(
	guard esmodels.SnapshotGuard,
) {
	es.reader.SetSnapshotGuard(guard)
}

// Get returns the current aggregate state using the optimal path (warm or
// cold). Returns ErrNotFound if the aggregate has never been written.
func (es *EventStore[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	return es.reader.Get(
		ctx,
		aggregateID,
	)
}

// Exists returns true if the aggregate has at least one event in the store.
// It is faster than Get as it issues a minimal single-event ReadRange query.
func (es *EventStore[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	return es.reader.Exists(
		ctx,
		aggregateID,
	)
}

// Preload pays the cold-path cost offline by triggering a full Get.
// ErrNotFound is silently ignored — the aggregate may not exist yet.
func (es *EventStore[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	return es.reader.Preload(
		ctx,
		aggregateID,
	)
}

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
	evt, err := es.WriteTx(
		ctx,
		cmd,
		nil,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	if !cmd.ShouldSnapshot() {
		return evt,
			nil
	}

	err = es.Snapshot(
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

// WriteTx loads the aggregate state, validates the command, and in one
// transaction appends the resulting event and runs hook (when non-nil). It
// never writes a snapshot.
func (es *EventStore[T]) WriteTx(
	ctx context.Context,
	cmd asynxmd.Command[T],
	hook esmodels.TxHook[T],
) (asynxmd.Event[T], error) {
	currentPtr, loadedVersion, err := es.loadCurrent(
		ctx,
		cmd.AggregateID(),
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	err = cmd.Validate(currentPtr)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	newState := cmd.EmitEvent(currentPtr)
	evt, err := es.appendAtomically(
		ctx,
		cmd,
		hook,
		loadedVersion,
		valueOrZero(currentPtr),
		newState,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	return evt,
		nil
}

func (es *EventStore[T]) loadCurrent(
	ctx context.Context,
	aggregateID string,
) (current *T, version int64, err error) {
	state, version, err := es.reader.Load(
		ctx,
		aggregateID,
	)
	if err == asynxmd.ErrNotFound {
		return nil,
			version,
			nil
	}

	if err != nil {
		return nil,
			0,
			err
	}

	return &state,
		version,
		nil
}

func valueOrZero[T any](
	ptr *T,
) (value T) {
	if ptr == nil {
		return value
	}

	return *ptr
}

func (es *EventStore[T]) appendAtomically(
	ctx context.Context,
	cmd asynxmd.Command[T],
	hook esmodels.TxHook[T],
	loadedVersion int64,
	previousState T,
	newState T,
) (evt asynxmd.Event[T], err error) {
	appendInTx := func(
		tx asynxmd.Tx,
	) (txErr error) {
		evt, txErr = es.appendWithHook(
			ctx,
			tx,
			cmd,
			hook,
			loadedVersion,
			previousState,
			newState,
		)
		return txErr
	}

	err = es.writer.EventStore().Atomically(
		ctx,
		appendInTx,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	return evt,
		nil
}

func (es *EventStore[T]) appendWithHook(
	ctx context.Context,
	tx asynxmd.Tx,
	cmd asynxmd.Command[T],
	hook esmodels.TxHook[T],
	loadedVersion int64,
	previousState T,
	newState T,
) (asynxmd.Event[T], error) {
	evt, err := es.writer.AppendTx(
		ctx,
		tx,
		cmd.AggregateID(),
		cmd.EventName(),
		loadedVersion,
		previousState,
		newState,
	)
	if err != nil {
		return evt,
			err
	}

	if hook == nil {
		return evt,
			nil
	}

	return evt,
		hook(
			ctx,
			tx,
			evt,
		)
}

// Snapshot upserts the snapshot for evt.Aggregate at evt.Version.
func (es *EventStore[T]) Snapshot(
	ctx context.Context,
	evt asynxmd.Event[T],
) error {
	return es.writer.Snapshot(
		ctx,
		evt,
	)
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
	return es.writer.SnapshotStore().Delete(
		ctx,
		aggregateID,
	)
}

// Delete removes all events and snapshots for the aggregate from the backing stores.
// Idempotent — deleting a non-existent aggregate is not an error.
func (es *EventStore[T]) Delete(
	ctx context.Context,
	aggregateID string,
) error {
	deleteStream := func(
		tx asynxmd.Tx,
	) error {
		return tx.DeleteStream(
			ctx,
			"events:"+aggregateID,
		)
	}

	err := es.writer.EventStore().Atomically(
		ctx,
		deleteStream,
	)
	if err != nil {
		return err
	}

	return es.DeleteSnapshot(
		ctx,
		aggregateID,
	)
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
	return es.replayer.Replay(
		ctx,
		aggregateID,
		fromVersion,
		toVersion,
		fn,
	)
}
