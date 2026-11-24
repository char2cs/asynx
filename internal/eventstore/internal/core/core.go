package core

import (
	"context"

	"github.com/char2cs/asynx/v2/internal/eventstore/internal/reader"
	"github.com/char2cs/asynx/v2/internal/eventstore/internal/replayer"
	"github.com/char2cs/asynx/v2/internal/eventstore/internal/writer"
	esmodels "github.com/char2cs/asynx/v2/internal/eventstore/models"
	"github.com/char2cs/asynx/v2/models/command"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/handler"
	"github.com/char2cs/asynx/v2/models/storage"
)

// EventStore is the implementation behind the eventstore.EventStore interface:
// it wires a Reader, a Writer and a Replayer over one event store and one
// snapshot store and exposes them as the read, write and maintenance
// operations. It adds the one thing none of the three can do alone, the write
// path that loads state, validates a command and appends the resulting event in
// a single transaction. It holds no mutable state of its own beyond what New
// sets up (the snapshot guard is installed once at wiring time), and is safe
// for concurrent use. Construct it with New.
type EventStore[T any] struct {
	reader   *reader.Reader[T]
	writer   *writer.Writer[T]
	replayer *replayer.Replayer[T]
}

// New builds a fully-configured EventStore. eventStore and snapshotStore use
// different contracts (models.Store vs models.SnapshotStore) and cannot be
// the same instance. upcasters maps SchemaVersion to the migration that lifts
// an event one version up and is cloned, so the caller may keep mutating its
// map; schemaVersion is the current (target) schema version for all new events.
// onCorruption is called when a snapshot cannot be deserialized; pass nil to
// silently fall back to the cold replay path.
func New[T any](
	eventStore storage.Store,
	snapshotStore storage.SnapshotStore,
	upcasters map[int]event.Upcaster,
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
// it when that cannot be arranged, which skips the snapshot. Without it a
// concurrent Forget could be undone by a late snapshot. Call it once, while
// wiring, before the store is used from several goroutines. See
// models.SnapshotGuard.
func (es *EventStore[T]) GuardAutoSnapshots(
	guard esmodels.SnapshotGuard,
) {
	es.reader.SetSnapshotGuard(guard)
}

// ObserveAutoSnapshotErrors installs the callback that observes a failed
// auto-snapshot write made by the read path (Get, Load and Preload cache a
// snapshot after an upcast). The failed write never fails the read; the
// callback is called synchronously on the reading goroutine with an Event
// carrying the aggregate id (AggregateID), the version the snapshot was for
// (Version), the id of that version's event (ID), the schema version
// (SchemaVersion) and the state that failed to be cached (Aggregate); the
// other fields are zero. A panic in the callback is contained and written to
// stderr as one line. A nil callback, the
// default, drops the failure. Call it once, while wiring, before the store
// is used from several goroutines.
func (es *EventStore[T]) ObserveAutoSnapshotErrors(
	fn func(context.Context, event.Event[T], error),
) {
	es.reader.SetSnapshotErrorHandler(fn)
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
// It is faster than Get as it issues a minimal single-event ReadRange query and
// never looks at snapshots. Storage errors are returned with false.
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
	cmd command.Command[T],
) (event.Event[T], error) {
	evt, err := es.WriteTx(
		ctx,
		cmd,
		nil,
	)
	if err != nil {
		return event.Event[T]{},
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
		return event.Event[T]{},
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
	cmd command.Command[T],
	hook esmodels.TxHook[T],
) (event.Event[T], error) {
	currentPtr, loadedVersion, err := es.loadCurrent(
		ctx,
		cmd.AggregateID(),
	)
	if err != nil {
		return event.Event[T]{},
			err
	}

	err = cmd.Validate(currentPtr)
	if err != nil {
		return event.Event[T]{},
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
		return event.Event[T]{},
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
	if err == failure.ErrNotFound {
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
	cmd command.Command[T],
	hook esmodels.TxHook[T],
	loadedVersion int64,
	previousState T,
	newState T,
) (event.Event[T], error) {
	pending := &pendingAppend[T]{
		store:         es,
		cmd:           cmd,
		hook:          hook,
		loadedVersion: loadedVersion,
		previousState: previousState,
		newState:      newState,
	}
	err := es.writer.EventStore().Atomically(
		ctx,
		pending.inTransaction(ctx),
	)
	if err != nil {
		return event.Event[T]{},
			err
	}

	return pending.evt,
		nil
}

func (es *EventStore[T]) appendWithHook(
	ctx context.Context,
	tx storage.Tx,
	cmd command.Command[T],
	hook esmodels.TxHook[T],
	loadedVersion int64,
	previousState T,
	newState T,
) (event.Event[T], error) {
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

// Snapshot upserts the snapshot of evt.Aggregate at evt.Version, replacing the
// aggregate's previous snapshot. It is called after the event's transaction
// committed, so a failure leaves the event durable and only costs a slower next
// read; the caller decides whether to report it (the command executor does,
// through the snapshot error handler, Write returns it).
func (es *EventStore[T]) Snapshot(
	ctx context.Context,
	evt event.Event[T],
) error {
	return es.writer.Snapshot(
		ctx,
		evt,
	)
}

// Store returns the backing event store, the same instance appends run on. The
// command executor uses it to enlist its own writes in the transaction of an
// append and to read and acknowledge deliveries.
func (es *EventStore[T]) Store() storage.Store {
	return es.writer.EventStore()
}

// DeleteSnapshot removes the aggregate's snapshot and leaves its events alone,
// so the next read replays from event 1. Idempotent: a missing snapshot is not
// an error. Forget calls it before erasing the stream so that a half-failed
// erase can never leave a stale snapshot behind.
func (es *EventStore[T]) DeleteSnapshot(
	ctx context.Context,
	aggregateID string,
) error {
	return es.writer.SnapshotStore().Delete(
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
	fn handler.Projection[T],
) error {
	return es.replayer.Replay(
		ctx,
		aggregateID,
		fromVersion,
		toVersion,
		fn,
	)
}
