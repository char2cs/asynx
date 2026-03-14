// Package eventstore provides the persistence layer for Asynx aggregates.
//
// EventStore[T] coordinates three sub-modules:
//   - reader   — optimistic snapshot + delta warm path, full replay cold path
//   - writer   — RFC 6902 full-state diffs, conditional snapshots
//   - replayer — sequential patch application, schema upcasting, auto-snapshots
//
// All operations are stateless (no in-memory cache). Storage is the single
// source of truth; operators may layer caching (Redis, Memcached) on top.
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

// New builds a fully-configured EventStore. eventStore and snapshotStore may
// be the same Store instance. upcasters maps SchemaVersion → migration func;
// schemaVersion is the current (target) schema version for all new events.
// onCorruption is called when a snapshot cannot be deserialized; pass nil to
// silently fall back to the cold replay path.
func New[T any](
	eventStore asynxmd.Store,
	snapshotStore asynxmd.Store,
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

// Write validates the command, computes the new state via cmd.EmitEvent,
// and durably appends the event. It writes a snapshot if cmd.ShouldSnapshot()
// returns true.
//
// If cmd.Validate returns an error, Write returns it immediately without
// touching storage.
//
// If a concurrent writer already appended at the same version, Store.Append
// returns an error which Write propagates to the caller (ErrPipelineFailed
// semantics — the caller should re-read state and retry).
func (es *EventStore[T]) Write(
	ctx context.Context,
	current *T,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	if err := cmd.Validate(current); err != nil {
		return asynxmd.Event[T]{}, err
	}

	newState := cmd.EmitEvent(current)

	var previousState T
	if current != nil {
		previousState = *current
	}

	return es.writer.Write(
		ctx,
		cmd.AggregateID(),
		cmd.EventName(),
		previousState,
		newState,
		cmd.ShouldSnapshot(),
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
	return es.replayer.Replay(ctx, aggregateID, fromVersion, toVersion, fn)
}
