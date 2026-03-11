// Package eventstore provides the persistence layer for Asynx aggregates.
//
// EventStore[T] coordinates three sub-modules:
//   - Reader  — optimistic snapshot + delta warm path, full replay cold path
//   - Writer  — RFC 6902 full-state diffs, conditional snapshots
//   - Replayer — sequential patch application, schema upcasting, auto-snapshots
//
// All operations are stateless (no in-memory cache). Storage is the single
// source of truth; operators may layer caching (Redis, Memcached) on top.
package eventstore

import (
	"context"
	"encoding/json"
	"maps"

	asynxmd "github.com/char2cs/asynx/models"
)

// EventStore[T] is the public API for durable aggregate persistence.
// Construct it with New; all fields are set up by the constructor.
type EventStore[T any] struct {
	eventStore    asynxmd.Store
	snapshotStore asynxmd.Store
	reader        *Reader[T]
	writer        *Writer[T]
	replayer      *Replayer[T]
}

// New builds a fully-configured EventStore. eventStore and snapshotStore may
// be the same Store instance. upcasters maps SchemaVersion → migration func;
// schemaVersion is the current (target) schema version for all new events.
func New[T any](
	eventStore asynxmd.Store,
	snapshotStore asynxmd.Store,
	upcasters map[int]asynxmd.Upcaster,
	schemaVersion int,
) *EventStore[T] {
	var zero T

	clonedUpcasters := maps.Clone(upcasters)
	if clonedUpcasters == nil {
		clonedUpcasters = make(map[int]asynxmd.Upcaster)
	}

	replayer := &Replayer[T]{
		eventStore:           eventStore,
		snapshotStore:        snapshotStore,
		upcasters:            clonedUpcasters,
		currentSchemaVersion: schemaVersion,
		stateZeroValue:       zero,
	}

	reader := &Reader[T]{
		eventStore:     eventStore,
		snapshotStore:  snapshotStore,
		replayer:       replayer,
		stateZeroValue: zero,
	}

	writer := &Writer[T]{
		eventStore:           eventStore,
		snapshotStore:        snapshotStore,
		currentSchemaVersion: schemaVersion,
	}

	return &EventStore[T]{
		eventStore:    eventStore,
		snapshotStore: snapshotStore,
		reader:        reader,
		writer:        writer,
		replayer:      replayer,
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

// Write validates the command against current state, emits the new state,
// computes the RFC 6902 diff, and durably appends the event. It writes a
// snapshot if cmd.ShouldSnapshot() returns true.
//
// Write determines the next version by reading the latest event from storage,
// so each call incurs one extra read. This enables optimistic concurrency:
// if a concurrent writer already appended at the same version, Store.Append
// returns an error which Write propagates to the caller (ErrPipelineFailed
// semantics — the caller should re-read state and retry).
//
// Returns ErrValidation if cmd.Validate fails.
func (es *EventStore[T]) Write(
	ctx context.Context,
	aggregateID string,
	current *T,
	newState T,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	nextVersion, err := es.nextVersion(ctx, aggregateID)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	var previousState T
	if current != nil {
		previousState = *current
	}

	return es.writer.Write(
		ctx,
		aggregateID,
		cmd.EventName(),
		previousState,
		newState,
		nextVersion,
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
	fn func(asynxmd.Event[T]),
) error {
	return es.replayer.Replay(ctx, aggregateID, fromVersion, toVersion, fn)
}

// nextVersion determines the version at which the next event should be written.
// It reads the snapshot (to skip over sealed history) then counts delta events
// to compute latestVersion + 1.
//
// For a brand-new aggregate (no snapshot, no events) it returns 1.
func (es *EventStore[T]) nextVersion(
	ctx context.Context,
	aggregateID string,
) (int64, error) {
	var snapVersion int64

	snapBlobs, err := es.snapshotStore.ReadFrom(ctx, "snapshots:"+aggregateID, 0)
	if err != nil {
		return 0, err
	}

	if len(snapBlobs) > 0 {
		var snap snapshotBlob
		if err := json.Unmarshal(snapBlobs[0], &snap); err == nil {
			snapVersion = snap.Version
		}
	}

	deltaBlobs, err := es.eventStore.ReadFrom(ctx, "events:"+aggregateID, snapVersion+1)
	if err != nil {
		return 0, err
	}

	// Versions are consecutive integers starting at 1, so the next version is
	// simply (last known version) + 1 without unmarshalling individual blobs.
	return snapVersion + int64(len(deltaBlobs)) + 1, nil
}
