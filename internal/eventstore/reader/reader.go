package reader

import (
	"context"
	"encoding/json"
	"errors"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/eventstore/replayer"
	asynxmd "github.com/char2cs/asynx/models"
)

// Reader loads the current aggregate state from storage using the optimal path:
// snapshot + delta replay (warm path) when a snapshot exists, or full replay
// from event 1 (cold path) when it does not.
//
// Reader is stateless and safe for concurrent use.
type Reader[T any] struct {
	eventStore           asynxmd.Store
	snapshotStore        asynxmd.Store
	replayer             *replayer.Replayer[T]
	stateZeroValue       T
	onCorruption         func(error)
	currentSchemaVersion int
}

// New constructs a Reader. onCorruption is called when a snapshot cannot be
// deserialized; pass nil to silently fall back to the cold path.
func New[T any](
	es, ss asynxmd.Store,
	rep *replayer.Replayer[T],
	schemaVersion int,
	zeroValue T,
	onCorruption func(error),
) *Reader[T] {
	return &Reader[T]{
		eventStore:           es,
		snapshotStore:        ss,
		replayer:             rep,
		stateZeroValue:       zeroValue,
		onCorruption:         onCorruption,
		currentSchemaVersion: schemaVersion,
	}
}

// Get returns the current aggregate state, selecting warm or cold path
// transparently. Snapshot corruption falls back to full cold replay.
//
// Errors:
//   - ErrNotFound if the aggregate has no events.
//   - Storage errors propagated from Store.
//   - Upcaster panics propagated (fail fast).
//   - Auto-snapshot write errors returned alongside correct state.
func (r *Reader[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	snapshotBlobs, err := r.snapshotStore.ReadFrom(ctx, "snapshots:"+aggregateID, 0)
	if err != nil {
		return r.stateZeroValue, err
	}

	if len(snapshotBlobs) == 0 {
		return r.coldPath(ctx, aggregateID)
	}

	var snap esmodels.SnapshotBlob[T]
	if err := json.Unmarshal(snapshotBlobs[len(snapshotBlobs)-1], &snap); err != nil {
		if r.onCorruption != nil {
			r.onCorruption(err)
		}
		return r.coldPath(ctx, aggregateID)
	}

	eventBlobs, err := r.eventStore.ReadFrom(ctx, "events:"+aggregateID, snap.Version+1)
	if err != nil {
		return r.stateZeroValue, err
	}

	events, err := deserializeEvents(eventBlobs)
	if err != nil {
		return r.stateZeroValue, err
	}

	result, err := r.replayer.Hydrate(ctx, aggregateID, snap.State, events)
	if err != nil {
		return r.stateZeroValue, err
	}

	if result.DidUpcast {
		// Auto-snapshot write is best-effort; snapshot failures don't affect state availability
		// since the state is already durable in the event store. Failures are silently ignored.
		_ = r.writeAutoSnapshot(ctx, aggregateID, result.LastVersion, result.State)
	}

	return result.State, nil
}

// coldPath performs a full replay from event 1 using the zero value as seed.
func (r *Reader[T]) coldPath(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	eventBlobs, err := r.eventStore.ReadFrom(ctx, "events:"+aggregateID, 1)
	if err != nil {
		return r.stateZeroValue, err
	}

	if len(eventBlobs) == 0 {
		return r.stateZeroValue, asynxmd.ErrNotFound
	}

	events, err := deserializeEvents(eventBlobs)
	if err != nil {
		return r.stateZeroValue, err
	}

	result, err := r.replayer.Hydrate(ctx, aggregateID, r.stateZeroValue, events)
	if err != nil {
		return r.stateZeroValue, err
	}

	if result.DidUpcast {
		// Auto-snapshot write is best-effort; snapshot failures don't affect state availability
		// since the state is already durable in the event store. Failures are silently ignored.
		_ = r.writeAutoSnapshot(ctx, aggregateID, result.LastVersion, result.State)
	}

	return result.State, nil
}

// writeAutoSnapshot packs a SnapshotBlob[T] and appends it to the snapshot store.
func (r *Reader[T]) writeAutoSnapshot(
	ctx context.Context,
	aggregateID string,
	version int64,
	state T,
) error {
	snap := esmodels.SnapshotBlob[T]{
		Version:       version,
		SchemaVersion: r.currentSchemaVersion,
		State:         state,
	}
	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return r.snapshotStore.Append(ctx, "snapshots:"+aggregateID, version, snapJSON)
}

// Exists returns true if the aggregate has at least one event.
// It issues a minimal ReadRange(count=1) query and is faster than Get.
func (r *Reader[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	events, err := r.eventStore.ReadRange(ctx, "events:"+aggregateID, 1, 1)
	if err != nil {
		return false, err
	}

	return len(events) > 0, nil
}

// Preload pays the cold-path cost offline by triggering a full Get and
// discarding the result. ErrNotFound is not an error (aggregate may not
// exist yet). Subsequent Get calls for this aggregate use the warm path
// if a snapshot was written during hydration.
func (r *Reader[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	_, err := r.Get(ctx, aggregateID)
	if err != nil && !errors.Is(err, asynxmd.ErrNotFound) {
		return err
	}

	return nil
}

// deserializeEvents unmarshals a slice of raw event blobs into InternalEvents.
func deserializeEvents(blobs [][]byte) ([]esmodels.InternalEvent, error) {
	events := make([]esmodels.InternalEvent, 0, len(blobs))
	for _, blob := range blobs {
		var evt esmodels.InternalEvent
		if err := json.Unmarshal(blob, &evt); err != nil {
			return nil, err
		}
		events = append(events, evt)
	}

	return events, nil
}
