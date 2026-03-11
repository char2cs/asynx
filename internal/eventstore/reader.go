package eventstore

import (
	"context"
	"encoding/json"

	asynxmd "github.com/char2cs/asynx/models"
)

// Reader loads the current aggregate state from storage using the optimal path:
// snapshot + delta replay (warm path) when a snapshot exists, or full replay
// from event 1 (cold path) when it does not.
//
// Reader is stateless and safe for concurrent use.
type Reader[T any] struct {
	eventStore     asynxmd.Store
	snapshotStore  asynxmd.Store
	replayer       *Replayer[T]
	stateZeroValue T
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

	var snap snapshotBlob
	if err := json.Unmarshal(snapshotBlobs[0], &snap); err != nil {
		// Snapshot corrupted — fall back to full replay.
		return r.coldPath(ctx, aggregateID)
	}

	var snapshotState T
	if err := json.Unmarshal(snap.State, &snapshotState); err != nil {
		// State deserialization failed — fall back to full replay.
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

	return r.replayer.hydrate(ctx, aggregateID, snapshotState, events)
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

	return r.replayer.hydrate(ctx, aggregateID, r.stateZeroValue, events)
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
	if err != nil && err != asynxmd.ErrNotFound {
		return err
	}

	return nil
}

// deserializeEvents unmarshals a slice of raw event blobs into internalEvents.
func deserializeEvents(blobs [][]byte) ([]internalEvent, error) {
	events := make([]internalEvent, 0, len(blobs))
	for _, blob := range blobs {
		var evt internalEvent
		if err := json.Unmarshal(blob, &evt); err != nil {
			return nil, err
		}
		events = append(events, evt)
	}

	return events, nil
}
