package reader

import (
	"context"
	"encoding/json"

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
	EventStore     asynxmd.Store
	SnapshotStore  asynxmd.Store
	Replayer       *replayer.Replayer[T]
	StateZeroValue T
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
	snapshotBlobs, err := r.SnapshotStore.ReadFrom(ctx, "snapshots:"+aggregateID, 0)
	if err != nil {
		return r.StateZeroValue, err
	}

	if len(snapshotBlobs) == 0 {
		return r.coldPath(ctx, aggregateID)
	}

	var snap esmodels.SnapshotBlob
	if err := json.Unmarshal(snapshotBlobs[len(snapshotBlobs)-1], &snap); err != nil {
		// Snapshot corrupted — fall back to full replay.
		return r.coldPath(ctx, aggregateID)
	}

	var snapshotState T
	if err := json.Unmarshal(snap.State, &snapshotState); err != nil {
		// State deserialization failed — fall back to full replay.
		return r.coldPath(ctx, aggregateID)
	}

	eventBlobs, err := r.EventStore.ReadFrom(ctx, "events:"+aggregateID, snap.Version+1)
	if err != nil {
		return r.StateZeroValue, err
	}

	events, err := deserializeEvents(eventBlobs)
	if err != nil {
		return r.StateZeroValue, err
	}

	return r.Replayer.Hydrate(ctx, aggregateID, snapshotState, events)
}

// coldPath performs a full replay from event 1 using the zero value as seed.
func (r *Reader[T]) coldPath(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	eventBlobs, err := r.EventStore.ReadFrom(ctx, "events:"+aggregateID, 1)
	if err != nil {
		return r.StateZeroValue, err
	}

	if len(eventBlobs) == 0 {
		return r.StateZeroValue, asynxmd.ErrNotFound
	}

	events, err := deserializeEvents(eventBlobs)
	if err != nil {
		return r.StateZeroValue, err
	}

	return r.Replayer.Hydrate(ctx, aggregateID, r.StateZeroValue, events)
}

// Exists returns true if the aggregate has at least one event.
// It issues a minimal ReadRange(count=1) query and is faster than Get.
func (r *Reader[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	events, err := r.EventStore.ReadRange(ctx, "events:"+aggregateID, 1, 1)
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
