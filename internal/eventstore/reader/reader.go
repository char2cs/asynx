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
	snapshotStore        asynxmd.SnapshotStore
	replayer             *replayer.Replayer[T]
	stateZeroValue       T
	onCorruption         func(error)
	currentSchemaVersion int
	guard                SnapshotGuard
}

// SnapshotGuard runs fn while no command or Forget can run for the aggregate,
// and reports whether it did. It is how a lock-free read gets to write its
// auto-snapshot without racing a Forget's erase.
type SnapshotGuard func(aggregateID string, fn func()) bool

// SetSnapshotGuard installs the guard a lock-free read takes before writing an
// auto-snapshot. Without one the write is only checked against the stream, not
// serialised with anything.
func (r *Reader[T]) SetSnapshotGuard(guard SnapshotGuard) { r.guard = guard }

// New constructs a Reader. onCorruption is called when a snapshot cannot be
// deserialized; pass nil to silently fall back to the cold path.
func New[T any](
	es asynxmd.Store,
	ss asynxmd.SnapshotStore,
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
// transparently. Snapshot corruption falls back to full cold replay. It does
// not hold the aggregate's command lock, so an auto-snapshot it writes after an
// upcast goes through the snapshot guard (see saveUpcasted).
//
// Errors:
//   - ErrNotFound if the aggregate has no events.
//   - Storage errors propagated from Store.
//   - Upcaster panics propagated (fail fast).
//
// A failed auto-snapshot write is dropped: the state is already durable in the
// event store.
func (r *Reader[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	state, _, err := r.load(ctx, aggregateID, false)
	return state, err
}

// Load returns the current aggregate state together with its version — the
// version of the last event applied to build that state (or the snapshot
// version when no deltas follow it). The version is captured from the same
// read that produced the state, so callers can use it as the expected version
// for an optimistic-concurrency write. A brand-new aggregate returns
// ErrNotFound with version 0. The caller holds the aggregate's command lock.
func (r *Reader[T]) Load(
	ctx context.Context,
	aggregateID string,
) (T, int64, error) {
	return r.load(ctx, aggregateID, true)
}

func (r *Reader[T]) load(
	ctx context.Context,
	aggregateID string,
	lockHeld bool,
) (T, int64, error) {
	snapBlob, found, err := r.snapshotStore.Get(ctx, aggregateID)
	if err != nil {
		return r.stateZeroValue, 0, err
	}

	if !found {
		return r.coldPath(ctx, aggregateID, lockHeld)
	}

	var snap esmodels.SnapshotBlob[T]
	if err := json.Unmarshal(snapBlob, &snap); err != nil {
		if r.onCorruption != nil {
			r.onCorruption(err)
		}
		return r.coldPath(ctx, aggregateID, lockHeld)
	}

	eventBlobs, err := r.eventStore.ReadFrom(ctx, "events:"+aggregateID, snap.Version+1)
	if err != nil {
		return r.stateZeroValue, 0, err
	}

	events, err := deserializeEvents(eventBlobs)
	if err != nil {
		return r.stateZeroValue, 0, err
	}

	result, err := r.replayer.Hydrate(ctx, aggregateID, snap.State, events)
	if err != nil {
		return r.stateZeroValue, 0, err
	}

	loadedVersion := snap.Version
	if len(events) > 0 {
		loadedVersion = events[len(events)-1].Version
	}

	if result.DidUpcast {
		r.saveUpcasted(ctx, aggregateID, lockHeld, result, events[len(events)-1].ID)
	}

	return result.State, loadedVersion, nil
}

// coldPath performs a full replay from event 1 using the zero value as seed.
func (r *Reader[T]) coldPath(
	ctx context.Context,
	aggregateID string,
	lockHeld bool,
) (T, int64, error) {
	eventBlobs, err := r.eventStore.ReadFrom(ctx, "events:"+aggregateID, 1)
	if err != nil {
		return r.stateZeroValue, 0, err
	}

	if len(eventBlobs) == 0 {
		return r.stateZeroValue, 0, asynxmd.ErrNotFound
	}

	events, err := deserializeEvents(eventBlobs)
	if err != nil {
		return r.stateZeroValue, 0, err
	}

	result, err := r.replayer.Hydrate(ctx, aggregateID, r.stateZeroValue, events)
	if err != nil {
		return r.stateZeroValue, 0, err
	}

	loadedVersion := events[len(events)-1].Version

	if result.DidUpcast {
		r.saveUpcasted(ctx, aggregateID, lockHeld, result, events[len(events)-1].ID)
	}

	return result.State, loadedVersion, nil
}

// saveUpcasted writes the auto-snapshot of an upcast state. It is best-effort:
// the state is already durable in the event store, so a failure is dropped.
//
// A read that does not hold the aggregate's command lock must not let its
// snapshot land after a Forget's erase, which would bring the erased state back
// and make a re-created aggregate start above version 1. It therefore writes
// under the snapshot guard (skipping the write if the aggregate is busy: the
// next command snapshots anyway) and only if the event it was built from is
// still the stream's event at that version.
func (r *Reader[T]) saveUpcasted(
	ctx context.Context,
	aggregateID string,
	lockHeld bool,
	result replayer.HydrateResult[T],
	lastEventID string,
) {
	save := func() {
		_ = r.writeAutoSnapshot(ctx, aggregateID, result.LastVersion, result.State)
	}
	if lockHeld {
		save()
		return
	}
	saveIfStreamUnchanged := func() {
		if r.streamHolds(ctx, aggregateID, result.LastVersion, lastEventID) {
			save()
		}
	}
	if r.guard == nil {
		saveIfStreamUnchanged()
		return
	}
	r.guard(aggregateID, saveIfStreamUnchanged)
}

// streamHolds reports whether the aggregate's event at version is still the
// event with the given ID: the stream has not been erased, nor erased and
// re-created, since the state was built from it.
func (r *Reader[T]) streamHolds(
	ctx context.Context,
	aggregateID string,
	version int64,
	eventID string,
) bool {
	blobs, err := r.eventStore.ReadRange(ctx, "events:"+aggregateID, version, 1)
	if err != nil || len(blobs) != 1 {
		return false
	}
	events, err := deserializeEvents(blobs)
	if err != nil {
		return false
	}
	return events[0].Version == version && events[0].ID == eventID
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
	return r.snapshotStore.Put(ctx, aggregateID, version, snapJSON)
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
