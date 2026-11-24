package reader

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/char2cs/asynx/internal/eventstore/internal/replayer"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
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
	guard                esmodels.SnapshotGuard
}

// SetSnapshotGuard installs the guard a lock-free read takes before writing an
// auto-snapshot. Without one the write is only checked against the stream, not
// serialised with anything.
func (r *Reader[T]) SetSnapshotGuard(
	guard esmodels.SnapshotGuard,
) {
	r.guard = guard
}

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
	state, _, err := r.load(
		ctx,
		aggregateID,
		lockNotHeld,
	)
	return state,
		err
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
	return r.load(
		ctx,
		aggregateID,
		lockHeld,
	)
}

func (r *Reader[T]) load(
	ctx context.Context,
	aggregateID string,
	mode lockMode,
) (T, int64, error) {
	snap, usable, err := r.readSnapshot(
		ctx,
		aggregateID,
	)
	if err != nil {
		return r.stateZeroValue,
			0,
			err
	}

	if !usable {
		return r.coldPath(
			ctx,
			aggregateID,
			mode,
		)
	}

	return r.warmPath(
		ctx,
		aggregateID,
		mode,
		snap,
	)
}

func (r *Reader[T]) readSnapshot(
	ctx context.Context,
	aggregateID string,
) (snap esmodels.SnapshotBlob[T], usable bool, err error) {
	snapBlob, found, err := r.snapshotStore.Get(
		ctx,
		aggregateID,
	)
	if err != nil ||
		!found {
		return snap,
			false,
			err
	}

	err = json.Unmarshal(
		snapBlob,
		&snap,
	)
	if err != nil {
		r.reportCorruption(err)
		return esmodels.SnapshotBlob[T]{},
			false,
			nil
	}

	return snap,
		true,
		nil
}

func (r *Reader[T]) reportCorruption(
	err error,
) {
	if r.onCorruption == nil {
		return
	}

	r.onCorruption(err)
}

func (r *Reader[T]) warmPath(
	ctx context.Context,
	aggregateID string,
	mode lockMode,
	snap esmodels.SnapshotBlob[T],
) (T, int64, error) {
	events, err := r.readEvents(
		ctx,
		aggregateID,
		snap.Version+1,
	)
	if err != nil {
		return r.stateZeroValue,
			0,
			err
	}

	return r.hydrate(
		ctx,
		aggregateID,
		mode,
		snap.State,
		snap.Version,
		events,
	)
}

func (r *Reader[T]) coldPath(
	ctx context.Context,
	aggregateID string,
	mode lockMode,
) (T, int64, error) {
	events, err := r.readEvents(
		ctx,
		aggregateID,
		1,
	)
	if err != nil {
		return r.stateZeroValue,
			0,
			err
	}

	if len(events) == 0 {
		return r.stateZeroValue,
			0,
			asynxmd.ErrNotFound
	}

	return r.hydrate(
		ctx,
		aggregateID,
		mode,
		r.stateZeroValue,
		0,
		events,
	)
}

func (r *Reader[T]) readEvents(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
) ([]esmodels.InternalEvent, error) {
	eventBlobs, err := r.eventStore.ReadFrom(
		ctx,
		"events:"+aggregateID,
		fromVersion,
	)
	if err != nil {
		return nil,
			err
	}

	return deserializeEvents(eventBlobs)
}

func (r *Reader[T]) hydrate(
	ctx context.Context,
	aggregateID string,
	mode lockMode,
	seed T,
	seedVersion int64,
	events []esmodels.InternalEvent,
) (T, int64, error) {
	result, err := r.replayer.Hydrate(
		ctx,
		aggregateID,
		seed,
		events,
	)
	if err != nil {
		return r.stateZeroValue,
			0,
			err
	}

	loadedVersion := seedVersion
	if len(events) > 0 {
		loadedVersion = events[len(events)-1].Version
	}

	if result.DidUpcast {
		r.saveUpcasted(
			ctx,
			aggregateID,
			mode,
			result,
			events[len(events)-1].ID,
		)
	}

	return result.State,
		loadedVersion,
		nil
}

func (r *Reader[T]) saveUpcasted(
	ctx context.Context,
	aggregateID string,
	mode lockMode,
	result replayer.HydrateResult[T],
	lastEventID string,
) {
	if mode == lockHeld {
		r.saveBestEffort(
			ctx,
			aggregateID,
			result,
		)
		return
	}

	saveIfStreamUnchanged := func() {
		r.saveIfStreamHolds(
			ctx,
			aggregateID,
			result,
			lastEventID,
		)
	}
	if r.guard == nil {
		saveIfStreamUnchanged()
		return
	}

	r.guard(
		aggregateID,
		saveIfStreamUnchanged,
	)
}

func (r *Reader[T]) saveIfStreamHolds(
	ctx context.Context,
	aggregateID string,
	result replayer.HydrateResult[T],
	lastEventID string,
) {
	if !r.streamHolds(
		ctx,
		aggregateID,
		result.LastVersion,
		lastEventID,
	) {
		return
	}

	r.saveBestEffort(
		ctx,
		aggregateID,
		result,
	)
}

func (r *Reader[T]) saveBestEffort(
	ctx context.Context,
	aggregateID string,
	result replayer.HydrateResult[T],
) {
	err := r.writeAutoSnapshot(
		ctx,
		aggregateID,
		result.LastVersion,
		result.State,
	)
	if err != nil {
		return
	}
}

func (r *Reader[T]) streamHolds(
	ctx context.Context,
	aggregateID string,
	version int64,
	eventID string,
) bool {
	blobs, err := r.eventStore.ReadRange(
		ctx,
		"events:"+aggregateID,
		version,
		1,
	)
	if err != nil ||
		len(blobs) != 1 {
		return false
	}

	events, err := deserializeEvents(blobs)
	if err != nil {
		return false
	}

	return events[0].Version == version && events[0].ID == eventID
}

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

	return r.snapshotStore.Put(
		ctx,
		aggregateID,
		version,
		snapJSON,
	)
}

// Exists returns true if the aggregate has at least one event.
// It issues a minimal ReadRange(count=1) query and is faster than Get.
func (r *Reader[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	events, err := r.eventStore.ReadRange(
		ctx,
		"events:"+aggregateID,
		1,
		1,
	)
	if err != nil {
		return false,
			err
	}

	return len(events) > 0,
		nil
}

// Preload pays the cold-path cost offline by triggering a full Get and
// discarding the result. ErrNotFound is not an error (aggregate may not
// exist yet). Subsequent Get calls for this aggregate use the warm path
// if a snapshot was written during hydration.
func (r *Reader[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	_, err := r.Get(
		ctx,
		aggregateID,
	)
	if err != nil &&
		!errors.Is(
			err,
			asynxmd.ErrNotFound,
		) {
		return err
	}

	return nil
}

func deserializeEvents(
	blobs [][]byte,
) (events []esmodels.InternalEvent, err error) {
	events = make(
		[]esmodels.InternalEvent,
		len(blobs),
	)
	for i := 0; i < len(blobs) &&
		err == nil; i++ {
		err = json.Unmarshal(
			blobs[i],
			&events[i],
		)
	}

	if err != nil {
		return nil,
			err
	}

	return events,
		nil
}
