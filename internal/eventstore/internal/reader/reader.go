package reader

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/char2cs/asynx/internal/eventstore/internal/replayer"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/storage"
)

// Reader loads the current aggregate state from storage using the optimal path:
// snapshot + delta replay (warm path) when a usable snapshot exists, or full
// replay from event 1 (cold path) when it does not. A snapshot that cannot be
// decoded is reported to the corruption callback and treated as absent.
//
// Reading is a query except for one deliberate side effect: when replay had to
// upcast an old event, the reader seals the migration by storing the upcast
// state as a snapshot, so later reads take the warm path. That write is best
// effort: a failed write never fails the read, because the state it would have
// cached is already durable in the event store and a missing snapshot only
// costs a slower next read. The failure is reported to the handler installed
// with SetSnapshotErrorHandler and is dropped when none is installed (the
// corruption callback is about undecodable snapshots, not failed writes).
//
// Reader holds no per-call state and is safe for concurrent use, with one
// exception: SetSnapshotGuard must be called before the reader is shared.
type Reader[T any] struct {
	eventStore           storage.Store
	snapshotStore        storage.SnapshotStore
	replayer             *replayer.Replayer[T]
	stateZeroValue       T
	onCorruption         func(error)
	currentSchemaVersion int
	guard                esmodels.SnapshotGuard
	onSnapshotError      func(context.Context, event.Event[T], error)
}

// SetSnapshotErrorHandler installs the callback that observes a failed
// auto-snapshot write. It is called synchronously on the goroutine that
// performed the read, after the read's state was computed and before the read
// returns, with the aggregate id, the version the snapshot was meant for and
// the write error; see the exported builder option for the Event fields that
// are populated. For a lock-free Get the callback runs inside the snapshot
// guard, so the aggregate's command lock is held while it runs and it should
// return quickly. A panic in the callback is contained and never breaks the
// read; one line about it is written to stderr. A
// failure to re-check the stream before a lock-free write is reported to the
// callback like a failed write. A nil callback, the default, drops the failure. Call it before the
// reader is used from more than one goroutine.
func (r *Reader[T]) SetSnapshotErrorHandler(
	fn func(context.Context, event.Event[T], error),
) {
	r.onSnapshotError = fn
}

// SetSnapshotGuard installs the guard a lock-free Get takes before storing an
// auto-snapshot. The guard runs the write while no command or Forget runs for
// the aggregate and reports false, without running it, when it cannot arrange
// that; the snapshot is then skipped, which is safe because the next command
// that snapshots replaces it anyway. Without a guard the write is only checked
// against the stream and not serialised with anything. Call it before the
// reader is used from more than one goroutine; Load never uses the guard because
// its caller already holds the aggregate's command lock.
func (r *Reader[T]) SetSnapshotGuard(
	guard esmodels.SnapshotGuard,
) {
	r.guard = guard
}

// New constructs a Reader over the event and snapshot stores. rep hydrates
// events into state and decides whether a replay upcast anything; schemaVersion
// is the version stamped on the auto-snapshots the reader writes; zeroValue is
// the seed for a cold replay and the state returned alongside an error.
// onCorruption is called with the decoding error when a snapshot cannot be
// deserialized; pass nil to fall back to the cold path silently.
func New[T any](
	es storage.Store,
	ss storage.SnapshotStore,
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

// Get returns the current aggregate state, selecting the warm or cold path
// transparently. It does not hold the aggregate's command lock, so an
// auto-snapshot it writes after an upcast goes through the snapshot guard and
// is skipped when the aggregate is busy or the stream moved on or was erased
// since it was read, which keeps a concurrent Forget from being undone.
//
// Errors:
//   - ErrNotFound if the aggregate has no events.
//   - Storage errors propagated from the stores.
//   - Upcaster failures propagated.
//
// A failed or skipped auto-snapshot write is not an error; a failed one is
// reported to the snapshot error handler when one is installed.
func (r *Reader[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	loaded, err := r.read(
		ctx,
		aggregateID,
	)
	if err != nil {
		return r.stateZeroValue,
			err
	}

	r.sealUpcastLockFree(
		ctx,
		aggregateID,
		loaded,
	)
	return loaded.state,
		nil
}

// Load returns the current aggregate state together with its version: the
// version of the last event applied to build that state (or the snapshot
// version when no deltas follow it). The version is captured from the same
// read that produced the state, so callers can use it as the expected version
// for an optimistic-concurrency write. A brand-new aggregate returns
// ErrNotFound with version 0.
//
// The caller must hold the aggregate's command lock. That is what lets Load
// store the auto-snapshot of an upcast state directly, without the guard or the
// stream check Get needs; as with Get, a failed write does not fail the read and
// is reported to the snapshot error handler when one is installed.
func (r *Reader[T]) Load(
	ctx context.Context,
	aggregateID string,
) (T, int64, error) {
	loaded, err := r.read(
		ctx,
		aggregateID,
	)
	if err != nil {
		return r.stateZeroValue,
			0,
			err
	}

	r.sealUpcastLockHeld(
		ctx,
		aggregateID,
		loaded,
	)
	return loaded.state,
		loaded.version,
		nil
}

func (r *Reader[T]) read(
	ctx context.Context,
	aggregateID string,
) (loadedState[T], error) {
	snap, usable, err := r.readSnapshot(
		ctx,
		aggregateID,
	)
	if err != nil {
		return loadedState[T]{},
			err
	}

	if !usable {
		return r.coldPath(
			ctx,
			aggregateID,
		)
	}

	return r.warmPath(
		ctx,
		aggregateID,
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
	snap esmodels.SnapshotBlob[T],
) (loadedState[T], error) {
	events, err := r.readEvents(
		ctx,
		aggregateID,
		snap.Version+1,
	)
	if err != nil {
		return loadedState[T]{},
			err
	}

	return r.hydrate(
		ctx,
		snap.State,
		snap.Version,
		events,
	)
}

func (r *Reader[T]) coldPath(
	ctx context.Context,
	aggregateID string,
) (loadedState[T], error) {
	events, err := r.readEvents(
		ctx,
		aggregateID,
		1,
	)
	if err != nil {
		return loadedState[T]{},
			err
	}

	if len(events) == 0 {
		return loadedState[T]{},
			failure.ErrNotFound
	}

	return r.hydrate(
		ctx,
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
	seed T,
	seedVersion int64,
	events []esmodels.InternalEvent,
) (loadedState[T], error) {
	result, err := r.replayer.Hydrate(
		ctx,
		seed,
		events,
	)
	if err != nil {
		return loadedState[T]{},
			err
	}

	loaded := loadedState[T]{
		state:    result.State,
		version:  seedVersion,
		upcasted: result.DidUpcast,
	}
	if len(events) > 0 {
		last := events[len(events)-1]
		loaded.version = last.Version
		loaded.lastEventID = last.ID
	}

	return loaded,
		nil
}

// Exists reports whether the aggregate has at least one event. It reads a
// single event instead of rebuilding state, so it never touches snapshots and is
// much cheaper than Get. A storage error is returned with false.
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

// Preload pays the cold-path cost ahead of time by performing a Get and
// discarding the state. ErrNotFound is not an error, because the aggregate may
// not exist yet; every other error is returned. Like Get it runs lock-free and
// may store an auto-snapshot of an upcast state, so a later Get takes the warm
// path.
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
			failure.ErrNotFound,
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
