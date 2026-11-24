package reader

import (
	"context"
	"encoding/json"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
)

func (r *Reader[T]) sealUpcastLockHeld(
	ctx context.Context,
	aggregateID string,
	loaded loadedState[T],
) {
	if !loaded.upcasted {
		return
	}

	r.storeAutoSnapshot(
		ctx,
		aggregateID,
		loaded,
	)
}

func (r *Reader[T]) sealUpcastLockFree(
	ctx context.Context,
	aggregateID string,
	loaded loadedState[T],
) {
	if !loaded.upcasted {
		return
	}

	storeIfStreamHolds := func() {
		r.storeAutoSnapshotIfStreamHolds(
			ctx,
			aggregateID,
			loaded,
		)
	}
	if r.guard == nil {
		storeIfStreamHolds()
		return
	}

	r.guard(
		aggregateID,
		storeIfStreamHolds,
	)
}

func (r *Reader[T]) storeAutoSnapshotIfStreamHolds(
	ctx context.Context,
	aggregateID string,
	loaded loadedState[T],
) {
	if !r.streamHolds(
		ctx,
		aggregateID,
		loaded.version,
		loaded.lastEventID,
	) {
		return
	}

	r.storeAutoSnapshot(
		ctx,
		aggregateID,
		loaded,
	)
}

func (r *Reader[T]) storeAutoSnapshot(
	ctx context.Context,
	aggregateID string,
	loaded loadedState[T],
) {
	err := r.writeAutoSnapshot(
		ctx,
		aggregateID,
		loaded.version,
		loaded.state,
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
