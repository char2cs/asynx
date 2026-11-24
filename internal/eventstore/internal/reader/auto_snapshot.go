package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/models/event"
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

	if r.guard == nil {
		r.storeAutoSnapshotIfStreamHolds(
			ctx,
			aggregateID,
			loaded,
		)
		return
	}

	r.guard(
		aggregateID,
		r.guardedStore(
			ctx,
			aggregateID,
			loaded,
		),
	)
}

func (r *Reader[T]) guardedStore(
	ctx context.Context,
	aggregateID string,
	loaded loadedState[T],
) func() {
	return func() {
		r.storeAutoSnapshotIfStreamHolds(
			ctx,
			aggregateID,
			loaded,
		)
	}
}

func (r *Reader[T]) storeAutoSnapshotIfStreamHolds(
	ctx context.Context,
	aggregateID string,
	loaded loadedState[T],
) {
	holds, err := r.streamHolds(
		ctx,
		aggregateID,
		loaded.version,
		loaded.lastEventID,
	)
	if err != nil {
		r.reportSnapshotError(
			ctx,
			aggregateID,
			loaded,
			fmt.Errorf(
				"check the stream before the auto-snapshot of %s at version %d: %w",
				aggregateID,
				loaded.version,
				err,
			),
		)
		return
	}

	if !holds {
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
		r.reportSnapshotError(
			ctx,
			aggregateID,
			loaded,
			err,
		)
	}
}

func (r *Reader[T]) reportSnapshotError(
	ctx context.Context,
	aggregateID string,
	loaded loadedState[T],
	err error,
) {
	if r.onSnapshotError == nil {
		return
	}

	defer writeSnapshotHandlerPanicToStderr()
	r.onSnapshotError(
		ctx,
		event.Event[T]{
			ID:            loaded.lastEventID,
			AggregateID:   aggregateID,
			Version:       loaded.version,
			SchemaVersion: r.currentSchemaVersion,
			Aggregate:     loaded.state,
		},
		err,
	)
}

func writeSnapshotHandlerPanicToStderr() {
	recovered := recover()
	if recovered == nil {
		return
	}

	fmt.Fprintf(
		os.Stderr,
		"asynx: the snapshot error handler panicked and was contained: %v\n",
		recovered,
	)
}

func (r *Reader[T]) streamHolds(
	ctx context.Context,
	aggregateID string,
	version int64,
	eventID string,
) (bool, error) {
	blobs, err := r.eventStore.ReadRange(
		ctx,
		"events:"+aggregateID,
		version,
		1,
	)
	if err != nil {
		return false,
			err
	}

	if len(blobs) != 1 {
		return false,
			nil
	}

	events, err := deserializeEvents(blobs)
	if err != nil {
		return false,
			err
	}

	return events[0].Version == version &&
			events[0].ID == eventID,
		nil
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
