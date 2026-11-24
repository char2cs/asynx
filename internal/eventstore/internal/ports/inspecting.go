package ports

import (
	"context"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Inspecting exposes replay, the backing store and the snapshot guard.
type Inspecting[T any] interface {
	// GuardAutoSnapshots installs the guard under which a lock-free read writes
	// an auto-snapshot.
	GuardAutoSnapshots(
		guard esmodels.SnapshotGuard,
	)
	// Store returns the underlying event store.
	Store() asynxmd.Store
	// Replay feeds the aggregate's events in the version range to fn.
	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn asynxmd.ProjectionHandler[T],
	) error
}
