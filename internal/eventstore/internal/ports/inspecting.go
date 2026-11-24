package ports

import (
	"context"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Inspecting gives the command processor access to the machinery under the
// event store: the history of an aggregate, the backing store it shares
// transactions with, and the hook that makes lock-free reads safe against
// Forget.
type Inspecting[T any] interface {
	// GuardAutoSnapshots installs the guard under which a lock-free Get or
	// Preload stores the snapshot of an upcast state. The guard must run its
	// function while no command or Forget runs for the aggregate and report
	// false without running it when that cannot be arranged, in which case the
	// snapshot is skipped. It must be called before the store is shared between
	// goroutines; without a guard the write is only checked against the stream
	// and is not serialised with anything.
	GuardAutoSnapshots(
		guard esmodels.SnapshotGuard,
	)
	// Store returns the backing event store, so callers can enlist their own
	// writes (projections, receipts, deliveries) in the same transaction as an
	// append.
	Store() asynxmd.Store
	// Replay feeds fn every event of the aggregate with a version in
	// fromVersion..toVersion (toVersion 0 means to the end), in version order,
	// each upcast to the current schema and paired with the state before and
	// after it. Replay is read-only and never writes a snapshot. fn runs on the
	// caller's goroutine; the first storage, upcast or patch error stops the
	// replay and is returned. An aggregate with no events replays nothing.
	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn asynxmd.ProjectionHandler[T],
	) error
}
