package eventstore

import (
	"context"

	"github.com/char2cs/asynx/internal/eventstore/internal/core"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// EventStore is the API for durable aggregate persistence. Construct it with
// New.
type EventStore[T any] interface {
	// GuardAutoSnapshots installs the guard under which a lock-free read writes
	// an auto-snapshot.
	GuardAutoSnapshots(
		guard esmodels.SnapshotGuard,
	)
	// Get returns the current aggregate state.
	Get(
		ctx context.Context,
		aggregateID string,
	) (T, error)
	// Exists reports whether the aggregate has at least one event.
	Exists(
		ctx context.Context,
		aggregateID string,
	) (bool, error)
	// Preload pays the cold-path cost offline by triggering a full Get.
	Preload(
		ctx context.Context,
		aggregateID string,
	) error
	// Write validates cmd, appends its event and snapshots when due.
	Write(
		ctx context.Context,
		cmd asynxmd.Command[T],
	) (asynxmd.Event[T], error)
	// WriteTx is Write with hook running inside the append's transaction.
	WriteTx(
		ctx context.Context,
		cmd asynxmd.Command[T],
		hook esmodels.TxHook[T],
	) (asynxmd.Event[T], error)
	// Snapshot writes the snapshot of the state evt produced.
	Snapshot(
		ctx context.Context,
		evt asynxmd.Event[T],
	) error
	// Store returns the underlying event store.
	Store() asynxmd.Store
	// DeleteSnapshot removes the aggregate's snapshot.
	DeleteSnapshot(
		ctx context.Context,
		aggregateID string,
	) error
	// Delete removes the aggregate's events and snapshot.
	Delete(
		ctx context.Context,
		aggregateID string,
	) error
	// Replay feeds the aggregate's events in the version range to fn.
	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn asynxmd.ProjectionHandler[T],
	) error
}

// New builds a fully-configured EventStore. eventStore and snapshotStore use
// different contracts and cannot be the same instance. upcasters maps a schema
// version to its migration; schemaVersion is the current schema version for new
// events. onCorruption is called when a snapshot cannot be deserialized; pass
// nil to fall back to the cold replay path silently.
func New[T any](
	eventStore asynxmd.Store,
	snapshotStore asynxmd.SnapshotStore,
	upcasters map[int]asynxmd.Upcaster,
	schemaVersion int,
	onCorruption func(error),
) EventStore[T] {
	return core.New[T](
		eventStore,
		snapshotStore,
		upcasters,
		schemaVersion,
		onCorruption,
	)
}
