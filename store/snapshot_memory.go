package store

import (
	"github.com/char2cs/asynx/models/storage"
)

// SnapshotMemory is a thread-safe in-memory snapshot store for tests and
// examples. It satisfies storage.SnapshotStore, so it passes the
// store/storetest.RunSnapshots conformance suite, and adds failure injection
// through SetError. It keeps one snapshot per aggregate, the latest put; the
// version passed to Put is not retained and no monotonicity guard is applied.
// Data is not persisted across restarts; production deployments should
// implement storage.SnapshotStore over a durable backend.
type SnapshotMemory interface {
	storage.SnapshotStore

	// SetError schedules a one-shot error for the next Put call on
	// aggregateID. That Put returns err unchanged and stores nothing, and the
	// schedule is then cleared. Scheduling again before it fired replaces the
	// earlier error, and other aggregates are not affected.
	SetError(
		aggregateID string,
		err error,
	)
}
