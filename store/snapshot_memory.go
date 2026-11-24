package store

import (
	"github.com/char2cs/asynx/models"
)

// SnapshotMemory is a thread-safe in-memory snapshot store for tests and
// examples. It satisfies models.SnapshotStore and adds failure injection via
// SetError. Data is not persisted across restarts; production deployments
// should implement models.SnapshotStore over a durable backend.
type SnapshotMemory interface {
	models.SnapshotStore

	// SetError schedules a one-shot error for the next Put call on
	// aggregateID. The error is consumed and removed after the first
	// matching Put.
	SetError(
		aggregateID string,
		err error,
	)
}
