package store

import (
	"github.com/char2cs/asynx/models/storage"
)

// Memory is a thread-safe in-memory store for tests and examples. It
// satisfies storage.Store, so it passes the store/storetest conformance suite,
// adds failure injection through SetError, and offers the Append and Delete
// seeding helpers of MemorySeeder for building streams without writing a
// transaction.
//
// Transactions are serialized and rolled back through an undo log. Isolation
// is read-uncommitted: reads outside a transaction may observe its writes
// before it commits, and see them disappear if it rolls back. Nothing is
// persisted: all data is lost when the process ends, so production
// deployments must implement storage.Store over a durable backend.
type Memory interface {
	storage.Store
	MemorySeeder

	// SetError schedules a one-shot error for the next Append call on
	// streamID, whether made through MemorySeeder or inside a transaction.
	// That Append returns err unchanged and stores nothing, and the schedule
	// is then cleared. Scheduling again before it fired replaces the earlier
	// error, and other streams are not affected.
	SetError(
		streamID string,
		err error,
	)
}
