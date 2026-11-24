package store

import (
	"github.com/char2cs/asynx/models"
)

// Memory is a thread-safe in-memory store for tests and examples.
// It satisfies models.Store, adds failure injection via SetError, and offers
// Append/Delete helpers for seeding streams without writing a transaction.
//
// Transactions are serialized and rolled back through an undo log. Isolation
// is read-uncommitted: reads outside a transaction may observe its writes
// before it commits.
type Memory interface {
	models.Store
	MemorySeeder

	// SetError schedules a one-shot error for the next Append call on
	// streamID. The error is consumed and removed after the first
	// matching Append.
	SetError(
		streamID string,
		err error,
	)
}
