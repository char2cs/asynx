package store

import (
	"context"
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

	// SetError schedules a one-shot error for the next Append call on
	// streamID. The error is consumed and removed after the first
	// matching Append.
	SetError(
		streamID string,
		err error,
	)

	// Append writes one blob in its own transaction.
	Append(
		ctx context.Context,
		streamID string,
		version int64,
		data []byte,
	) error

	// Delete removes streamID in its own transaction.
	Delete(
		ctx context.Context,
		streamID string,
	) error
}
