package store

import (
	"context"
)

// MemorySeeder writes and removes streams outside a transaction, to seed them
// in tests. Each method runs its own transaction, so it is atomic on its own
// and serializes with every other transaction of the store. It must not be
// called from inside the function passed to Atomically.
type MemorySeeder interface {
	// Append writes one blob at (streamID, version) in its own transaction.
	// A version already stored returns an error wrapping
	// models.ErrPipelineFailed, and an error scheduled with SetError is
	// returned instead, once.
	Append(
		ctx context.Context,
		streamID string,
		version int64,
		data []byte,
	) error

	// Delete removes streamID in its own transaction. Deleting a stream that
	// does not exist is not an error.
	Delete(
		ctx context.Context,
		streamID string,
	) error
}
