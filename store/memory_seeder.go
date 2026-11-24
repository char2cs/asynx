package store

import (
	"context"
)

// MemorySeeder writes and removes streams outside a transaction, to seed
// them in tests.
type MemorySeeder interface {
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
