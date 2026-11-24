package models

import (
	"context"
)

// TxStreamWriter appends to and deletes event streams inside a transaction.
type TxStreamWriter interface {
	// Append stores data at (streamID, version). It enforces
	// (streamID, version) uniqueness — the sole coordination mechanism for
	// correctness across concurrent writers — and returns an error wrapping
	// ErrPipelineFailed on a conflict.
	Append(
		ctx context.Context,
		streamID string,
		version int64,
		data []byte,
	) error

	// DeleteStream removes every record of streamID.
	// Idempotent — deleting a non-existent stream is not an error.
	DeleteStream(
		ctx context.Context,
		streamID string,
	) error
}
