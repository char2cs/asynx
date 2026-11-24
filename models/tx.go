package models

import (
	"context"
)

// Tx is the write side of a Store transaction. Every method joins the
// transaction opened by Store.Atomically: if the function passed to
// Atomically returns an error, all of them are rolled back together.
//
// Tx must not be used after the function passed to Atomically returns.
type Tx interface {
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

	// DropDeliveries removes every pending delivery of aggregateID.
	// Idempotent.
	DropDeliveries(
		ctx context.Context,
		aggregateID string,
	) error

	// PutReceipt stores the receipt for commandID. It returns an error
	// wrapping ErrDuplicateReceipt when a receipt for commandID already exists.
	PutReceipt(
		ctx context.Context,
		commandID string,
		r Receipt,
	) error

	// EnqueueDelivery records that (aggregateID, version) must be delivered to
	// durable subscribers.
	EnqueueDelivery(
		ctx context.Context,
		aggregateID string,
		version int64,
	) error

	// Native returns the store's underlying transaction handle (for example a
	// *sql.Tx) so synchronous projections can write their own tables inside the
	// same transaction. In-memory stores return nil.
	Native() any
}
