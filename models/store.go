package models

import "context"

// Receipt records the outcome of an idempotent command (see IdempotentCommand).
type Receipt struct {
	// Accepted is true when the command produced an event.
	Accepted bool
	// EventID and Version identify the event an accepted command produced.
	EventID string
	Version int64
	// Rejection holds the validation error text of a rejected command.
	Rejection string
}

// Delivery is a pending hand-off of one committed event to durable
// subscribers. It is written in the same transaction as the event and removed
// by AckDelivery once every handler for the event has finished.
type Delivery struct {
	AggregateID string
	Version     int64
}

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

// Store is the durable backend for event streams, command receipts and the
// delivery log. Its contract is the foundation of asynx's guarantees; run
// store/storetest.Run against any implementation.
//
// Writes go through Atomically only, so an event can never be committed
// without the projections, receipt and delivery entry that belong to it.
type Store interface {
	// ReadFrom returns the blobs of streamID with version >= fromVersion, in
	// version order.
	ReadFrom(
		ctx context.Context,
		streamID string,
		fromVersion int64,
	) ([][]byte, error)

	// ReadRange returns at most count blobs of streamID starting at
	// fromVersion, in version order.
	ReadRange(
		ctx context.Context,
		streamID string,
		fromVersion int64,
		count int64,
	) ([][]byte, error)

	// GetReceipt returns the receipt for commandID; found is false when none
	// exists.
	GetReceipt(
		ctx context.Context,
		commandID string,
	) (r Receipt, found bool, err error)

	// PendingDeliveries returns up to limit unacknowledged deliveries in
	// enqueue order. Deliveries of one aggregate are always in ascending
	// version order.
	PendingDeliveries(
		ctx context.Context,
		limit int,
	) ([]Delivery, error)

	// AckDelivery removes the delivery. Acknowledging a delivery that does not
	// exist is not an error.
	AckDelivery(
		ctx context.Context,
		aggregateID string,
		version int64,
	) error

	// Atomically runs fn in a transaction: it commits when fn returns nil and
	// rolls back when fn returns an error, which Atomically then returns.
	// fn must not call Atomically again.
	Atomically(
		ctx context.Context,
		fn func(tx Tx) error,
	) error
}
