package models

import (
	"context"
)

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
