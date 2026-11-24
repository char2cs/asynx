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
	StoreReader
	DeliveryLog

	// Atomically runs fn in a transaction: it commits when fn returns nil and
	// rolls back when fn returns an error, which Atomically then returns.
	// fn must not call Atomically again.
	Atomically(
		ctx context.Context,
		fn func(tx Tx) error,
	) error
}
