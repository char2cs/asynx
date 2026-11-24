package storage

import (
	"context"
)

// TxDeliveryWriter writes the delivery log inside a transaction. It is the
// write side of DeliveryLog: entries enqueued here are listed by
// DeliveryLog.PendingDeliveries once the transaction has committed, and are
// removed by DeliveryLog.AckDelivery or by DropDeliveries.
type TxDeliveryWriter interface {
	// EnqueueDelivery records that (aggregateID, version) must be delivered to
	// durable subscribers. The entry is appended to the log, so it is listed
	// after every entry enqueued before it, including entries enqueued earlier
	// in the same transaction. A key that was acknowledged or dropped may be
	// enqueued again, because a forgotten aggregate starts over at version 1.
	// The entry does not exist if the transaction rolls back.
	EnqueueDelivery(
		ctx context.Context,
		aggregateID string,
		version int64,
	) error

	// DropDeliveries removes every pending delivery of aggregateID, whatever
	// its version, and leaves the other aggregates' entries alone. It is
	// idempotent: an aggregate with no pending entries is not an error. The
	// removal is part of the transaction, so a rollback restores the entries
	// in their original positions, and an entry enqueued after the drop in the
	// same transaction survives it.
	DropDeliveries(
		ctx context.Context,
		aggregateID string,
	) error
}
