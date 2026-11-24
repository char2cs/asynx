package models

import (
	"context"
)

// TxDeliveryWriter enqueues and drops deliveries inside a transaction.
type TxDeliveryWriter interface {
	// DropDeliveries removes every pending delivery of aggregateID.
	// Idempotent.
	DropDeliveries(
		ctx context.Context,
		aggregateID string,
	) error

	// EnqueueDelivery records that (aggregateID, version) must be delivered to
	// durable subscribers.
	EnqueueDelivery(
		ctx context.Context,
		aggregateID string,
		version int64,
	) error
}
