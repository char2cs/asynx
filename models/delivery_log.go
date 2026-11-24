package models

import (
	"context"
)

// DeliveryLog lists and acknowledges the pending deliveries.
type DeliveryLog interface {
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
}
