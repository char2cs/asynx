package storage

import (
	"context"

	"github.com/char2cs/asynx/v2/models/event"
)

// DeliveryLog lists and acknowledges the pending deliveries: the committed
// events whose durable subscribers have not all finished yet. Entries are
// enqueued by TxDeliveryWriter in the transaction that commits the event, so
// after a crash the log says exactly which events still need delivering.
// Delivery is therefore at-least-once: an entry is removed only after its
// handlers are done. Both methods are safe for concurrent use and return the
// context's error when ctx is already done.
type DeliveryLog interface {
	// PendingDeliveries returns up to limit unacknowledged deliveries in
	// enqueue order, oldest first. Deliveries of one aggregate are always in
	// ascending version order. Entries are not removed by listing them:
	// calling it twice returns the same entries until they are acknowledged
	// or dropped. Only committed entries are listed.
	PendingDeliveries(
		ctx context.Context,
		limit int,
	) ([]event.Delivery, error)

	// AckDelivery removes the entry for (aggregateID, version), after every
	// handler for the event has finished, the dead-letter handler has
	// accepted it, or it has been superseded. Acknowledging an entry that does
	// not exist, for example one a Forget already dropped with
	// TxDeliveryWriter.DropDeliveries, is not an error. Other entries,
	// including other versions of the same aggregate, are untouched.
	AckDelivery(
		ctx context.Context,
		aggregateID string,
		version int64,
	) error
}
