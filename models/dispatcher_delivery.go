package models

import (
	"context"
)

// DispatcherDelivery is the queueing side of a Dispatcher: enqueuing requests and finding or explaining their deliveries.
type DispatcherDelivery[T any] interface {
	// Enqueue puts the request on its aggregate's queue and returns its
	// ticket. Enqueue order is delivery order, so callers that must keep
	// versions ordered enqueue while holding their per-aggregate lock and wait
	// on the ticket afterwards. A request carrying an Ack for a parked
	// aggregate is not queued: its ticket settles and ends with
	// ErrDeliveryParked, and its version is recorded as undelivered (see
	// Unrun). After Close, Enqueue fails with ErrDispatcherClosed.
	Enqueue(
		ctx context.Context,
		request DispatchRequest[T],
	) (Ticket, error)

	// TicketFor returns the ticket of the event at (aggregateID, version) while
	// its delivery is under way: queued, being handled, or retrying. If the
	// event was never queued there is none and false is returned. Once the
	// delivery is over there is none either, though a finished delivery may
	// still be returned for a brief moment after its Final closes (its ticket
	// then reports the outcome at once). A retried idempotent command uses it
	// to wait for the handlers of the event its first attempt produced.
	TicketFor(
		aggregateID string,
		version int64,
	) (Ticket, bool)

	// Unrun reports why no handler ran for the event at (aggregateID, version):
	// ErrDeliveryParked while the aggregate is parked and the event was left
	// undelivered by the park. It is nil for an event whose handlers ran,
	// whatever their outcome, and once the park has lifted.
	Unrun(
		aggregateID string,
		version int64,
	) error
}
