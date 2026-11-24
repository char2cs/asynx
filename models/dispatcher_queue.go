package models

import (
	"context"
)

// DispatcherQueue puts events on the per-aggregate queues and takes them off
// again. Everything queued for one aggregate is handled one at a time in
// enqueue order, and aggregates are independent of one another.
type DispatcherQueue[T any] interface {
	// Enqueue puts the request on its aggregate's queue and returns its
	// ticket. Enqueue order is delivery order, so callers that must keep
	// versions ordered enqueue while holding their per-aggregate lock and wait
	// on the ticket afterwards. Enqueue does not block on handlers and never
	// fails for a full queue. A request carrying an Ack for a parked
	// aggregate is not queued: its ticket settles and ends with
	// ErrDeliveryParked, and its version is recorded as undelivered (see
	// DispatcherInspector.Unrun). After Close, Enqueue fails with
	// ErrDispatcherClosed.
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

	// Supersede abandons the aggregate's pending durable deliveries ahead of an
	// erase: queued ones leave memory, a retry in progress gives up, and the
	// aggregate is left unparked with no undelivered record. The entries stay
	// in the delivery log for the caller to drop. Interrupted handlers may
	// still be finishing.
	Supersede(
		aggregateID string,
	)
}
