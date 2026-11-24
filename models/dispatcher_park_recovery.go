package models

import (
	"context"
)

// DispatcherParkRecovery lifts parks and reports the delivery status of aggregates.
type DispatcherParkRecovery interface {
	// ResumeUndelivered lifts the park of the given generation and reports
	// whether it did. A park that is gone, or has been lifted and imposed again
	// since the caller listed it (see Parks), is left alone, because what the
	// caller read about it is stale. It also returns, in ascending order, the
	// versions the park left undelivered: the events no handler took, which a
	// redelivery must cover even when the caller's own scan of the delivery log
	// predates them. Putting them back if the redelivery fails is the caller's
	// (see Park), because lifting the park forgets them.
	ResumeUndelivered(
		aggregateID string,
		generation uint64,
	) (lifted bool, undelivered []int64)

	// Inspect reports the aggregate's delivery status, read in one atomic
	// step: whether it is parked, the park's generation (as Parks lists it),
	// the cause that parked it and, in ascending order, the versions the park
	// left undelivered. An aggregate that is not parked, or that the
	// dispatcher knows nothing of, has the zero DeliveryStatus. Inspect
	// changes nothing.
	Inspect(
		aggregateID string,
	) DeliveryStatus

	// InHandler reports whether ctx belongs to a handler this dispatcher is
	// running for aggregateID. Waiting on that aggregate's deliveries from
	// there would wait on the handler itself.
	InHandler(
		ctx context.Context,
		aggregateID string,
	) bool
}
