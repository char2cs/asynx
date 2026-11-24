package dispatch

import (
	"context"
)

// Inspector reports on deliveries without changing them.
type Inspector interface {
	// Inspect reports the aggregate's delivery status, read in one atomic
	// step: whether it is parked, the park's generation (as
	// dispatcher.Parking.Parks lists it), the cause that parked it and, in
	// ascending order, the versions the park left undelivered. An aggregate
	// that is not parked, or that the dispatcher knows nothing of, has the
	// zero Status. Inspect changes nothing.
	Inspect(
		aggregateID string,
	) Status

	// Unrun reports why no handler ran for the event at (aggregateID, version):
	// failure.ErrDeliveryParked while the aggregate is parked and the event was left
	// undelivered by the park. It is nil for an event whose handlers ran,
	// whatever their outcome, and once the park has lifted.
	Unrun(
		aggregateID string,
		version int64,
	) error

	// InHandler reports whether ctx belongs to a handler this dispatcher is
	// running for aggregateID. Waiting on that aggregate's deliveries from
	// there would wait on the handler itself, which asynx uses to refuse a
	// SendWait or a Forget made from inside a handler.
	InHandler(
		ctx context.Context,
		aggregateID string,
	) bool
}
