package asynx

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// DeliveryControl inspects and resumes parked durable deliveries.
type DeliveryControl[T any] interface {
	// PausedAggregates lists the aggregates whose durable delivery is parked:
	// attempts ran out (see Builder.WithDeliveryMaxAttempts) or a Forget could
	// not deliver its marker. Their durable events stay pending in the store and
	// SendWait on them returns ErrDeliveryParked. Parking lasts until
	// ResumeDeliveries or a restart; it never lifts by itself.
	PausedAggregates() []string

	// InspectDelivery says why an aggregate is paused. The DeliveryStatus it
	// returns reports whether its durable delivery is parked, the park's
	// generation (it changes each time a park is imposed), the error that
	// parked it (Cause), and, in ascending order, the versions no handler took
	// (Undelivered). The cause is ErrDeliveryParked wrapping the
	// last handler error and the attempt count when the attempts ran out, the
	// ack error when acknowledging the delivery failed, ErrDispatcherClosed
	// when a shutdown interrupted the retries, or the error of the failed
	// Forget, enqueue or redelivery that parked it. An aggregate that is not
	// parked, or that the dispatcher does not know, has the zero
	// DeliveryStatus, as does every aggregate of an instance without a
	// dispatcher.
	InspectDelivery(
		aggregateID string,
	) models.DeliveryStatus

	// ResumeDeliveries redelivers the paused aggregates' pending events, in
	// order, ahead of any live event, to the durable subscribers only (live
	// subscribers do not replay). Call it once the cause is fixed.
	ResumeDeliveries(
		ctx context.Context,
	) error
}
