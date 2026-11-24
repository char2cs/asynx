package models

import (
	"context"
)

// DeliveryErrorHandler observes a failed durable delivery attempt. It is told
// of every failed attempt, of a timed-out handler, of a dead-lettered event and
// of a park after the attempts ran out; it cannot change the outcome. attempt
// counts from 1 for the event's current failing streak.
//
// Its error is a *SubscriberError naming the subscriber that failed; it
// matches ErrHandlerPanic or ErrHandlerTimeout for those failures, or wraps the
// handler's own error, so errors.Is and errors.As work as expected. A panic in
// the handler is swallowed.
type DeliveryErrorHandler[T any] func(
	ctx context.Context,
	event Event[T],
	err error,
	attempt int,
)
