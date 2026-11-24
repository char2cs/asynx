package handler

import (
	"context"

	"github.com/char2cs/asynx/v2/models/event"
)

// DeliveryError observes a failed durable delivery attempt. It is told
// of every failed attempt, of a timed-out handler, of a dead-lettered event and
// of a park after the attempts ran out; it cannot change the outcome. attempt
// counts from 1 for the event's current failing streak.
//
// Its error is a *failure.SubscriberError naming the subscriber that failed; it
// matches failure.ErrHandlerPanic or failure.ErrHandlerTimeout for those failures, or wraps the
// handler's own error, so errors.Is and errors.As work as expected. A panic in
// the handler is swallowed.
type DeliveryError[T any] func(
	ctx context.Context,
	event event.Event[T],
	err error,
	attempt int,
)
