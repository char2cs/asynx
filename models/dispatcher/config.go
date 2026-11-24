package dispatcher

import (
	"time"

	"github.com/char2cs/asynx/v2/models/handler"
	"github.com/char2cs/asynx/v2/models/pubsub"
	"github.com/char2cs/asynx/v2/models/subscription"
)

// Config is what a Dispatcher is built from. It carries final
// values: whoever builds it applies the defaults (20 attempts, a backoff of
// 100ms doubling up to 30s) and drops non-positive settings, so an
// implementation never needs to know them.
//
// Bus is the live-subscriber transport: dispatch.KindEvent and dispatch.KindTombstone
// requests are published through pubsub.Bus.PublishSync. It may be nil, then no live
// subscriber is reached.
//
// DurableSubs are the durable subscribers the dispatcher runs itself, with
// retry, for durable deliveries.
//
// OnPublishError is told of a failing live publish, of a failing Ack and of a
// panic in the dispatcher itself while it handles an event. It is optional; a
// panic in it is swallowed.
//
// OnDeliveryError is told of every failed durable delivery attempt, of a
// timed-out handler, of a dead-lettered event and of a park after the attempts
// ran out. It is optional; a panic in it is swallowed.
//
// OnPanic is told when a durable handler panics, in addition to the attempt
// failing. It is optional; a panic in it is swallowed.
//
// DeadLetter, when set, may accept an event whose attempts ran out: returning
// nil counts the event as delivered. Otherwise the aggregate is parked. A panic
// in it is swallowed and counts as a refusal. It applies only to requests that
// carry an Ack and are not deletion markers: a dispatch.KindTombstone, or a
// redelivered event named event.ForgetEventName, is never dead-lettered, and running
// out of attempts parks its aggregate when it carries an Ack.
//
// RetryInitial is the delay before the first retry of a failed durable
// delivery; it doubles after every attempt up to RetryMax. Each delay is
// jittered over its upper half.
//
// MaxAttempts caps the attempts of a failing durable delivery.
type Config[T any] struct {
	Bus             pubsub.Bus[T]
	DurableSubs     []subscription.Durable[T]
	OnPublishError  handler.PublishError[T]
	OnDeliveryError handler.DeliveryError[T]
	OnPanic         handler.Panic[T]
	DeadLetter      handler.DeadLetter[T]
	RetryInitial    time.Duration
	RetryMax        time.Duration
	MaxAttempts     int
}
