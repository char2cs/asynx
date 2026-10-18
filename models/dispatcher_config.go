package models

import "time"

// DispatcherConfig is what a Dispatcher is built from. It carries final
// values: whoever builds it applies the defaults (20 attempts, a backoff of
// 100ms doubling up to 30s) and drops non-positive settings, so an
// implementation never needs to know them.
//
// Bus is the live-subscriber transport: DispatchEvent and DispatchTombstone
// requests are published through Bus.PublishSync. It may be nil, then no live
// subscriber is reached.
//
// DurableSubs are the durable subscribers the dispatcher runs itself, with
// retry, for durable deliveries.
//
// OnPublishError is told of a failing live publish and of a failing Ack. It is
// optional; a panic in it is swallowed.
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
// in it is swallowed and counts as a refusal.
//
// RetryInitial is the delay before the first retry of a failed durable
// delivery; it doubles after every attempt up to RetryMax. Each delay is
// jittered over its upper half.
//
// MaxAttempts caps the attempts of a failing durable delivery.
type DispatcherConfig[T any] struct {
	Bus             Bus[T]
	DurableSubs     []DurableSub[T]
	OnPublishError  PublishErrorHandler[T]
	OnDeliveryError DeliveryErrorHandler[T]
	OnPanic         PanicHandler[T]
	DeadLetter      DeadLetterHandler[T]
	RetryInitial    time.Duration
	RetryMax        time.Duration
	MaxAttempts     int
}
