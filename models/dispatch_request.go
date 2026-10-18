package models

import "context"

// DispatchRequest is one event handed to a Dispatcher for ordered delivery.
//
// Event is the committed event; its AggregateID selects the per-aggregate
// order it is delivered in.
//
// Ack, when nil, makes the request live only: no durable subscriber runs for a
// DispatchEvent. When non-nil the delivery is durable: the durable subscribers
// that receive the event run with retry, holding back the aggregate's later
// events, and Ack runs exactly once after every one of them has succeeded. Ack
// normally acknowledges the event's entry in the delivery log; a failing Ack
// parks the aggregate. A DispatchTombstone, or a DispatchRedelivery of an event
// named ForgetEventName, runs every durable subscriber even with a nil Ack;
// then no ack runs.
//
// Kind says what the event is; see DispatchKind.
type DispatchRequest[T any] struct {
	Event Event[T]
	Ack   func(context.Context) error
	Kind  DispatchKind
}
