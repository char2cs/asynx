package dispatch

import (
	"context"

	"github.com/char2cs/asynx/v2/models/event"
)

// Request is one event handed to a dispatcher.Dispatcher for ordered delivery.
//
// Event is the committed event; its AggregateID selects the per-aggregate
// order it is delivered in.
//
// Ack, when nil on a KindEvent, makes the request live only: no durable
// subscriber runs. A KindRedelivery of an ordinary event never reaches live
// subscribers, so with a nil Ack nothing runs at all. When non-nil
// the delivery is durable: the durable subscribers that receive the event run
// with retry, holding back the aggregate's later events, and Ack runs exactly
// once after every one of them has succeeded. Ack normally acknowledges the
// event's entry in the delivery log; a failing Ack parks the aggregate. Only a
// request carrying an Ack is held out of the queue while its aggregate is
// parked, can be dead-lettered (unless it is a deletion marker), and parks its
// aggregate when its attempts run out or it is interrupted.
//
// A deletion marker (a KindTombstone, or a KindRedelivery of an event
// named event.ForgetEventName) runs every durable subscriber, with retry, even with a
// nil Ack; then no ack runs, the marker is queued and run whatever the park
// state, and a failure ends its ticket with an error without parking the
// aggregate. A deletion marker is never dead-lettered.
//
// Kind says what the event is; see Kind.
type Request[T any] struct {
	Event event.Event[T]
	Ack   func(context.Context) error
	Kind  Kind
}
