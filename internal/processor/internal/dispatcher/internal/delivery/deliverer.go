package delivery

import (
	"context"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

// Deliverer runs a job's durable delivery on behalf of the delivery goroutine
// of its aggregate's turn. It is safe for concurrent use by the deliveries of
// different aggregates.
//
// A Deliverer has no lock of its own. It changes an aggregate's state only by
// parking it, and only through the registry's ParkIfCurrent, so a park that
// someone else imposed or lifted meanwhile is never undone. Every path that
// parks and then tells someone (attempts exhausted, a failed ack, an
// interrupt) parks first and reports afterwards: whoever is told may act on
// the park at once, for instance by resuming the aggregate, and must find it
// parked already.
type Deliverer[T any] interface {
	// Deliver hands job's event to every durable subscriber that receives it
	// (all of them for a deletion marker, the matching ones otherwise) and
	// retries the ones that failed, with a jittered backoff doubling from the
	// initial delay up to the maximum, until they all succeed. halt is the
	// channel the registry popped the job with.
	//
	// Each attempt runs the pending subscribers one after the other. A panic
	// in a handler is reported to the panic handler and counts as a failure
	// (ErrHandlerPanic); every failure is reported to the delivery error
	// handler wrapped in a SubscriberError naming its subscriber. A handler
	// that outlives its Timeout is reported once as timed out and still
	// waited for. The job settles after the first attempt, whatever its
	// outcome.
	//
	// It returns nil once every subscriber has succeeded, or when a
	// dead-letter handler accepts the event after the attempts ran out.
	// Otherwise the delivery stays pending in the store and the error says
	// why it stopped: ErrDeliveryParked when the attempts ran out (the
	// aggregate of a job that carries an Ack is then parked, unless halt was
	// closed meanwhile) or the aggregate was parked or superseded while it
	// ran, ErrDispatcherClosed when the dispatcher was interrupted (parking
	// the aggregate likewise). A job that carries an Ack and whose halt is
	// already closed returns ErrDeliveryParked without running anything.
	Deliver(
		ctx context.Context,
		aggregateID string,
		halt <-chan struct{},
		job contracts.Job[T],
	) error

	// Park parks the aggregate of a job that carries an Ack, with cause as the
	// reason Inspect then reports, unless halt has been closed since the job
	// was popped (someone else settled the aggregate's fate meanwhile). A job
	// without an Ack never parks its aggregate. Every give-up path goes
	// through it: attempts exhausted (ErrDeliveryParked wrapping the last
	// handler error and the attempt count), a failed ack (the ack error) and
	// an interrupt (ErrDispatcherClosed).
	//
	// A caller that then tells someone about the give-up, through a publish
	// or delivery error handler, calls Park first and reports afterwards: the
	// order is what keeps the aggregate parked by the time the recipient acts.
	Park(
		aggregateID string,
		halt <-chan struct{},
		job contracts.Job[T],
		cause error,
	)
}
