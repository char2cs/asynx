package delivery

import "github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"

// Deliverer runs a job's durable delivery on behalf of its aggregate's worker.
// It is safe for concurrent use by the workers of different aggregates.
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
		aggregateID string,
		halt <-chan struct{},
		job jobs.Job[T],
	) error

	// ParkThenReport parks the aggregate of a job that carries an Ack, unless
	// halt has been closed since the job was popped (someone else settled the
	// aggregate's fate meanwhile), and only then calls report. The order is
	// what callers depend on: whoever report tells may resume the aggregate at
	// once, and must find it parked. Every give-up path goes through it:
	// attempts exhausted, a failed ack, an interrupt.
	ParkThenReport(
		aggregateID string,
		halt <-chan struct{},
		job jobs.Job[T],
		report func(),
	)
}
