package workers

import (
	"context"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

// Pool runs the deliveries of jobs, one goroutine per job, which the dispatcher
// starts and which take their turn on their aggregate.
//
// A run handles its job as follows. A job that carries an Ack and
// whose aggregate is parked, or was parked since the job began, is
// settled unrun and ends with ErrDeliveryParked without running anything.
// Otherwise the event is published to the live subscribers, unless the job
// skips them; a bus error, or a panic in the bus, is reported to the publish
// error handler and does not skip the durable delivery. A durable job then
// runs its durable delivery, and a job that carries an Ack is acknowledged
// once every durable subscriber has succeeded. A failed ack parks the
// aggregate before the error is reported to the publish error handler, so the
// aggregate's next event cannot overtake this one. A panic in the dispatcher
// itself never kills the run: it is reported to the publish error handler
// and ends the job with that error. Whatever happens, the job is settled and
// completed when the run is done with it, and the aggregate's next job is
// then let in.
type Pool[T any] interface {
	// Run delivers job. It blocks until turn is closed, which is when the
	// aggregate's previous job has finished, then begins job in the registry:
	// a job a park dropped meanwhile has been settled and completed by the
	// park and runs nothing. ctx is the context the job's handlers run with;
	// it is passed down the call chain and kept nowhere. Run closes done when
	// it is over, even after a panic, and the dispatcher counts it for Close.
	Run(
		ctx context.Context,
		job contracts.Job[T],
		turn <-chan struct{},
		done chan<- struct{},
	)
}
