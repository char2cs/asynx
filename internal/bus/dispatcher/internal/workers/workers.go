// Package workers runs the dispatcher's per-aggregate goroutines: one worker
// for every aggregate with jobs queued, which hands them, one at a time and in
// queue order, to the live subscribers (the bus) and then to the durable
// delivery, and acknowledges each durable job once its subscribers have all
// succeeded. Different aggregates' workers run concurrently.
//
// A worker drains its aggregate's queue, then waits for more work: it loops on
// a wake signal, drains what is left and exits when the pool is stopped, and
// retires after the idle timeout unless a job arrived meanwhile (the registry
// decides that atomically with the enqueue, so no job is ever stranded on a
// retired worker). The package has no lock of its own: the queue, the halt
// channel and the park all live in the registry.
package workers

import (
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/delivery"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/reporter"
)

// New creates a Pool whose workers pop jobs from registry, publish them
// through bus (nil publishes nothing), run their durable delivery through
// deliverer and report errors through reports. A worker with nothing to do
// for idleTimeout retires.
func New[T any](
	bus asynxmd.Bus[T],
	registry aggregates.Registry[jobs.Job[T]],
	deliverer delivery.Deliverer[T],
	reports reporter.Reporter[T],
	idleTimeout time.Duration,
) Pool[T] {
	return &aggregatePool[T]{
		bus:         bus,
		registry:    registry,
		deliverer:   deliverer,
		reports:     reports,
		idleTimeout: idleTimeout,
		stop:        make(chan struct{}),
	}
}
