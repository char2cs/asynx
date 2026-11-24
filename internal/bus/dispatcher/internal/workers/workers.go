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
