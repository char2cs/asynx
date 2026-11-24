package delivery

import (
	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/reporter"
)

// New creates a Deliverer for the durable subscribers, dead-letter handler and
// retry settings in cfg, whose values it takes as final (the caller applies
// the defaults). It parks aggregates through registry, reports failures and
// panics through reports, and stops retrying once interrupted is closed.
func New[T any](
	cfg asynxmd.DispatcherConfig[T],
	registry aggregates.Registry[jobs.Job[T]],
	reports reporter.Reporter[T],
	interrupted <-chan struct{},
) Deliverer[T] {
	return &durableDeliverer[T]{
		subs:         cfg.DurableSubs,
		deadLetter:   cfg.DeadLetter,
		retryInitial: cfg.RetryInitial,
		retryMax:     cfg.RetryMax,
		maxAttempts:  cfg.MaxAttempts,
		registry:     registry,
		reports:      reports,
		interrupted:  interrupted,
	}
}
