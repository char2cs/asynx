// Package delivery runs the durable half of a job: the attempt loop that
// hands the event to every durable subscriber that receives it, retrying the
// failed ones with a jittered, doubling backoff until they all succeed, the
// attempt cap is reached, the aggregate is parked, or the dispatcher is
// interrupted.
//
// The package has no lock of its own. It changes an aggregate's state only by
// parking it, and only through the registry's ParkIfCurrent, so a park that
// someone else imposed or lifted meanwhile is never undone. Every path that
// parks and then tells someone (attempts exhausted, a failed ack, an
// interrupt) goes through Deliverer.ParkThenReport, which parks first: whoever
// is told may act on the park at once.
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
