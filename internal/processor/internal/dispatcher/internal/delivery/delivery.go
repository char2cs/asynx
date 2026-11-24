package delivery

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/handler"
	"github.com/char2cs/asynx/models/subscription"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/reporter"
)

// New creates a Deliverer for the durable subscribers, dead-letter handler and
// retry settings in cfg, whose values it takes as final (the caller applies
// the defaults). It parks aggregates through registry, reports failures and
// panics through reports, and stops retrying once interrupted is closed.
func New[T any](
	cfg dispatcher.Config[T],
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

type durableDeliverer[T any] struct {
	subs         []subscription.Durable[T]
	deadLetter   handler.DeadLetter[T]
	retryInitial time.Duration
	retryMax     time.Duration
	maxAttempts  int
	registry     aggregates.Registry[jobs.Job[T]]
	reports      reporter.Reporter[T]
	interrupted  <-chan struct{}
}

// Deliver runs job's durable delivery with retries, as documented on
// Deliverer.Deliver: the subscribers that receive the event are tried one
// after the other, the failed ones are retried after a jittered, doubling
// backoff, and the job settles once the first attempt is over. The returned
// error says why a delivery that did not succeed stopped.
func (d *durableDeliverer[T]) Deliver(
	ctx context.Context,
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) error {
	pending := d.subscribersFor(job)
	if job.Acked() &&
		isClosed(halt) {
		return failure.ErrDeliveryParked
	}

	if len(pending) == 0 {
		return nil
	}

	loop := &retryLoop[T]{
		deliverer:   d,
		aggregateID: aggregateID,
		halt:        halt,
		job:         job,
		pending:     pending,
		attempt:     1,
		backoff:     d.retryInitial,
	}
	return loop.run(ctx)
}

// Park parks the aggregate of an acked job through the registry's
// ParkIfCurrent, as documented on Deliverer.Park. A job without an Ack never
// parks its aggregate, and a halt channel that was closed since the job began
// leaves whatever park or supersede closed it untouched.
func (d *durableDeliverer[T]) Park(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
	cause error,
) {
	if !job.Acked() {
		return
	}

	d.registry.ParkIfCurrent(
		aggregateID,
		halt,
		cause,
	)
}

func (d *durableDeliverer[T]) subscribersFor(
	job jobs.Job[T],
) []subscription.Durable[T] {
	return slices.DeleteFunc(
		slices.Clone(d.subs),
		notReceiving(job),
	)
}

func (d *durableDeliverer[T]) wait(
	ctx context.Context,
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
	backoff time.Duration,
) error {
	timer := time.NewTimer(jittered(backoff))
	defer timer.Stop()
	select {
	case <-d.interrupted:
		d.Park(
			aggregateID,
			halt,
			job,
			failure.ErrDispatcherClosed,
		)
		return failure.ErrDispatcherClosed
	case <-halt:
		return failure.ErrDeliveryParked
	case <-timer.C:
		return backoffEnded(
			job,
			halt,
		)
	}
}

func (d *durableDeliverer[T]) giveUp(
	ctx context.Context,
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
	last error,
	attempt int,
) error {
	if isClosed(halt) {
		return failure.ErrDeliveryParked
	}

	if d.deadLettered(
		ctx,
		job,
		last,
		attempt,
	) {
		return nil
	}

	parked := fmt.Errorf(
		"%w after %d attempts: %w",
		failure.ErrDeliveryParked,
		attempt,
		last,
	)
	d.Park(
		aggregateID,
		halt,
		job,
		parked,
	)
	d.reports.DeliveryError(
		ctx,
		job,
		parked,
		attempt,
	)
	return parked
}

func (d *durableDeliverer[T]) deadLettered(
	ctx context.Context,
	job jobs.Job[T],
	last error,
	attempt int,
) bool {
	if !d.deadLetters(job) {
		return false
	}

	err := d.callDeadLetter(
		ctx,
		job,
		last,
	)
	if err != nil {
		d.reports.DeliveryError(
			ctx,
			job,
			fmt.Errorf(
				"dead-letter handler: %w",
				err,
			),
			attempt,
		)
		return false
	}

	d.reports.DeliveryError(
		ctx,
		job,
		fmt.Errorf(
			"dead-lettered after %d attempts: %w",
			attempt,
			last,
		),
		attempt,
	)
	return true
}

func (d *durableDeliverer[T]) deadLetters(
	job jobs.Job[T],
) bool {
	if d.deadLetter == nil {
		return false
	}

	return job.Acked() && !job.Forget()
}

func (d *durableDeliverer[T]) callDeadLetter(
	ctx context.Context,
	job jobs.Job[T],
	last error,
) (err error) {
	defer recoverAsError(&err)
	return d.deadLetter(
		ctx,
		job.Event(),
		last,
	)
}

func backoffEnded[T any](
	job jobs.Job[T],
	halt <-chan struct{},
) error {
	if job.Acked() &&
		isClosed(halt) {
		return failure.ErrDeliveryParked
	}

	return nil
}

func notReceiving[T any](
	job jobs.Job[T],
) func(subscription.Durable[T]) bool {
	return func(
		sub subscription.Durable[T],
	) bool {
		return !job.Forget() &&
			!sub.Match(job.Event().EventName)
	}
}

func recoverAsError(
	err *error,
) {
	recovered := recover()
	if recovered == nil {
		return
	}

	*err = fmt.Errorf(
		"%w: %v",
		failure.ErrHandlerPanic,
		recovered,
	)
}
