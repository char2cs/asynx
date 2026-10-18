package delivery

import (
	"errors"
	"fmt"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/reporter"
)

type durableDeliverer[T any] struct {
	subs         []asynxmd.DurableSub[T]
	deadLetter   asynxmd.DeadLetterHandler[T]
	retryInitial time.Duration
	retryMax     time.Duration
	maxAttempts  int
	registry     aggregates.Registry[jobs.Job[T]]
	reports      reporter.Reporter[T]
	interrupted  <-chan struct{}
}

func (d *durableDeliverer[T]) Deliver(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) error {
	pending := d.subscribersFor(job)
	if job.Acked() && isClosed(halt) {
		return asynxmd.ErrDeliveryParked
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
	for loop.step() {
	}
	return loop.err
}

func (d *durableDeliverer[T]) ParkThenReport(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
	report func(),
) {
	d.registry.ParkIfCurrent(aggregateID, halt, job.Acked())
	report()
}

func (d *durableDeliverer[T]) subscribersFor(
	job jobs.Job[T],
) []asynxmd.DurableSub[T] {
	var subs []asynxmd.DurableSub[T]
	for _, sub := range d.subs {
		subs = appendReceiver(subs, sub, job)
	}
	return subs
}

func (d *durableDeliverer[T]) attempt(
	job jobs.Job[T],
	pending []asynxmd.DurableSub[T],
	attempt int,
) ([]asynxmd.DurableSub[T], error) {
	var failed []asynxmd.DurableSub[T]
	var errs []error
	for _, sub := range pending {
		failed, errs = d.try(job, sub, attempt, failed, errs)
	}
	return failed, errors.Join(errs...)
}

func (d *durableDeliverer[T]) try(
	job jobs.Job[T],
	sub asynxmd.DurableSub[T],
	attempt int,
	failed []asynxmd.DurableSub[T],
	errs []error,
) ([]asynxmd.DurableSub[T], []error) {
	err := d.runDurable(job, sub, attempt)
	if err == nil {
		return failed, errs
	}
	failure := &asynxmd.SubscriberError{Subscriber: sub.Name, Err: err}
	d.reports.DeliveryError(job, failure, attempt)
	return append(failed, sub), append(errs, failure)
}

func (d *durableDeliverer[T]) runDurable(
	job jobs.Job[T],
	sub asynxmd.DurableSub[T],
	attempt int,
) error {
	if sub.Timeout <= 0 {
		return d.callRecovering(job, sub)
	}
	result := make(chan error, 1)
	go d.callInto(result, job, sub)
	timer := time.NewTimer(sub.Timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		d.reportTimeout(job, sub, attempt)
		return <-result
	}
}

func (d *durableDeliverer[T]) reportTimeout(
	job jobs.Job[T],
	sub asynxmd.DurableSub[T],
	attempt int,
) {
	timedOut := &asynxmd.SubscriberError{
		Subscriber: sub.Name,
		Err:        fmt.Errorf("%w after %v", asynxmd.ErrHandlerTimeout, sub.Timeout),
	}
	d.reports.DeliveryError(job, timedOut, attempt)
}

func (d *durableDeliverer[T]) callInto(
	result chan<- error,
	job jobs.Job[T],
	sub asynxmd.DurableSub[T],
) {
	result <- d.callRecovering(job, sub)
}

func (d *durableDeliverer[T]) callRecovering(
	job jobs.Job[T],
	sub asynxmd.DurableSub[T],
) (err error) {
	defer d.recoverHandlerPanic(job, &err)
	return sub.Handler(job.Context(), job.Event())
}

func (d *durableDeliverer[T]) recoverHandlerPanic(
	job jobs.Job[T],
	err *error,
) {
	recovered := recover()
	if recovered == nil {
		return
	}
	d.reports.Panic(job, recovered)
	*err = fmt.Errorf("%w: %v", asynxmd.ErrHandlerPanic, recovered)
}

func (d *durableDeliverer[T]) wait(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
	backoff time.Duration,
) error {
	timer := time.NewTimer(jittered(backoff))
	defer timer.Stop()
	select {
	case <-d.interrupted:
		d.ParkThenReport(aggregateID, halt, job, func() {})
		return asynxmd.ErrDispatcherClosed
	case <-halt:
		return asynxmd.ErrDeliveryParked
	case <-timer.C:
		return nil
	}
}

func (d *durableDeliverer[T]) giveUp(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
	last error,
	attempt int,
) error {
	if isClosed(halt) {
		return asynxmd.ErrDeliveryParked
	}
	if d.deadLettered(job, last, attempt) {
		return nil
	}
	parked := fmt.Errorf("%w after %d attempts: %w", asynxmd.ErrDeliveryParked, attempt, last)
	d.ParkThenReport(aggregateID, halt, job, func() {
		d.reports.DeliveryError(job, parked, attempt)
	})
	return parked
}

func (d *durableDeliverer[T]) deadLettered(
	job jobs.Job[T],
	last error,
	attempt int,
) bool {
	if !d.deadLetters(job) {
		return false
	}
	err := d.callDeadLetter(job, last)
	if err != nil {
		d.reports.DeliveryError(job, fmt.Errorf("dead-letter handler: %w", err), attempt)
		return false
	}
	d.reports.DeliveryError(job, fmt.Errorf("dead-lettered after %d attempts: %w", attempt, last), attempt)
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
	job jobs.Job[T],
	last error,
) (err error) {
	defer recoverAsError(&err)
	return d.deadLetter(job.Context(), job.Event(), last)
}

func appendReceiver[T any](
	subs []asynxmd.DurableSub[T],
	sub asynxmd.DurableSub[T],
	job jobs.Job[T],
) []asynxmd.DurableSub[T] {
	if !job.Forget() && !sub.Match(job.Event().EventName) {
		return subs
	}
	return append(subs, sub)
}

func recoverAsError(
	err *error,
) {
	recovered := recover()
	if recovered == nil {
		return
	}
	*err = fmt.Errorf("%w: %v", asynxmd.ErrHandlerPanic, recovered)
}
