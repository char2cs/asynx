package dispatcher

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// DurableSub is a durable subscriber the dispatcher runs itself, with retry,
// instead of publishing through the bus.
type DurableSub[T any] struct {
	// Name identifies the subscriber in the errors reported for it.
	Name string
	// Match reports whether the subscriber wants an event name.
	Match func(eventName string) bool
	// Handler delivers an event. A non-nil error, a panic, or a timeout then
	// error keeps the delivery pending and retries.
	Handler asynxmd.DurableHandler[T]
	// Timeout, when positive, reports a slow handler as timed out. The handler
	// is still waited for.
	Timeout time.Duration
}

// WithDurableSubs registers the durable subscribers run for jobs enqueued with
// an ack (durable deliveries).
func WithDurableSubs[T any](subs []DurableSub[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.durable = subs }
}

// WithDeliveryErrorHandler sets the callback invoked for each failed durable
// delivery attempt.
func WithDeliveryErrorHandler[T any](fn asynxmd.DeliveryErrorHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.onDeliveryError = fn }
}

// WithDeliveryPanicHandler sets the callback invoked when a durable handler
// panics, in addition to the failure being reported and retried.
func WithDeliveryPanicHandler[T any](fn asynxmd.PanicHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.onPanic = fn }
}

// WithRetryBackoff sets the delay before a failed durable delivery is retried:
// it starts at initial and doubles up to max. Non-positive values are ignored.
func WithRetryBackoff[T any](
	initial time.Duration,
	max time.Duration,
) Opt[T] {
	return func(d *Dispatcher[T]) {
		if initial > 0 {
			d.retryInitial = initial
		}
		if max > 0 {
			d.retryMax = max
		}
	}
}

// WithMaxAttempts caps the attempts of a failing durable delivery. When the
// cap is reached the delivery goes to the dead-letter handler if one is set and
// accepts it; otherwise the aggregate is parked. Non-positive values are ignored.
func WithMaxAttempts[T any](n int) Opt[T] {
	return func(d *Dispatcher[T]) {
		if n > 0 {
			d.maxAttempts = n
		}
	}
}

// WithDeadLetterHandler sets the handler for deliveries that exhausted their
// attempts.
func WithDeadLetterHandler[T any](fn asynxmd.DeadLetterHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.deadLetter = fn }
}

// haltLocked parks an aggregate's durable delivery: queued durable jobs leave
// memory (they stay pending in the store), the retry loop in progress gives up,
// and later durable events are not queued. Callers hold d.mu.
func (d *Dispatcher[T]) haltLocked(agg string) {
	if _, already := d.halted[agg]; already {
		return
	}
	d.parkSeq++
	d.halted[agg] = d.parkSeq

	q, ok := d.queues[agg]
	if !ok {
		return
	}
	close(q.halt)

	kept := make([]*dispatchJob[T], 0, len(q.jobs))
	for _, j := range q.jobs {
		if j.ack == nil {
			kept = append(kept, j)
			continue
		}
		d.markParkedUnrun(j)
		j.complete(asynxmd.ErrDeliveryParked)
	}
	q.jobs = kept
}

// resumeLocked lifts a halt. Callers hold d.mu.
func (d *Dispatcher[T]) resumeLocked(agg string) {
	if _, ok := d.halted[agg]; !ok {
		return
	}
	delete(d.halted, agg)
	d.forgetUnrun(agg)
	if q, ok := d.queues[agg]; ok {
		q.halt = make(chan struct{})
	}
}

// Supersede abandons an aggregate's pending durable deliveries ahead of an
// erase: queued ones leave memory, a retry loop in progress gives up, and the
// aggregate is not left parked. The entries stay in the delivery log for the
// caller to drop. Interrupted handlers may still be finishing.
func (d *Dispatcher[T]) Supersede(agg string) {
	d.mu.Lock()
	d.haltLocked(agg)
	d.resumeLocked(agg)
	d.mu.Unlock()
}

// Halted lists the parked aggregates.
func (d *Dispatcher[T]) Halted() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.halted))
	for agg := range d.halted {
		out = append(out, agg)
	}
	return out
}

// Resume lifts an aggregate's park and reports whether it was parked. The caller
// then redelivers its pending entries, holding the aggregate's command lock
// across both so no live event slips ahead of them; when it was not parked,
// someone else has done that already and redelivering would repeat it.
func (d *Dispatcher[T]) Resume(agg string) bool {
	parked, _ := d.resumeParked(agg, func(uint64) bool { return true })
	return parked
}

// Parks lists the parked aggregates with the generation of each park. A park
// that is lifted and imposed again has a new generation.
func (d *Dispatcher[T]) Parks() map[string]uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.halted)
}

// ResumeUndelivered lifts the park of the given generation and reports whether
// it did; a park that is gone, or has been lifted and imposed again since the
// caller listed it (see Parks), is left alone, because what the caller read
// about it is stale. It also returns, in ascending order, the versions the park
// left undelivered: the events no handler took, which a redelivery must cover
// even when the caller's own scan of the delivery log predates them. Putting
// them back if the redelivery fails is the caller's (see ParkUndelivered),
// because lifting the park forgets them.
func (d *Dispatcher[T]) ResumeUndelivered(
	agg string,
	generation uint64,
) (lifted bool, undelivered []int64) {
	return d.resumeParked(agg, func(g uint64) bool { return g == generation })
}

func (d *Dispatcher[T]) resumeParked(
	agg string,
	wanted func(generation uint64) bool,
) (lifted bool, undelivered []int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	generation, parked := d.halted[agg]
	if !parked || !wanted(generation) {
		return false, nil
	}
	undelivered = d.undelivered(agg)
	d.resumeLocked(agg)
	return true, undelivered
}

func (d *Dispatcher[T]) undelivered(agg string) []int64 {
	d.unrunMu.Lock()
	defer d.unrunMu.Unlock()
	return slices.Sorted(maps.Keys(d.unrun[agg]))
}

// Park halts an aggregate's durable delivery, as exhausting its attempts does.
// Resume undoes it.
func (d *Dispatcher[T]) Park(agg string) {
	d.mu.Lock()
	d.haltLocked(agg)
	d.mu.Unlock()
}

// ParkUndelivered parks the aggregate and remembers that the events at the
// given versions, still pending in the delivery log, have not been handed to a
// handler, so a retry of the command that produced one is told so (see Unrun).
func (d *Dispatcher[T]) ParkUndelivered(
	agg string,
	versions []int64,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.haltLocked(agg)
	d.unrunMu.Lock()
	defer d.unrunMu.Unlock()
	if d.unrun[agg] == nil {
		d.unrun[agg] = make(map[int64]error)
	}
	for _, v := range versions {
		d.unrun[agg][v] = asynxmd.ErrDeliveryParked
	}
}

// Interrupt makes failing durable deliveries give up retrying (leaving them
// pending and parking their aggregates) without closing the dispatcher. Call it
// at the start of shutdown so callers waiting on a retrying event are released.
func (d *Dispatcher[T]) Interrupt() {
	d.quitOnce.Do(func() { close(d.quit) })
}

// Interrupted is closed once Interrupt or Close has been called: from then on
// failing durable deliveries give up instead of retrying.
func (d *Dispatcher[T]) Interrupted() <-chan struct{} { return d.quit }

// deliverDurable runs the durable subscribers until they all succeed. It
// returns nil on success. Otherwise the delivery stays pending and the error
// says why it stopped: ErrDispatcherClosed (interrupted), or ErrDeliveryParked
// (attempts exhausted, or the aggregate was parked meanwhile).
func (d *Dispatcher[T]) deliverDurable(
	agg string,
	q *aggregateQueue[T],
	halt <-chan struct{},
	job *dispatchJob[T],
) error {
	pending := d.subscribersFor(job)

	isAlreadyHalted := job.ack != nil && closed(halt)
	if isAlreadyHalted {
		return asynxmd.ErrDeliveryParked
	}

	backoff := d.retryInitial
	for attempt := 1; len(pending) > 0; attempt++ {
		failed, lastErr := d.attempt(job, pending, attempt)
		if attempt == 1 {
			job.markSettled()
		}
		pending = failed
		if len(pending) == 0 {
			break
		}
		if attempt >= d.maxAttempts {
			return d.giveUp(agg, q, halt, job, lastErr, attempt)
		}

		select {
		case <-d.quit:
			d.parkIfCurrent(agg, q, halt, job)
			return asynxmd.ErrDispatcherClosed
		case <-halt:
			return asynxmd.ErrDeliveryParked
		case <-time.After(jittered(backoff)):
		}
		backoff = min(backoff*2, d.retryMax)
	}
	return nil
}

// jittered spreads a retry delay over its upper half, so deliveries that failed
// together (a downstream outage) do not retry in lockstep.
func jittered(delay time.Duration) time.Duration {
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + rand.N(half+1)
}

// subscribersFor lists the durable subscribers that receive the job: all of
// them for a deletion marker, the matching ones otherwise.
func (d *Dispatcher[T]) subscribersFor(job *dispatchJob[T]) (subs []DurableSub[T]) {
	for _, sub := range d.durable {
		receives := job.forget || sub.Match(job.event.EventName)
		if receives {
			subs = append(subs, sub)
		}
	}
	return subs
}

// attempt runs each pending subscriber once and returns the ones that failed,
// with their errors joined, so the dead-letter handler learns of every one.
func (d *Dispatcher[T]) attempt(
	job *dispatchJob[T],
	pending []DurableSub[T],
	attempt int,
) (failed []DurableSub[T], lastErr error) {
	var errs []error
	for _, sub := range pending {
		err := d.runDurable(job, sub, attempt)
		if err == nil {
			continue
		}
		failure := &asynxmd.SubscriberError{Subscriber: sub.Name, Err: err}
		d.reportDeliveryError(job, failure, attempt)
		failed = append(failed, sub)
		errs = append(errs, failure)
	}
	return failed, errors.Join(errs...)
}

// giveUp ends a delivery whose attempts ran out. A dead-letter handler may
// accept the event, which then counts as delivered. Otherwise the aggregate is
// parked so its later events do not overtake this one.
func (d *Dispatcher[T]) giveUp(
	agg string,
	q *aggregateQueue[T],
	halt <-chan struct{},
	job *dispatchJob[T],
	last error,
	attempt int,
) error {
	select {
	case <-halt:
		return asynxmd.ErrDeliveryParked
	default:
	}
	if job.ack != nil && !job.forget && d.deadLetter != nil {
		err := d.callDeadLetter(job, last)
		if err == nil {
			d.reportDeliveryError(job, fmt.Errorf("dead-lettered after %d attempts: %w", attempt, last), attempt)
			return nil
		}
		d.reportDeliveryError(job, fmt.Errorf("dead-letter handler: %w", err), attempt)
	}

	parked := fmt.Errorf("%w after %d attempts: %w", asynxmd.ErrDeliveryParked, attempt, last)
	d.reportDeliveryError(job, parked, attempt)
	d.parkIfCurrent(agg, q, halt, job)
	return parked
}

// parkIfCurrent parks the aggregate of a durable job, unless the halt it started
// under has been closed since: then someone else has already settled the
// aggregate's fate (a Forget superseded the delivery, a Resume lifted the park).
func (d *Dispatcher[T]) parkIfCurrent(
	agg string,
	q *aggregateQueue[T],
	halt <-chan struct{},
	job *dispatchJob[T],
) {
	if job.ack == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	select {
	case <-halt:
		return
	default:
	}
	d.haltLocked(agg)
}

func (d *Dispatcher[T]) callDeadLetter(
	job *dispatchJob[T],
	last error,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", asynxmd.ErrHandlerPanic, r)
		}
	}()
	return d.deadLetter(job.ctx, job.event, last)
}

// reportDeliveryError calls the delivery error handler. A panic in it is
// swallowed: it must not abandon the delivery, which would release the hold on
// the aggregate's later events.
func (d *Dispatcher[T]) reportDeliveryError(
	job *dispatchJob[T],
	err error,
	attempt int,
) {
	if d.onDeliveryError == nil {
		return
	}
	defer func() { _ = recover() }()
	d.onDeliveryError(job.ctx, job.event, err, attempt)
}

// notifyPanic calls the panic handler, whose own panic is swallowed.
func (d *Dispatcher[T]) notifyPanic(
	job *dispatchJob[T],
	recovered any,
) {
	if d.onPanic == nil {
		return
	}
	defer func() { _ = recover() }()
	d.onPanic(job.ctx, job.event, recovered)
}

func (d *Dispatcher[T]) callRecovering(
	job *dispatchJob[T],
	sub DurableSub[T],
) (err error) {
	defer func() { d.errorFromPanic(job, recover(), &err) }()
	return sub.Handler(job.ctx, job.event)
}

func (d *Dispatcher[T]) errorFromPanic(
	job *dispatchJob[T],
	recovered any,
	err *error,
) {
	if recovered == nil {
		return
	}
	d.notifyPanic(job, recovered)
	*err = fmt.Errorf("%w: %v", asynxmd.ErrHandlerPanic, recovered)
}

// runDurable runs one durable handler, turning a panic into an error. A slow
// handler is reported once as timed out and still waited for.
func (d *Dispatcher[T]) runDurable(
	job *dispatchJob[T],
	sub DurableSub[T],
	attempt int,
) error {
	call := func() error { return d.callRecovering(job, sub) }

	if sub.Timeout <= 0 {
		return call()
	}

	result := make(chan error, 1)
	go func() { result <- call() }()

	timer := time.NewTimer(sub.Timeout)
	defer timer.Stop()

	select {
	case err := <-result:
		return err
	case <-timer.C:
		timedOut := &asynxmd.SubscriberError{Subscriber: sub.Name, Err: fmt.Errorf("%w after %v", asynxmd.ErrHandlerTimeout, sub.Timeout)}
		d.reportDeliveryError(job, timedOut, attempt)
		return <-result
	}
}
