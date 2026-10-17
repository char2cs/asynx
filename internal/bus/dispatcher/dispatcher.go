package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

const (
	defaultIdleTimeout  = 5 * time.Second
	defaultRetryInitial = 100 * time.Millisecond
	defaultRetryMax     = 30 * time.Second

	// defaultMaxAttempts caps a failing durable delivery: with the default
	// backoff that is about six minutes of retrying.
	defaultMaxAttempts = 20
	// defaultMaxQueued bounds the jobs held in memory per aggregate.
	defaultMaxQueued = 10_000
)

// Opt is a functional option for configuring a Dispatcher.
type Opt[T any] func(*Dispatcher[T])

// WithPublishErrorHandler sets the callback invoked when bus.PublishSync
// returns a non-nil error. When nil (the default), errors are silently dropped.
func WithPublishErrorHandler[T any](fn asynxmd.PublishErrorHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.onPublishError = fn }
}

// DurableSub is a durable subscriber the dispatcher runs itself, with retry,
// instead of publishing through the bus.
type DurableSub[T any] struct {
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
func WithRetryBackoff[T any](initial, max time.Duration) Opt[T] {
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

// WithMaxQueued bounds how many jobs the dispatcher holds in memory per
// aggregate. A durable event arriving beyond it parks the aggregate: the event
// is already in the store's delivery log, so nothing is lost, and
// Dispatcher.Resume plus a redelivery brings it back. Non-positive values are
// ignored.
func WithMaxQueued[T any](n int) Opt[T] {
	return func(d *Dispatcher[T]) {
		if n > 0 {
			d.maxQueued = n
		}
	}
}

// WithDeadLetterHandler sets the handler for deliveries that exhausted their
// attempts.
func WithDeadLetterHandler[T any](fn asynxmd.DeadLetterHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.deadLetter = fn }
}

// WithIdleTimeout sets how long a per-aggregate worker goroutine waits for new
// events before cleaning itself up. Zero or negative values are ignored.
func WithIdleTimeout[T any](t time.Duration) Opt[T] {
	return func(d *Dispatcher[T]) {
		if t > 0 {
			d.idleTimeout = t
		}
	}
}

// Dispatcher provides per-aggregate ordered event delivery by maintaining a
// FIFO channel and dedicated worker goroutine for each aggregate that has
// in-flight events. Events for the same aggregate are delivered sequentially
// via bus.PublishSync; different aggregates are delivered concurrently.
type Dispatcher[T any] struct {
	bus            asynxmd.Bus[T]
	mu             sync.Mutex
	queues         map[string]*aggregateQueue[T]
	closed         bool
	waiting        bool          // prevents new Dispatch calls during WaitIdle
	stopCh         chan struct{} // closed on Close to signal workers to drain and exit
	quit           chan struct{} // closed by Interrupt/Close: retry loops give up
	quitOnce       sync.Once
	halted         map[string]struct{} // aggregates whose durable delivery is parked
	wg             sync.WaitGroup      // tracks goroutine lifetime (for Close)
	jobsWg         sync.WaitGroup      // tracks in-flight jobs (for WaitIdle)
	onPublishError asynxmd.PublishErrorHandler[T]
	idleTimeout    time.Duration

	durable         []DurableSub[T]
	onDeliveryError asynxmd.DeliveryErrorHandler[T]
	onPanic         asynxmd.PanicHandler[T]
	retryInitial    time.Duration
	retryMax        time.Duration
	maxAttempts     int
	maxQueued       int
	deadLetter      asynxmd.DeadLetterHandler[T]
}

// aggregateQueue is one aggregate's FIFO of jobs. Enqueue never blocks, because
// callers enqueue while holding the aggregate's command lock, and a handler
// that sends a command back to its own aggregate would otherwise deadlock
// against a full queue. Memory is bounded instead by parking (see WithMaxQueued).
type aggregateQueue[T any] struct {
	jobs []*dispatchJob[T] // protected by Dispatcher.mu
	wake chan struct{}     // capacity 1: signals the worker that jobs is non-empty
	// halt is closed when the aggregate is halted, interrupting a retry loop in
	// progress. Protected by Dispatcher.mu; replaced when the halt is lifted.
	halt chan struct{}
}

// Ticket tracks one enqueued event.
type Ticket struct {
	// Settled closes once every handler has run at least once. A failing
	// durable handler keeps retrying after it closes.
	Settled <-chan struct{}
	// Final yields exactly one value when the event's delivery is over: nil if
	// every handler succeeded (and the ack ran), else why it ended without that:
	// ErrDeliveryParked, ErrDispatcherClosed, or an ack failure.
	Final <-chan error
}

type dispatchJob[T any] struct {
	event asynxmd.Event[T]
	ctx   context.Context
	// done closes once the event has been handled at least once.
	done   chan struct{}
	settle sync.Once
	final  chan error // capacity 1
	finish sync.Once
	// ack, when non-nil, marks a durable delivery: durable subscribers run and
	// ack is called once they have all succeeded.
	ack func(context.Context) error
	// forget marks the tombstone: every durable subscriber receives it, once,
	// without a delivery entry, and Final reports whether they all succeeded.
	forget bool
}

func (j *dispatchJob[T]) markSettled() { j.settle.Do(func() { close(j.done) }) }

func (j *dispatchJob[T]) complete(err error) {
	j.finish.Do(func() { j.final <- err })
}

// durable reports whether the job runs durable subscribers.
func (j *dispatchJob[T]) durable() bool { return j.ack != nil || j.forget }

// New creates a Dispatcher that publishes events through bus.
func New[T any](bus asynxmd.Bus[T], opts ...Opt[T]) *Dispatcher[T] {
	d := &Dispatcher[T]{
		bus:          bus,
		queues:       make(map[string]*aggregateQueue[T]),
		stopCh:       make(chan struct{}),
		quit:         make(chan struct{}),
		halted:       make(map[string]struct{}),
		maxAttempts:  defaultMaxAttempts,
		maxQueued:    defaultMaxQueued,
		idleTimeout:  defaultIdleTimeout,
		retryInitial: defaultRetryInitial,
		retryMax:     defaultRetryMax,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Dispatch enqueues an event for ordered delivery. The enqueue itself is
// synchronous (establishes ordering). If waitHandlers is true the call blocks
// until the event's handlers have completed.
func (d *Dispatcher[T]) Dispatch(ctx context.Context, event asynxmd.Event[T], waitHandlers bool) error {
	done, err := d.Enqueue(ctx, event, nil)
	if err != nil {
		return err
	}
	if waitHandlers {
		<-done
	}
	return nil
}

// Enqueue puts an event on its aggregate's queue and returns a channel that is
// closed once every handler for the event has been run at least once. Enqueue order is
// delivery order, so callers that must keep versions ordered enqueue while
// holding their per-aggregate lock and wait on the channel afterwards.
//
// ack, when non-nil, marks a durable delivery: the durable subscribers matching
// the event run (with retry, holding back the aggregate's later events until
// they succeed) and ack runs once they all have. An ack error is reported to the
// publish error handler. A failing live subscriber (bus error) does not skip
// the durable delivery.
//
// A durable event for a parked aggregate is not queued: it stays pending in the
// store's delivery log and its ticket ends with ErrDeliveryParked.
func (d *Dispatcher[T]) Enqueue(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	t, err := d.enqueue(ctx, event, ack, false, false)
	return t.Settled, err
}

// EnqueueTicket is Enqueue returning the full Ticket.
func (d *Dispatcher[T]) EnqueueTicket(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (Ticket, error) {
	return d.enqueue(ctx, event, ack, false, false)
}

// EnqueueRecovered enqueues a redelivered event. It is exempt from the
// per-aggregate memory bound, so a long backlog can be recovered whole.
func (d *Dispatcher[T]) EnqueueRecovered(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (Ticket, error) {
	return d.enqueue(ctx, event, ack, false, true)
}

// EnqueueForget enqueues a tombstone. Every durable subscriber receives it
// (whatever its pattern) before the ticket's Final yields nil.
func (d *Dispatcher[T]) EnqueueForget(ctx context.Context, tombstone asynxmd.Event[T]) (Ticket, error) {
	return d.enqueue(ctx, tombstone, nil, true, false)
}

func (d *Dispatcher[T]) enqueue(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
	forget, exempt bool,
) (Ticket, error) {
	d.mu.Lock()
	if d.closed || d.waiting {
		d.mu.Unlock()
		return Ticket{}, asynxmd.ErrDispatcherClosed
	}
	// Track this dispatch call immediately after checking not-waiting/not-closed.
	// This ensures WaitIdle() cannot return with jobsWg==0 while a goroutine
	// is still inside this function (potentially accessing d.mu/d.queues).
	d.jobsWg.Add(1)
	d.mu.Unlock()

	job := &dispatchJob[T]{
		event:  event,
		ctx:    context.WithoutCancel(ctx),
		done:   make(chan struct{}),
		final:  make(chan error, 1),
		ack:    ack,
		forget: forget,
	}
	ticket := Ticket{Settled: job.done, Final: job.final}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		// Undo the Add(1) since we're not enqueueing a job.
		d.jobsWg.Done()
		return Ticket{}, asynxmd.ErrDispatcherClosed
	}

	agg := event.AggregateID
	q, exists := d.queues[agg]

	if ack != nil {
		_, parked := d.halted[agg]
		overflow := !exempt && exists && len(q.jobs) >= d.maxQueued
		if parked || overflow {
			if overflow {
				d.haltLocked(agg)
			}
			d.mu.Unlock()
			if overflow {
				d.reportDeliveryError(job, fmt.Errorf(
					"%w: %d events queued behind a failing delivery", asynxmd.ErrDeliveryParked, d.maxQueued), 0)
			}
			// The event stays pending in the delivery log.
			job.markSettled()
			job.complete(asynxmd.ErrDeliveryParked)
			d.jobsWg.Done()
			return ticket, nil
		}
	}

	if !exists {
		q = &aggregateQueue[T]{wake: make(chan struct{}, 1), halt: make(chan struct{})}
		d.queues[agg] = q
		d.wg.Add(1)
		go d.worker(agg, q)
	}

	// Appending under d.mu makes "queue is empty" exact for the idle worker, so
	// it can never retire a queue that has a job in flight.
	q.jobs = append(q.jobs, job)
	select {
	case q.wake <- struct{}{}:
	default:
	}
	d.mu.Unlock()

	return ticket, nil
}

// haltLocked parks an aggregate's durable delivery: queued durable jobs leave
// memory (they stay pending in the store), the retry loop in progress gives up,
// and later durable events are not queued. Callers hold d.mu.
func (d *Dispatcher[T]) haltLocked(agg string) {
	if _, already := d.halted[agg]; already {
		return
	}
	d.halted[agg] = struct{}{}

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
		j.markSettled()
		j.complete(asynxmd.ErrDeliveryParked)
		d.jobsWg.Done()
	}
	q.jobs = kept
}

// resumeLocked lifts a halt. Callers hold d.mu.
func (d *Dispatcher[T]) resumeLocked(agg string) {
	if _, ok := d.halted[agg]; !ok {
		return
	}
	delete(d.halted, agg)
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

// Resume lifts an aggregate's park. The caller then redelivers its pending
// entries, holding the aggregate's command lock across both so no live event
// slips ahead of them.
func (d *Dispatcher[T]) Resume(agg string) {
	d.mu.Lock()
	d.resumeLocked(agg)
	d.mu.Unlock()
}

// Park halts an aggregate's durable delivery, as exhausting its attempts does.
// Resume undoes it.
func (d *Dispatcher[T]) Park(agg string) {
	d.mu.Lock()
	d.haltLocked(agg)
	d.mu.Unlock()
}

// Interrupt makes failing durable deliveries give up retrying (leaving them
// pending and parking their aggregates) without closing the dispatcher. Call it
// at the start of shutdown so callers waiting on a retrying event are released.
func (d *Dispatcher[T]) Interrupt() {
	d.quitOnce.Do(func() { close(d.quit) })
}

// Close marks the dispatcher as closed, closes all per-aggregate channels, and
// waits for every worker to drain its queue. If ctx expires before all workers
// finish, the context error is returned.
func (d *Dispatcher[T]) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.waiting = false // allow any remaining Dispatch calls to fail cleanly
	close(d.stopCh)   // signal all workers to drain and exit
	d.mu.Unlock()
	d.Interrupt()

	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitIdle blocks until all in-flight jobs have been handled. The per-aggregate
// worker goroutines may still be alive (waiting for their idle timeout), but
// every dispatched event has been delivered to the bus. New Dispatch calls are
// rejected during WaitIdle to prevent the race where a new goroutine enters
// Dispatch while WaitIdle is checking idleness.
func (d *Dispatcher[T]) WaitIdle() {
	d.mu.Lock()
	d.waiting = true
	d.mu.Unlock()

	d.jobsWg.Wait()

	d.mu.Lock()
	d.waiting = false
	d.mu.Unlock()
}

// waitWorkers blocks until all per-aggregate worker goroutines have exited
// (i.e. after their idle timeout elapses). Only for use in tests.
func (d *Dispatcher[T]) waitWorkers() { d.wg.Wait() }

func (d *Dispatcher[T]) worker(aggregateID string, q *aggregateQueue[T]) {
	defer d.wg.Done()
	for {
		// Run everything already queued, in order.
		for {
			job, ok := d.pop(q)
			if !ok {
				break
			}
			d.handle(aggregateID, q, job)
		}

		select {
		case <-q.wake:
		case <-d.stopCh:
			// Drain jobs enqueued before Close, then exit.
			for {
				job, ok := d.pop(q)
				if !ok {
					return
				}
				d.handle(aggregateID, q, job)
			}
		case <-time.After(d.idleTimeout):
			d.mu.Lock()
			if len(q.jobs) > 0 {
				d.mu.Unlock()
				continue
			}
			delete(d.queues, aggregateID)
			d.mu.Unlock()
			return
		}
	}
}

// pop removes and returns the oldest queued job.
func (d *Dispatcher[T]) pop(q *aggregateQueue[T]) (*dispatchJob[T], bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(q.jobs) == 0 {
		return nil, false
	}
	job := q.jobs[0]
	q.jobs[0] = nil
	q.jobs = q.jobs[1:]
	return job, true
}

// handle delivers one job: live subscribers through the bus, then (for a
// durable delivery) the durable subscribers, retrying the failed ones until they
// succeed, the attempt cap is reached, or the dispatcher is interrupted. A panic
// in the dispatcher itself never kills the worker; it is reported to the
// publish error handler.
func (d *Dispatcher[T]) handle(agg string, q *aggregateQueue[T], job *dispatchJob[T]) {
	defer d.jobsWg.Done()
	var outcome error
	defer func() {
		job.markSettled()
		job.complete(outcome)
	}()
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("asynx: dispatcher panic: %v", r)
			d.reportPublishError(job, err)
			outcome = err
		}
	}()

	// Popped just before its aggregate was parked: leave it pending.
	if job.ack != nil && d.isHalted(agg) {
		outcome = asynxmd.ErrDeliveryParked
		return
	}

	// A failing live subscriber must not skip the durable delivery: the next
	// event of the aggregate would otherwise be delivered past this one.
	if d.bus != nil {
		if err := d.bus.PublishSync(job.ctx, job.event); err != nil {
			d.reportPublishError(job, err)
		}
	}

	if !job.durable() {
		return
	}
	if outcome = d.deliverDurable(agg, q, job); outcome != nil {
		return
	}
	if job.ack != nil {
		if err := job.ack(job.ctx); err != nil {
			d.reportPublishError(job, err)
			outcome = err
		}
	}
}

func (d *Dispatcher[T]) isHalted(agg string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.halted[agg]
	return ok
}

func (d *Dispatcher[T]) reportPublishError(job *dispatchJob[T], err error) {
	if d.onPublishError != nil {
		d.onPublishError(job.ctx, job.event, err)
	}
}

// deliverDurable runs the durable subscribers until they all succeed. It
// returns nil on success. Otherwise the delivery stays pending and the error
// says why it stopped: ErrDispatcherClosed (interrupted), or ErrDeliveryParked
// (attempts exhausted, or the aggregate was parked meanwhile).
func (d *Dispatcher[T]) deliverDurable(agg string, q *aggregateQueue[T], job *dispatchJob[T]) error {
	var pending []DurableSub[T]
	for _, sub := range d.durable {
		if job.forget || sub.Match(job.event.EventName) {
			pending = append(pending, sub)
		}
	}

	d.mu.Lock()
	halt := q.halt
	d.mu.Unlock()

	backoff := d.retryInitial
	for attempt := 1; len(pending) > 0; attempt++ {
		var failed []DurableSub[T]
		var lastErr error
		for _, sub := range pending {
			if err := d.runDurable(job, sub, attempt); err != nil {
				d.reportDeliveryError(job, err, attempt)
				failed = append(failed, sub)
				lastErr = err
			}
		}
		if attempt == 1 {
			job.markSettled()
		}
		pending = failed
		if len(pending) == 0 {
			break
		}
		if attempt >= d.maxAttempts {
			return d.giveUp(agg, job, lastErr, attempt)
		}

		select {
		case <-d.quit:
			// Parking the aggregate keeps its later durable events from being
			// delivered past this one while the dispatcher drains.
			if job.ack != nil {
				d.mu.Lock()
				d.haltLocked(agg)
				d.mu.Unlock()
			}
			return asynxmd.ErrDispatcherClosed
		case <-halt:
			return asynxmd.ErrDeliveryParked
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, d.retryMax)
	}
	return nil
}

// giveUp ends a delivery whose attempts ran out. A dead-letter handler may
// accept the event, which then counts as delivered. Otherwise the aggregate is
// parked so its later events do not overtake this one.
func (d *Dispatcher[T]) giveUp(agg string, job *dispatchJob[T], last error, attempt int) error {
	if job.ack != nil && d.deadLetter != nil {
		err := d.callDeadLetter(job, last)
		if err == nil {
			d.reportDeliveryError(job, fmt.Errorf("dead-lettered after %d attempts: %w", attempt, last), attempt)
			return nil
		}
		d.reportDeliveryError(job, fmt.Errorf("dead-letter handler: %w", err), attempt)
	}

	parked := fmt.Errorf("%w after %d attempts: %w", asynxmd.ErrDeliveryParked, attempt, last)
	d.reportDeliveryError(job, parked, attempt)
	if job.ack != nil {
		d.mu.Lock()
		d.haltLocked(agg)
		d.mu.Unlock()
	}
	return parked
}

func (d *Dispatcher[T]) callDeadLetter(job *dispatchJob[T], last error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", asynxmd.ErrHandlerPanic, r)
		}
	}()
	return d.deadLetter(job.ctx, job.event, last)
}

func (d *Dispatcher[T]) reportDeliveryError(job *dispatchJob[T], err error, attempt int) {
	if d.onDeliveryError != nil {
		d.onDeliveryError(job.ctx, job.event, err, attempt)
	}
}

// runDurable runs one durable handler, turning a panic into an error. A slow
// handler is reported once as timed out and still waited for.
func (d *Dispatcher[T]) runDurable(job *dispatchJob[T], sub DurableSub[T], attempt int) error {
	call := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				if d.onPanic != nil {
					func() {
						defer func() { _ = recover() }()
						d.onPanic(job.ctx, job.event, r)
					}()
				}
				err = fmt.Errorf("%w: %v", asynxmd.ErrHandlerPanic, r)
			}
		}()
		return sub.Handler(job.ctx, job.event)
	}

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
		d.reportDeliveryError(job, fmt.Errorf("%w after %v", asynxmd.ErrHandlerTimeout, sub.Timeout), attempt)
		return <-result
	}
}
