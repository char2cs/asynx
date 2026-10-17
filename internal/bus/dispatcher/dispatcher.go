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
	waiting        bool           // prevents new Dispatch calls during WaitIdle
	stopCh         chan struct{}  // closed on Close to signal workers to drain and exit
	wg             sync.WaitGroup // tracks goroutine lifetime (for Close)
	jobsWg         sync.WaitGroup // tracks in-flight jobs (for WaitIdle)
	onPublishError asynxmd.PublishErrorHandler[T]
	idleTimeout    time.Duration

	durable         []DurableSub[T]
	onDeliveryError asynxmd.DeliveryErrorHandler[T]
	onPanic         asynxmd.PanicHandler[T]
	retryInitial    time.Duration
	retryMax        time.Duration
}

// aggregateQueue is an unbounded FIFO of one aggregate's jobs. It is unbounded
// on purpose: Enqueue must never block, because callers enqueue while holding
// the aggregate's command lock, and a handler that sends a command back to its
// own aggregate would otherwise deadlock against a full queue.
type aggregateQueue[T any] struct {
	jobs []*dispatchJob[T] // protected by Dispatcher.mu
	wake chan struct{}     // capacity 1: signals the worker that jobs is non-empty
}

type dispatchJob[T any] struct {
	event asynxmd.Event[T]
	ctx   context.Context
	// done closes once the event has been handled at least once: every handler
	// ran and, if all succeeded, the ack ran. A failing durable handler keeps
	// retrying after done closes.
	done   chan struct{}
	settle sync.Once
	// ack, when non-nil, marks a durable delivery: durable subscribers run and
	// ack is called once they have all succeeded.
	ack func(context.Context) error
}

func (j *dispatchJob[T]) markSettled() { j.settle.Do(func() { close(j.done) }) }

// New creates a Dispatcher that publishes events through bus.
func New[T any](bus asynxmd.Bus[T], opts ...Opt[T]) *Dispatcher[T] {
	d := &Dispatcher[T]{
		bus:          bus,
		queues:       make(map[string]*aggregateQueue[T]),
		stopCh:       make(chan struct{}),
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
// publish error handler. If the bus rejects the event, ack is not called.
func (d *Dispatcher[T]) Enqueue(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	d.mu.Lock()
	if d.closed || d.waiting {
		d.mu.Unlock()
		return nil, asynxmd.ErrDispatcherClosed
	}
	// Track this dispatch call immediately after checking not-waiting/not-closed.
	// This ensures WaitIdle() cannot return with jobsWg==0 while a goroutine
	// is still inside this function (potentially accessing d.mu/d.queues).
	d.jobsWg.Add(1)
	d.mu.Unlock()

	job := &dispatchJob[T]{
		event: event,
		ctx:   context.WithoutCancel(ctx),
		done:  make(chan struct{}),
		ack:   ack,
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		// Undo the Add(1) since we're not enqueueing a job.
		d.jobsWg.Done()
		return nil, asynxmd.ErrDispatcherClosed
	}

	q, exists := d.queues[event.AggregateID]
	if !exists {
		q = &aggregateQueue[T]{wake: make(chan struct{}, 1)}
		d.queues[event.AggregateID] = q
		d.wg.Add(1)
		go d.worker(event.AggregateID, q)
	}

	// Appending under d.mu makes "queue is empty" exact for the idle worker, so
	// it can never retire a queue that has a job in flight.
	q.jobs = append(q.jobs, job)
	select {
	case q.wake <- struct{}{}:
	default:
	}
	d.mu.Unlock()

	return job.done, nil
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
			d.handle(job)
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
				d.handle(job)
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

// handle delivers one job: ephemeral subscribers through the bus, then (for a
// durable delivery) the durable subscribers, retrying the failed ones until they
// succeed or the dispatcher closes. A panic in the dispatcher itself never kills
// the worker; it is reported to the publish error handler.
func (d *Dispatcher[T]) handle(job *dispatchJob[T]) {
	defer d.jobsWg.Done()
	defer job.markSettled()
	defer func() {
		if r := recover(); r != nil {
			d.reportPublishError(job, fmt.Errorf("asynx: dispatcher panic: %v", r))
		}
	}()

	if d.bus != nil {
		if err := d.bus.PublishSync(job.ctx, job.event); err != nil {
			d.reportPublishError(job, err)
			return
		}
	}

	if job.ack == nil {
		return
	}
	if !d.deliverDurable(job) {
		return
	}
	if err := job.ack(job.ctx); err != nil {
		d.reportPublishError(job, err)
	}
}

func (d *Dispatcher[T]) reportPublishError(job *dispatchJob[T], err error) {
	if d.onPublishError != nil {
		d.onPublishError(job.ctx, job.event, err)
	}
}

// deliverDurable runs the matching durable subscribers until all succeed. It
// returns false when the dispatcher closed first; the delivery then stays
// pending for the next start's recovery.
func (d *Dispatcher[T]) deliverDurable(job *dispatchJob[T]) bool {
	var pending []DurableSub[T]
	for _, sub := range d.durable {
		if sub.Match(job.event.EventName) {
			pending = append(pending, sub)
		}
	}

	backoff := d.retryInitial
	for attempt := 1; len(pending) > 0; attempt++ {
		var failed []DurableSub[T]
		for _, sub := range pending {
			if err := d.runDurable(job, sub, attempt); err != nil {
				d.reportDeliveryError(job, err, attempt)
				failed = append(failed, sub)
			}
		}
		if attempt == 1 {
			job.markSettled()
		}
		pending = failed
		if len(pending) == 0 {
			break
		}

		select {
		case <-d.stopCh:
			return false
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, d.retryMax)
	}
	return true
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
