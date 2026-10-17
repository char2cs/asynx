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
)

// Opt is a functional option for configuring a Dispatcher.
type Opt[T any] func(*Dispatcher[T])

// WithPublishErrorHandler sets the callback invoked when bus.PublishSync
// returns a non-nil error. When nil (the default), errors are silently dropped.
func WithPublishErrorHandler[T any](fn asynxmd.PublishErrorHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) { d.onPublishError = fn }
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
	base           context.Context // cancelled when Close gives up: stops handler contexts
	cancelBase     context.CancelFunc
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
	deadLetter      asynxmd.DeadLetterHandler[T]
}

// aggregateQueue is one aggregate's FIFO of jobs. Enqueue never blocks, because
// callers enqueue while holding the aggregate's command lock, and a handler
// that sends a command back to its own aggregate would otherwise deadlock
// against a full queue. The queue is therefore unbounded: it grows with the
// commands sent to an aggregate whose handlers are slower than its writers.
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
	// Final closes when the event's delivery is over. Err then says how it
	// ended: nil if every handler succeeded (and the ack ran), else
	// ErrDeliveryParked, ErrDispatcherClosed or an ack failure.
	Final <-chan struct{}
	// Err is valid once Final is closed.
	Err func() error
	// Unrun, once Settled is closed, reports why no handler ran at all: the
	// event was not queued, or left the queue, because its aggregate is parked.
	// It is nil when the handlers ran.
	Unrun func() error
}

type dispatchJob[T any] struct {
	event asynxmd.Event[T]
	ctx   context.Context
	// done closes once the event has been handled at least once.
	done   chan struct{}
	settle sync.Once
	unrun  error // set before done closes
	final  chan struct{}
	err    error // set before final closes
	finish sync.Once
	cancel context.CancelFunc
	// ack, when non-nil, marks a durable delivery: durable subscribers run and
	// ack is called once they have all succeeded.
	ack func(context.Context) error
	// forget marks the tombstone: every durable subscriber receives it,
	// whatever its pattern, and Final reports whether they all succeeded.
	forget bool
	// skipLive keeps the live subscribers (the bus) out of a redelivery.
	skipLive bool
}

func (j *dispatchJob[T]) markSettled() { j.settle.Do(func() { close(j.done) }) }

// markUnrun settles a job whose handlers never ran.
func (j *dispatchJob[T]) markUnrun(err error) {
	j.settle.Do(func() { j.unrun = err; close(j.done) })
}

func (j *dispatchJob[T]) complete(err error) {
	j.finish.Do(func() {
		j.err = err
		j.cancel()
		close(j.final)
	})
}

// durable reports whether the job runs durable subscribers.
func (j *dispatchJob[T]) durable() bool { return j.ack != nil || j.forget }

// New creates a Dispatcher that publishes events through bus.
func New[T any](bus asynxmd.Bus[T], opts ...Opt[T]) *Dispatcher[T] {
	base, cancelBase := context.WithCancel(context.Background())
	d := &Dispatcher[T]{
		bus:          bus,
		queues:       make(map[string]*aggregateQueue[T]),
		stopCh:       make(chan struct{}),
		base:         base,
		cancelBase:   cancelBase,
		quit:         make(chan struct{}),
		halted:       make(map[string]struct{}),
		maxAttempts:  defaultMaxAttempts,
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
	t, err := d.enqueue(ctx, event, ack, enqueueOpts{})
	return t.Settled, err
}

// EnqueueTicket is Enqueue returning the full Ticket.
func (d *Dispatcher[T]) EnqueueTicket(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (Ticket, error) {
	return d.enqueue(ctx, event, ack, enqueueOpts{})
}

// EnqueueRedelivery enqueues an event read back from the delivery log. Only the
// durable subscribers receive it: live subscribers are fire-and-forget, they
// either saw the event or never will, and a replay would reorder their stream.
// A deletion marker is delivered to every durable subscriber, as the original
// was.
func (d *Dispatcher[T]) EnqueueRedelivery(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (Ticket, error) {
	return d.enqueue(ctx, event, ack, enqueueOpts{
		skipLive: true,
		forget:   event.EventName == asynxmd.ForgetEventName,
	})
}

// ReportError hands err, which concerns event, to the publish error handler.
func (d *Dispatcher[T]) ReportError(ctx context.Context, event asynxmd.Event[T], err error) {
	d.reportPublishError(&dispatchJob[T]{ctx: ctx, event: event}, err)
}

// EnqueueForget enqueues a tombstone. Every durable subscriber receives it
// (whatever its pattern) and ack runs, before the ticket's Final closes without
// an error.
func (d *Dispatcher[T]) EnqueueForget(
	ctx context.Context,
	tombstone asynxmd.Event[T],
	ack func(context.Context) error,
) (Ticket, error) {
	return d.enqueue(ctx, tombstone, ack, enqueueOpts{forget: true})
}

type enqueueOpts struct {
	forget   bool // the deletion marker
	skipLive bool // durable subscribers only
}

func (d *Dispatcher[T]) enqueue(
	ctx context.Context,
	event asynxmd.Event[T],
	ack func(context.Context) error,
	o enqueueOpts,
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

	// Handlers outlive the caller's context but not their own delivery: the
	// context is cancelled when the delivery ends, and when Close gives up.
	jobCtx, cancel := context.WithCancel(context.WithValue(context.WithoutCancel(ctx), inHandlerKey[T]{d}, event.AggregateID))
	stop := context.AfterFunc(d.base, cancel)
	job := &dispatchJob[T]{
		event:    event,
		ctx:      jobCtx,
		done:     make(chan struct{}),
		final:    make(chan struct{}),
		ack:      ack,
		forget:   o.forget,
		skipLive: o.skipLive,
		cancel:   func() { stop(); cancel() },
	}
	ticket := Ticket{
		Settled: job.done,
		Final:   job.final,
		Err:     func() error { return job.err },
		Unrun:   func() error { return job.unrun },
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		// Undo the Add(1) since we're not enqueueing a job.
		job.cancel()
		d.jobsWg.Done()
		return Ticket{}, asynxmd.ErrDispatcherClosed
	}

	agg := event.AggregateID
	q, exists := d.queues[agg]

	if ack != nil {
		if _, parked := d.halted[agg]; parked {
			d.mu.Unlock()
			// The event stays pending in the delivery log.
			job.markUnrun(asynxmd.ErrDeliveryParked)
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

// Close marks the dispatcher as closed, closes all per-aggregate channels, and
// waits for every worker to drain its queue. If ctx expires before all workers
// finish, the context error is returned.
func (d *Dispatcher[T]) Close(ctx context.Context) error {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		d.waiting = false // allow any remaining Dispatch calls to fail cleanly
		close(d.stopCh)   // signal all workers to drain and exit
	}
	d.mu.Unlock()
	d.Interrupt()

	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		d.cancelBase()
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
			job, halt, ok := d.pop(q)
			if !ok {
				break
			}
			d.handle(aggregateID, q, halt, job)
		}

		select {
		case <-q.wake:
		case <-d.stopCh:
			// Drain jobs enqueued before Close, then exit.
			for {
				job, halt, ok := d.pop(q)
				if !ok {
					return
				}
				d.handle(aggregateID, q, halt, job)
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

// pop removes and returns the oldest queued job together with the halt
// generation it is delivered under. Reading both under one hold of d.mu is what
// lets a park or supersede that follows be recognised as concerning this job:
// a halt channel read later could already be the next generation's.
func (d *Dispatcher[T]) pop(q *aggregateQueue[T]) (*dispatchJob[T], <-chan struct{}, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(q.jobs) == 0 {
		return nil, nil, false
	}
	job := q.jobs[0]
	q.jobs[0] = nil
	q.jobs = q.jobs[1:]
	return job, q.halt, true
}

// handle delivers one job: live subscribers through the bus, then (for a
// durable delivery) the durable subscribers, retrying the failed ones until they
// succeed, the attempt cap is reached, or the dispatcher is interrupted. A panic
// in the dispatcher itself never kills the worker; it is reported to the
// publish error handler.
func (d *Dispatcher[T]) handle(agg string, q *aggregateQueue[T], halt <-chan struct{}, job *dispatchJob[T]) {
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
	if job.ack != nil && (d.isHalted(agg) || closed(halt)) {
		outcome = asynxmd.ErrDeliveryParked
		job.markUnrun(outcome)
		return
	}

	// A failing live subscriber must not skip the durable delivery: the next
	// event of the aggregate would otherwise be delivered past this one.
	if d.bus != nil && !job.skipLive {
		if err := d.publishLive(job); err != nil {
			d.reportPublishError(job, err)
		}
	}

	if !job.durable() {
		return
	}
	if outcome = d.deliverDurable(agg, q, halt, job); outcome != nil {
		return
	}
	if job.ack != nil {
		if err := job.ack(job.ctx); err != nil {
			d.reportPublishError(job, err)
			outcome = err
			// The entry stays pending: park, so the aggregate's next event is not
			// delivered and acknowledged ahead of it.
			d.parkIfCurrent(agg, q, halt, job)
		}
	}
}

// reportPublishError calls the publish error handler. A panic in it is
// swallowed: it must not take down the worker or the process.
// publishLive publishes through the bus. A panic in a custom bus becomes an
// error, so it cannot skip the durable delivery that follows.
func (d *Dispatcher[T]) publishLive(job *dispatchJob[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("asynx: bus panic: %v", r)
		}
	}()
	return d.bus.PublishSync(job.ctx, job.event)
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (d *Dispatcher[T]) reportPublishError(job *dispatchJob[T], err error) {
	if d.onPublishError == nil {
		return
	}
	defer func() { _ = recover() }()
	d.onPublishError(job.ctx, job.event, err)
}

// inHandlerKey keys the context value by dispatcher: applications commonly use
// one aggregate ID across several instances (an arrow and its runtime), and a
// handler of one waiting on another is no deadlock.
type inHandlerKey[T any] struct{ d *Dispatcher[T] }

// InHandler reports whether ctx belongs to a handler this dispatcher is running
// for aggregate agg. Waiting on that aggregate's deliveries from there would
// wait on the handler itself.
func (d *Dispatcher[T]) InHandler(ctx context.Context, agg string) bool {
	id, ok := ctx.Value(inHandlerKey[T]{d}).(string)
	return ok && id == agg
}
