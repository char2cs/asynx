package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

const defaultIdleTimeout = 5 * time.Second

var _ asynxmd.Dispatcher[int] = (*Dispatcher[int])(nil)

// Opt is a functional option for the in-memory knobs of a Dispatcher, which
// asynxmd.DispatcherConfig does not carry.
type Opt[T any] func(*Dispatcher[T])

// WithIdleTimeout sets how long a per-aggregate worker goroutine waits for new
// events before cleaning itself up. Zero or negative values are ignored.
func WithIdleTimeout[T any](
	t time.Duration,
) Opt[T] {
	if t <= 0 {
		return func(
			*Dispatcher[T],
		) {
		}
	}
	return func(
		d *Dispatcher[T],
	) {
		d.idleTimeout = t
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
	stopCh         chan struct{} // closed on Close to signal workers to drain and exit
	quit           chan struct{} // closed by Interrupt/Close: retry loops give up
	quitOnce       sync.Once
	base           context.Context // cancelled when Close gives up: stops handler contexts
	cancelBase     context.CancelFunc
	halted         map[string]uint64 // aggregates whose durable delivery is parked -> the park's generation
	parkSeq        uint64
	wg             sync.WaitGroup // tracks goroutine lifetime (for Close)
	inflight       sync.Map       // *dispatchJob -> struct{}: queued or running (for WaitIdle)
	tickets        sync.Map       // ticketKey -> *dispatchJob: the same, by event (for TicketFor)
	unrunMu        sync.Mutex
	unrun          map[string]map[int64]error // aggregate -> version -> why no handler ran, until the park lifts
	onPublishError asynxmd.PublishErrorHandler[T]
	idleTimeout    time.Duration

	durable         []asynxmd.DurableSub[T]
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

// ticketKey identifies an event in flight.
type ticketKey struct {
	aggregateID string
	version     int64
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
	// onComplete, when set, runs once when the delivery is over.
	onComplete func()
}

func (j *dispatchJob[T]) ticket() asynxmd.Ticket {
	return asynxmd.Ticket{
		Settled: j.done,
		Final:   j.final,
		Err:     func() error { return j.err },
		Unrun:   func() error { return j.unrun },
	}
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
		if j.onComplete != nil {
			j.onComplete()
		}
	})
}

// durable reports whether the job runs durable subscribers.
func (j *dispatchJob[T]) durable() bool { return j.ack != nil || j.forget }

// New creates the in-memory Dispatcher from cfg, whose values it takes as
// final: the caller applies the retry defaults. Durable deliveries run the
// subscribers in cfg.DurableSubs on per-aggregate worker goroutines; live
// events are published through cfg.Bus.
func New[T any](
	cfg asynxmd.DispatcherConfig[T],
	opts ...Opt[T],
) *Dispatcher[T] {
	base, cancelBase := context.WithCancel(context.Background())
	d := &Dispatcher[T]{
		bus:             cfg.Bus,
		queues:          make(map[string]*aggregateQueue[T]),
		stopCh:          make(chan struct{}),
		base:            base,
		cancelBase:      cancelBase,
		quit:            make(chan struct{}),
		halted:          make(map[string]uint64),
		unrun:           make(map[string]map[int64]error),
		onPublishError:  cfg.OnPublishError,
		idleTimeout:     defaultIdleTimeout,
		durable:         cfg.DurableSubs,
		onDeliveryError: cfg.OnDeliveryError,
		onPanic:         cfg.OnPanic,
		retryInitial:    cfg.RetryInitial,
		retryMax:        cfg.RetryMax,
		maxAttempts:     cfg.MaxAttempts,
		deadLetter:      cfg.DeadLetter,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Enqueue puts the request's event on its aggregate's queue and returns its
// ticket. Enqueue order is delivery order, so callers that must keep versions
// ordered enqueue while holding their per-aggregate lock and wait on the ticket
// afterwards.
//
// A non-nil Ack marks a durable delivery: the durable subscribers that receive
// the event run (with retry, holding back the aggregate's later events until
// they succeed) and Ack runs once they all have. An ack error parks the
// aggregate and is reported to the publish error handler. A failing live
// subscriber (bus error) does not skip the durable delivery.
//
// A DispatchRedelivery skips the live subscribers. A DispatchTombstone, or a
// redelivered event named ForgetEventName, reaches every durable subscriber
// whatever its pattern, and its ticket's Final reports whether they all
// succeeded.
//
// A request carrying an Ack for a parked aggregate is not queued: it stays
// pending in the store's delivery log and its ticket ends with
// ErrDeliveryParked.
func (d *Dispatcher[T]) Enqueue(
	ctx context.Context,
	request asynxmd.DispatchRequest[T],
) (asynxmd.Ticket, error) {
	return d.enqueue(ctx, request.Event, request.Ack, optsFor(request))
}

func optsFor[T any](
	request asynxmd.DispatchRequest[T],
) enqueueOpts {
	if request.Kind == asynxmd.DispatchTombstone {
		return enqueueOpts{forget: true}
	}
	if request.Kind == asynxmd.DispatchRedelivery {
		return enqueueOpts{
			skipLive: true,
			forget:   request.Event.EventName == asynxmd.ForgetEventName,
		}
	}
	return enqueueOpts{}
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
) (asynxmd.Ticket, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return asynxmd.Ticket{}, asynxmd.ErrDispatcherClosed
	}
	d.mu.Unlock()

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
	ticket := job.ticket()

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		job.cancel()
		return asynxmd.Ticket{}, asynxmd.ErrDispatcherClosed
	}

	agg := event.AggregateID
	q, exists := d.queues[agg]

	if ack != nil {
		if _, parked := d.halted[agg]; parked {
			d.markParkedUnrun(job)
			d.mu.Unlock()
			job.complete(asynxmd.ErrDeliveryParked)
			return ticket, nil
		}
	}

	if !exists {
		q = &aggregateQueue[T]{wake: make(chan struct{}, 1), halt: make(chan struct{})}
		d.queues[agg] = q
		d.wg.Add(1)
		go d.worker(agg, q)
	}

	d.track(job)
	q.jobs = append(q.jobs, job)
	select {
	case q.wake <- struct{}{}:
	default:
	}
	d.mu.Unlock()

	return ticket, nil
}

// track registers a queued job until its delivery is over.
func (d *Dispatcher[T]) track(job *dispatchJob[T]) {
	key := ticketKey{job.event.AggregateID, job.event.Version}
	d.inflight.Store(job, struct{}{})
	d.tickets.Store(key, job)
	job.onComplete = func() {
		d.inflight.Delete(job)
		d.tickets.CompareAndDelete(key, job)
	}
}

// markParkedUnrun settles a job whose handlers never ran because its aggregate
// is parked, and remembers it until the park lifts, so a retry of the command
// that produced it can be told. Callers hold d.mu, which is what keeps a lift
// from running between the check that the aggregate is parked and the record.
func (d *Dispatcher[T]) markParkedUnrun(job *dispatchJob[T]) {
	job.markUnrun(asynxmd.ErrDeliveryParked)
	d.unrunMu.Lock()
	defer d.unrunMu.Unlock()
	agg := job.event.AggregateID
	if d.unrun[agg] == nil {
		d.unrun[agg] = make(map[int64]error)
	}
	d.unrun[agg][job.event.Version] = asynxmd.ErrDeliveryParked
}

// settleIfParked settles job as undelivered if its aggregate is parked, or was
// parked since halt was taken. Only a park still in force is remembered: after a
// lift the redelivery covers the job, and a record would outlive the park.
func (d *Dispatcher[T]) settleIfParked(
	agg string,
	halt <-chan struct{},
	job *dispatchJob[T],
) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, parked := d.halted[agg]; parked {
		d.markParkedUnrun(job)
		return true
	}
	if !closed(halt) {
		return false
	}
	job.markUnrun(asynxmd.ErrDeliveryParked)
	return true
}

func (d *Dispatcher[T]) forgetUnrun(agg string) {
	d.unrunMu.Lock()
	defer d.unrunMu.Unlock()
	delete(d.unrun, agg)
}

// Unrun reports why no handler ran for the event at (aggregateID, version):
// ErrDeliveryParked while the aggregate is parked and the event was left
// undelivered by it. It is nil for an event whose handlers ran, whatever their
// outcome, and once the park has lifted.
func (d *Dispatcher[T]) Unrun(
	aggregateID string,
	version int64,
) error {
	d.unrunMu.Lock()
	defer d.unrunMu.Unlock()
	return d.unrun[aggregateID][version]
}

// TicketFor returns the ticket of the event at (aggregateID, version) while its
// delivery is under way: queued, being handled, or retrying. Once the delivery
// is over (or if the event was never queued) there is none, and false is
// returned. A retried idempotent command uses it to wait for the handlers of
// the event its first attempt produced.
func (d *Dispatcher[T]) TicketFor(
	aggregateID string,
	version int64,
) (asynxmd.Ticket, bool) {
	v, ok := d.tickets.Load(ticketKey{aggregateID, version})
	if !ok {
		return asynxmd.Ticket{}, false
	}
	return v.(*dispatchJob[T]).ticket(), true
}

// Close marks the dispatcher as closed, closes all per-aggregate channels, and
// waits for every worker to drain its queue. If ctx expires before all workers
// finish, the context error is returned.
func (d *Dispatcher[T]) Close(ctx context.Context) error {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		close(d.stopCh)
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

// WaitIdle blocks until every event queued when it was called has been handled.
// The per-aggregate worker goroutines may still be alive (waiting for their
// idle timeout), but those events have been delivered to the bus. Events queued
// while it waits are not waited for, and are not refused: commands keep
// committing and enqueueing meanwhile.
func (d *Dispatcher[T]) WaitIdle() {
	var queued []*dispatchJob[T]
	d.inflight.Range(func(
		key any,
		_ any,
	) bool {
		queued = append(queued, key.(*dispatchJob[T]))
		return true
	})
	for _, job := range queued {
		<-job.final
	}
}

// waitWorkers blocks until all per-aggregate worker goroutines have exited
// (i.e. after their idle timeout elapses). Only for use in tests.
func (d *Dispatcher[T]) waitWorkers() { d.wg.Wait() }

func (d *Dispatcher[T]) worker(
	aggregateID string,
	q *aggregateQueue[T],
) {
	defer d.wg.Done()
	for {
		d.drain(aggregateID, q)
		if d.awaitWork(aggregateID, q) {
			return
		}
	}
}

// awaitWork blocks until the aggregate has more to handle and reports whether
// its worker is finished instead: the dispatcher stopped, or the worker idled
// out.
func (d *Dispatcher[T]) awaitWork(
	aggregateID string,
	q *aggregateQueue[T],
) (finished bool) {
	select {
	case <-q.wake:
		return false
	case <-d.stopCh:
		d.drain(aggregateID, q)
		return true
	case <-time.After(d.idleTimeout):
		return d.retire(aggregateID, q)
	}
}

// drain handles every job queued for the aggregate, in order.
func (d *Dispatcher[T]) drain(
	aggregateID string,
	q *aggregateQueue[T],
) {
	for {
		job, halt, ok := d.pop(q)
		if !ok {
			return
		}
		d.handle(aggregateID, q, halt, job)
	}
}

// retire removes an idle aggregate's queue and reports whether it did. A job
// appended under d.mu keeps the queue alive, so a queue with a job in flight is
// never retired.
func (d *Dispatcher[T]) retire(
	aggregateID string,
	q *aggregateQueue[T],
) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	hasJobs := len(q.jobs) > 0
	if hasJobs {
		return false
	}
	delete(d.queues, aggregateID)
	return true
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
// publish error handler. A failing live subscriber does not skip the durable
// delivery, and a failed ack parks the aggregate: either way the next event of
// the aggregate would otherwise be delivered ahead of this one.
func (d *Dispatcher[T]) handle(
	agg string,
	q *aggregateQueue[T],
	halt <-chan struct{},
	job *dispatchJob[T],
) {
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

	if job.ack != nil && d.settleIfParked(agg, halt, job) {
		outcome = asynxmd.ErrDeliveryParked
		return
	}

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
			outcome = err
			d.parkIfCurrent(agg, q, halt, job)
			d.reportPublishError(job, err)
		}
	}
}

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

// reportPublishError calls the publish error handler. A panic in it is
// swallowed: it must not take down the worker or the process.
func (d *Dispatcher[T]) reportPublishError(
	job *dispatchJob[T],
	err error,
) {
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
func (d *Dispatcher[T]) InHandler(
	ctx context.Context,
	agg string,
) bool {
	id, ok := ctx.Value(inHandlerKey[T]{d}).(string)
	return ok && id == agg
}
