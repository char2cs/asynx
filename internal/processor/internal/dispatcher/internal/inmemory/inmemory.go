package inmemory

import (
	"context"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/delivery"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/reporter"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/tracker"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/workers"
)

const defaultIdleTimeout = 5 * time.Second

var _ asynxmd.Dispatcher[int] = (*dispatcher[int])(nil)

type dispatcher[T any] struct {
	registry    aggregates.Registry[jobs.Job[T]]
	tracker     tracker.Tracker[T]
	workers     workers.Pool[T]
	quit        chan struct{}
	quitOnce    sync.Once
	base        context.Context
	cancelBase  context.CancelFunc
	idleTimeout time.Duration
}

// New creates the in-memory Dispatcher from cfg, whose values it takes as
// final: the caller applies the retry defaults. Durable deliveries run the
// subscribers in cfg.DurableSubs on per-aggregate worker goroutines; live
// events are published through cfg.Bus (nil publishes nothing). It implements
// the whole contract documented on models.Dispatcher. opts tune what
// DispatcherConfig does not carry, such as the idle timeout of a worker.
func New[T any](
	cfg asynxmd.DispatcherConfig[T],
	opts ...Opt[T],
) asynxmd.Dispatcher[T] {
	return newDispatcher(
		cfg,
		opts...,
	)
}

func newDispatcher[T any](
	cfg asynxmd.DispatcherConfig[T],
	opts ...Opt[T],
) *dispatcher[T] {
	base, cancelBase := context.WithCancel(context.Background())
	d := &dispatcher[T]{
		quit:        make(chan struct{}),
		base:        base,
		cancelBase:  cancelBase,
		idleTimeout: defaultIdleTimeout,
		tracker:     tracker.New[T](),
	}
	for _, opt := range opts {
		opt(d)
	}

	d.registry = aggregates.New(aggregates.Hooks[jobs.Job[T]]{Spawn: d.spawn, Admit: d.tracker.Track})
	reports := reporter.New(
		cfg.OnPublishError,
		cfg.OnDeliveryError,
		cfg.OnPanic,
	)
	deliverer := delivery.New(
		cfg,
		d.registry,
		reports,
		d.quit,
	)
	d.workers = workers.New(
		cfg.Bus,
		d.registry,
		deliverer,
		reports,
		d.idleTimeout,
	)
	return d
}

func (d *dispatcher[T]) spawn(
	aggregateID string,
) {
	d.workers.Spawn(aggregateID)
}

// Enqueue queues request for its aggregate and returns its ticket.
func (d *dispatcher[T]) Enqueue(
	ctx context.Context,
	request asynxmd.DispatchRequest[T],
) (asynxmd.Ticket, error) {
	job, release := d.newJob(
		ctx,
		request,
	)
	_, err := d.registry.Enqueue(job)
	if err != nil {
		release()
		return asynxmd.Ticket{},
			err
	}

	return job.Ticket(),
		nil
}

func (d *dispatcher[T]) newJob(
	ctx context.Context,
	request asynxmd.DispatchRequest[T],
) (jobs.Job[T], context.CancelFunc) {
	marked := context.WithValue(
		context.WithoutCancel(ctx),
		inHandlerKey[T]{d},
		request.Event.AggregateID,
	)
	jobCtx, cancel := context.WithCancel(marked)
	stop := context.AfterFunc(
		d.base,
		cancel,
	)
	release := func() {
		stop()
		cancel()
	}
	return jobs.New(
			jobCtx,
			request,
			release,
		),
		release
}

// TicketFor returns the ticket of the tracked event at (aggregateID, version).
func (d *dispatcher[T]) TicketFor(
	aggregateID string,
	version int64,
) (asynxmd.Ticket, bool) {
	return d.tracker.For(
		aggregateID,
		version,
	)
}

// Unrun reports why no handler ran for the event at (aggregateID, version).
func (d *dispatcher[T]) Unrun(
	aggregateID string,
	version int64,
) error {
	return d.registry.Unrun(
		aggregateID,
		version,
	)
}

// Park parks the aggregate's durable delivery for cause.
func (d *dispatcher[T]) Park(
	aggregateID string,
	cause error,
	undelivered ...int64,
) {
	d.registry.Park(
		aggregateID,
		cause,
		undelivered...,
	)
}

// Supersede abandons the aggregate's pending durable deliveries.
func (d *dispatcher[T]) Supersede(
	aggregateID string,
) {
	d.registry.Supersede(aggregateID)
}

// Parks returns the parked aggregates with their generations.
func (d *dispatcher[T]) Parks() map[string]uint64 {
	return d.registry.Parks()
}

// ResumeUndelivered lifts the park of the given generation and returns its undelivered versions.
func (d *dispatcher[T]) ResumeUndelivered(
	aggregateID string,
	generation uint64,
) (bool, []int64) {
	return d.registry.Lift(
		aggregateID,
		generation,
	)
}

// Inspect reports the aggregate's delivery status.
func (d *dispatcher[T]) Inspect(
	aggregateID string,
) asynxmd.DeliveryStatus {
	return d.registry.Inspect(aggregateID)
}

// InHandler reports whether ctx belongs to a handler running for aggregateID.
func (d *dispatcher[T]) InHandler(
	ctx context.Context,
	aggregateID string,
) bool {
	id, ok := ctx.Value(inHandlerKey[T]{d}).(string)
	return ok && id == aggregateID
}

// Interrupt stops retries of every delivery in progress.
func (d *dispatcher[T]) Interrupt() {
	d.quitOnce.Do(func() { close(d.quit) })
}

// Interrupted returns the channel closed by Interrupt.
func (d *dispatcher[T]) Interrupted() <-chan struct{} {
	return d.quit
}

// Close closes the dispatcher and waits for its workers until ctx is done.
func (d *dispatcher[T]) Close(
	ctx context.Context,
) error {
	if d.registry.Close() {
		d.workers.Stop()
	}

	d.Interrupt()
	done := make(chan struct{})
	go d.awaitWorkers(done)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		d.cancelBase()
		return ctx.Err()
	}
}

func (d *dispatcher[T]) awaitWorkers(
	done chan<- struct{},
) {
	d.workers.Wait()
	close(done)
}

// WaitIdle blocks until every event tracked when it was called has completed.
func (d *dispatcher[T]) WaitIdle() {
	d.tracker.Idle()
}
