package inmemory

import (
	"context"
	"sync"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/delivery"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/reporter"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/tracker"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/workers"
)

var _ asynxmd.Dispatcher[int] = (*dispatcher[int])(nil)

type dispatcher[T any] struct {
	registry aggregates.Registry[jobs.Job[T]]
	tracker  tracker.Tracker[T]
	workers  workers.Pool[T]
	running  sync.WaitGroup
	quit     chan struct{}
	quitOnce sync.Once
	cancels  *cancels
}

// New creates the in-memory Dispatcher from cfg, whose values it takes as
// final: the caller applies the retry defaults. Every event is delivered by a
// goroutine of its own, which waits for the turn of its aggregate; durable
// deliveries run the subscribers in cfg.DurableSubs, and live events are
// published through cfg.Bus (nil publishes nothing). It implements the whole
// contract documented on models.Dispatcher.
func New[T any](
	cfg asynxmd.DispatcherConfig[T],
) asynxmd.Dispatcher[T] {
	return newDispatcher(cfg)
}

func newDispatcher[T any](
	cfg asynxmd.DispatcherConfig[T],
) *dispatcher[T] {
	d := &dispatcher[T]{
		quit:    make(chan struct{}),
		cancels: newCancels(),
		tracker: tracker.New[T](),
	}
	d.registry = aggregates.New(aggregates.Hooks[jobs.Job[T]]{Admit: d.admit})
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
	)
	return d
}

func (d *dispatcher[T]) admit(
	job jobs.Job[T],
) {
	d.running.Add(1)
	d.tracker.Track(job)
}

// Enqueue queues request for its aggregate and returns its ticket.
func (d *dispatcher[T]) Enqueue(
	ctx context.Context,
	request asynxmd.DispatchRequest[T],
) (asynxmd.Ticket, error) {
	jobCtx, release := d.jobContext(
		ctx,
		request,
	)
	job := jobs.New(
		request,
		release,
	)
	admission, err := d.registry.Enqueue(job)
	if err != nil {
		release()
		return asynxmd.Ticket{},
			err
	}

	if !admission.Parked {
		go d.run(
			jobCtx,
			job,
			admission.Turn,
			admission.Done,
		)
	}

	return job.Ticket(),
		nil
}

func (d *dispatcher[T]) run(
	ctx context.Context,
	job jobs.Job[T],
	turn <-chan struct{},
	done chan<- struct{},
) {
	defer d.running.Done()
	d.workers.Run(
		ctx,
		job,
		turn,
		done,
	)
}

func (d *dispatcher[T]) jobContext(
	ctx context.Context,
	request asynxmd.DispatchRequest[T],
) (context.Context, context.CancelFunc) {
	marked := context.WithValue(
		context.WithoutCancel(ctx),
		inHandlerKey[T]{d},
		request.Event.AggregateID,
	)
	jobCtx, cancel := context.WithCancel(marked)
	return jobCtx,
		d.cancels.add(cancel)
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

// Close closes the dispatcher and waits for its deliveries until ctx is done.
func (d *dispatcher[T]) Close(
	ctx context.Context,
) error {
	d.registry.Close()
	d.Interrupt()
	done := make(chan struct{})
	go d.awaitRuns(done)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		d.cancels.cancelAll()
		return ctx.Err()
	}
}

func (d *dispatcher[T]) awaitRuns(
	done chan<- struct{},
) {
	d.running.Wait()
	close(done)
}

// WaitIdle blocks until every event tracked when it was called has completed.
func (d *dispatcher[T]) WaitIdle() {
	d.tracker.Idle()
}
