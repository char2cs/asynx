// Package dispatcher is asynx's default, in-memory models.Dispatcher: it
// delivers committed events to the live subscribers (the bus) and to the
// durable subscribers, in per-aggregate order, on one worker goroutine per
// aggregate with work.
//
// The implementation is split into subsystems under internal/: jobs (the unit
// of work), aggregates (the one registry, under the one mutex, of every
// aggregate's queue, halt channel and park), tracker (the deliveries in
// flight, for WaitIdle and TicketFor), reporter (the error and panic
// handlers), delivery (the durable attempt loop) and workers (the
// per-aggregate goroutines). This package wires them together and owns the
// lifecycle: Close, Interrupt and WaitIdle.
package dispatcher

import (
	"context"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/delivery"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/reporter"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/tracker"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/workers"
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
	return newDispatcher(cfg, opts...)
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
	reports := reporter.New(cfg.OnPublishError, cfg.OnDeliveryError, cfg.OnPanic)
	deliverer := delivery.New(cfg, d.registry, reports, d.quit)
	d.workers = workers.New(cfg.Bus, d.registry, deliverer, reports, d.idleTimeout)
	return d
}

func (d *dispatcher[T]) spawn(
	aggregateID string,
) {
	d.workers.Spawn(aggregateID)
}

func (d *dispatcher[T]) Enqueue(
	ctx context.Context,
	request asynxmd.DispatchRequest[T],
) (asynxmd.Ticket, error) {
	job, release := d.newJob(ctx, request)
	_, err := d.registry.Enqueue(job)
	if err != nil {
		release()
		return asynxmd.Ticket{}, err
	}
	return job.Ticket(), nil
}

func (d *dispatcher[T]) newJob(
	ctx context.Context,
	request asynxmd.DispatchRequest[T],
) (jobs.Job[T], context.CancelFunc) {
	marked := context.WithValue(context.WithoutCancel(ctx), inHandlerKey[T]{d}, request.Event.AggregateID)
	jobCtx, cancel := context.WithCancel(marked)
	stop := context.AfterFunc(d.base, cancel)
	release := func() {
		stop()
		cancel()
	}
	return jobs.New(request, jobCtx, release), release
}

func (d *dispatcher[T]) TicketFor(
	aggregateID string,
	version int64,
) (asynxmd.Ticket, bool) {
	return d.tracker.For(aggregateID, version)
}

func (d *dispatcher[T]) Unrun(
	aggregateID string,
	version int64,
) error {
	return d.registry.Unrun(aggregateID, version)
}

func (d *dispatcher[T]) Park(
	aggregateID string,
	cause error,
	undelivered ...int64,
) {
	d.registry.Park(aggregateID, cause, undelivered...)
}

func (d *dispatcher[T]) Supersede(
	aggregateID string,
) {
	d.registry.Supersede(aggregateID)
}

func (d *dispatcher[T]) Parks() map[string]uint64 {
	return d.registry.Parks()
}

func (d *dispatcher[T]) ResumeUndelivered(
	aggregateID string,
	generation uint64,
) (bool, []int64) {
	return d.registry.Lift(aggregateID, generation)
}

func (d *dispatcher[T]) Inspect(
	aggregateID string,
) asynxmd.DeliveryStatus {
	return d.registry.Inspect(aggregateID)
}

func (d *dispatcher[T]) InHandler(
	ctx context.Context,
	aggregateID string,
) bool {
	id, ok := ctx.Value(inHandlerKey[T]{d}).(string)
	return ok && id == aggregateID
}

func (d *dispatcher[T]) Interrupt() {
	d.quitOnce.Do(func() { close(d.quit) })
}

func (d *dispatcher[T]) Interrupted() <-chan struct{} {
	return d.quit
}

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

func (d *dispatcher[T]) WaitIdle() {
	d.tracker.Idle()
}
