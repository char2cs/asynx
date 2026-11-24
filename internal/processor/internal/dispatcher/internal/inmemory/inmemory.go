package inmemory

import (
	"context"
	"sync"

	"github.com/char2cs/asynx/v2/models/dispatch"
	"github.com/char2cs/asynx/v2/models/dispatcher"

	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/aggregates/contracts"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/delivery"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/reporter"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/tracker"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/workers"
)

var _ dispatcher.Dispatcher[int] = (*inMemoryDispatcher[int])(nil)

type inMemoryDispatcher[T any] struct {
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
//
// It is wired from subsystems, each with its own package: jobs (the unit of
// work), aggregates (the one registry, under the one mutex, of every
// aggregate's queue, turns, halt channel and park), tracker (the deliveries in
// flight, for WaitIdle and TicketFor), reporter (the error and panic
// handlers), delivery (the durable attempt loop) and workers (the delivery of
// one job). The dispatcher owns the lifecycle: Close, Interrupt and WaitIdle.
func New[T any](
	cfg dispatcher.Config[T],
) dispatcher.Dispatcher[T] {
	return newDispatcher(cfg)
}

func newDispatcher[T any](
	cfg dispatcher.Config[T],
) *inMemoryDispatcher[T] {
	d := &inMemoryDispatcher[T]{
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

func (d *inMemoryDispatcher[T]) admit(
	job jobs.Job[T],
) {
	d.running.Add(1)
	d.tracker.Track(job)
}

// Enqueue queues request behind the earlier events of its aggregate and
// returns the ticket that observes its delivery. It never blocks on handlers
// and never fails for a full queue, because callers enqueue while holding
// their per-aggregate command lock: the event is delivered by a goroutine of
// its own, which waits for the turn of its aggregate.
//
// It returns models.ErrDispatcherClosed once Close has been called. A request
// that carries an Ack for a parked aggregate is not queued: it is recorded as
// undelivered and returns a ticket that is already settled as unrun and
// completed with models.ErrDeliveryParked (see models.Dispatcher). ctx
// contributes only its values: the event is delivered under a context that
// outlives ctx and is cancelled when the delivery completes or Close gives up.
func (d *inMemoryDispatcher[T]) Enqueue(
	ctx context.Context,
	request dispatch.Request[T],
) (dispatch.Ticket, error) {
	jobCtx, release := d.jobContext(
		ctx,
		request,
	)
	job := jobs.New(
		request,
		release,
	)
	var admission contracts.Admission
	d.registry.Enqueue(
		job,
		&admission,
	)
	if admission.Err != nil {
		release()
		return dispatch.Ticket{},
			admission.Err
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

func (d *inMemoryDispatcher[T]) run(
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

func (d *inMemoryDispatcher[T]) jobContext(
	ctx context.Context,
	request dispatch.Request[T],
) (context.Context, context.CancelFunc) {
	marked := context.WithValue(
		context.WithoutCancel(ctx),
		inHandlerKey[T]{d},
		request.Event.AggregateID,
	)
	jobCtx, cancel := context.WithCancel(marked)
	live := newLiveCancel(
		d.cancels,
		cancel,
	)
	d.cancels.add(live)
	return jobCtx,
		live.release
}

// TicketFor returns the ticket of the event at (aggregateID, version) while its
// delivery is queued, running or retrying, and false for an event that was
// never queued or whose delivery is over. It is safe for concurrent use and
// does not wait.
func (d *inMemoryDispatcher[T]) TicketFor(
	aggregateID string,
	version int64,
) (dispatch.Ticket, bool) {
	return d.tracker.For(
		aggregateID,
		version,
	)
}

// Unrun reports why no handler ran for the event at (aggregateID, version):
// models.ErrDeliveryParked while the aggregate is parked and the park left the
// event undelivered, nil otherwise. The answer lasts until the park lifts.
func (d *inMemoryDispatcher[T]) Unrun(
	aggregateID string,
	version int64,
) error {
	return d.registry.Unrun(
		aggregateID,
		version,
	)
}

// Park parks the aggregate's durable delivery for cause and records the given
// versions as undelivered, so Unrun answers for them. It drops the aggregate's
// queued durable deliveries and makes the one in progress give up; on an
// aggregate that is already parked it only merges the versions and keeps the
// first non-nil cause. asynx calls it when recovery finds an aggregate whose
// log cannot be delivered.
func (d *inMemoryDispatcher[T]) Park(
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

// Supersede abandons the aggregate's queued and retrying durable deliveries
// ahead of an erase and leaves the aggregate running, unparked and with no
// record of undelivered versions. It is asynx's Forget that calls it.
func (d *inMemoryDispatcher[T]) Supersede(
	aggregateID string,
) {
	d.registry.Supersede(aggregateID)
}

// Parks returns a copy of the parked aggregates with the generation of each
// park, which ResumeUndelivered takes to lift exactly that park. A park that
// is lifted and imposed again has a new generation.
func (d *inMemoryDispatcher[T]) Parks() map[string]uint64 {
	return d.registry.Parks()
}

// ResumeUndelivered lifts the park of the given generation and returns, in
// ascending order, the versions it left undelivered, which the caller then
// redelivers. It reports false and does nothing for a park that is gone, or
// that has been lifted and imposed again since the caller listed it: what a
// caller read about a park is stale once the park changes generation.
func (d *inMemoryDispatcher[T]) ResumeUndelivered(
	aggregateID string,
	generation uint64,
) (bool, []int64) {
	var outcome contracts.LiftOutcome
	d.registry.Lift(
		aggregateID,
		generation,
		&outcome,
	)
	return outcome.Lifted,
		outcome.Undelivered
}

// Inspect reports whether the aggregate is parked, the park's generation, its
// cause and the versions it left undelivered, read under one hold of the
// registry's mutex. An aggregate that is not parked has the zero status.
func (d *inMemoryDispatcher[T]) Inspect(
	aggregateID string,
) dispatch.Status {
	return d.registry.Inspect(aggregateID)
}

// InHandler reports whether ctx belongs to a handler this dispatcher runs for
// aggregateID: the context every handler receives carries a mark that no other
// dispatcher, and no other aggregate, shares. asynx uses it to refuse a
// SendWait or a Forget that would wait on the handler it is called from.
func (d *inMemoryDispatcher[T]) InHandler(
	ctx context.Context,
	aggregateID string,
) bool {
	id, ok := ctx.Value(inHandlerKey[T]{d}).(string)
	return ok && id == aggregateID
}

// Interrupt makes every delivery that is waiting to retry give up, parking the
// aggregate of a request that carries an Ack, and closes the channel Interrupted
// returns. It leaves the dispatcher open: an event without durable retries is
// still delivered normally afterwards. Calling it again changes nothing.
func (d *inMemoryDispatcher[T]) Interrupt() {
	d.quitOnce.Do(func() { close(d.quit) })
}

// Interrupted returns the channel that Interrupt, and therefore Close, closes.
// It never closes otherwise.
func (d *inMemoryDispatcher[T]) Interrupted() <-chan struct{} {
	return d.quit
}

// Close refuses every later Enqueue, interrupts the retries in progress and
// waits for the deliveries already queued to finish, which includes the events
// queued behind the one inside a handler. It returns nil once they have, and
// ctx.Err() when ctx is done first: it then cancels the context of every
// delivery still running, but cannot stop a handler or a bus that ignores its
// context, and gives up waiting. Closing again is harmless, and may be retried
// with a longer ctx.
func (d *inMemoryDispatcher[T]) Close(
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

func (d *inMemoryDispatcher[T]) awaitRuns(
	done chan<- struct{},
) {
	d.running.Wait()
	close(done)
}

// WaitIdle blocks until every event tracked when it was called has completed.
// It refuses nothing: events enqueued while it waits are not waited for, and
// commands keep enqueueing.
func (d *inMemoryDispatcher[T]) WaitIdle() {
	d.tracker.Idle()
}
