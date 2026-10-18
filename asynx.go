package asynx

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/models"
)

type Asynx[T any] interface {
	// Send validates and persists the command, then returns the resulting Event[T]
	// once it is durably written. Projection handlers are fired asynchronously —
	// when Send returns, handlers may not have run yet.
	// Use SendWait when the caller needs projections to be up to date before proceeding.
	Send(
		ctx context.Context,
		cmd models.Command[T],
	) (models.Event[T], error)

	// SendWait behaves like Send but additionally blocks until all matching
	// projection handlers have completed. When SendWait returns without error,
	// the event is persisted and every projection subscribed to it has finished.
	// Use Send when projection consistency is not required at the call site.
	// The retry of an idempotent command (see IdempotentCommand) waits too, for
	// the handlers of the event its first attempt produced, if they are still
	// running, or reports ErrDeliveryParked if the event is still pending behind
	// a parked aggregate.
	SendWait(
		ctx context.Context,
		cmd models.Command[T],
	) (models.Event[T], error)

	// Forget writes a tombstone event for the aggregate, delivers it to all
	// ForgetHandlers and to every durable subscriber (each must succeed), then
	// erases all events, pending deliveries and snapshots. From the tombstone
	// until the erase finishes, commands for the aggregate are rejected with
	// ErrForgetInProgress, so none can slip in. Returns ErrValidation if the
	// aggregate does not exist. The tombstone's transaction also drops the
	// aggregate's pending deliveries: the deletion supersedes them. If Forget
	// fails before the erase (ctx done, a durable subscriber parked) the
	// aggregate keeps its tombstone and is parked (see PausedAggregates); call
	// Forget again, which delivers the handlers' notifications again. The
	// tombstone also stays in the delivery log until acknowledged, so
	// ResumeDeliveries or a restart delivers it ahead of anything newer. A handler
	// cannot Forget the aggregate it is handling (ErrForgetFromHandler); that is
	// recognised through the handler's ctx, so pass it on: a Forget made with an
	// unrelated context (context.Background()) waits behind the very handler that
	// called it, and only ctx, or the shutdown (which then fails it with
	// ErrShuttingDown), ends the wait.
	Forget(
		ctx context.Context,
		aggregateID string,
	) error

	// PausedAggregates lists the aggregates whose durable delivery is parked:
	// attempts ran out (see Builder.WithDeliveryMaxAttempts) or a Forget could
	// not deliver its marker. Their durable events stay pending in the store and
	// SendWait on them returns ErrDeliveryParked. Parking lasts until
	// ResumeDeliveries or a restart; it never lifts by itself.
	PausedAggregates() []string

	// ResumeDeliveries redelivers the paused aggregates' pending events, in
	// order, ahead of any live event, to the durable subscribers only (live
	// subscribers do not replay). Call it once the cause is fixed.
	ResumeDeliveries(ctx context.Context) error

	// OnForget registers a handler invoked when any aggregate is forgotten.
	// The handler receives the tombstone event; Event.Aggregate holds the last known state.
	// Returns a subscription ID that can be passed to Unsubscribe.
	OnForget(
		fn models.ForgetHandler[T],
	) (string, error)

	Shutdown(
		ctx context.Context,
	) error

	Get(
		ctx context.Context,
		aggregateID string,
	) (T, error)
	Exists(
		ctx context.Context,
		aggregateID string,
	) (bool, error)
	Preload(
		ctx context.Context,
		aggregateID string,
	) error

	Subscribe(
		pattern string,
		handler models.ProjectionHandler[T],
		opts ...models.SubscriptionOpt[T],
	) (string, error)
	Unsubscribe(
		id string,
	) error

	// Listen opens a channel-based subscription for events matching the given pattern.
	// The pattern is converted via Topic() internally.
	//
	// count > 0: the channel has capacity equal to count, auto-closes and
	//            auto-unsubscribes after count events are received.
	// count == 0: unbounded — the channel has a default capacity of 16; it never
	//             auto-closes. The caller must call the returned unsubscribe func
	//             to clean up. Note: unsub() does NOT close the channel in
	//             unbounded mode; do not range over the channel after calling unsub.
	// count < 0: treated the same as count == 0 (unbounded, capacity 16).
	//
	// The returned unsubscribe func is idempotent.
	// Returns ErrEmptyPattern if pattern is empty.
	Listen(
		pattern string,
		count int,
	) (<-chan models.Event[T], func(), error)

	// SubscribeWait blocks until the first event matching pattern arrives or ctx
	// is cancelled. The pattern is converted via Topic() internally (through Listen).
	//
	// Returns the matching event, or ctx.Err() if the context is cancelled or its
	// deadline is exceeded. Auto-unsubscribes in all cases.
	//
	// To bound the wait time, pass a context with a deadline (e.g.,
	// context.WithTimeout(parent, 5*time.Second)); pass context.Background() to
	// wait indefinitely.
	//
	// For subscribe-before-act ordering (subscribe → check state → send command →
	// then read), use Listen directly.
	SubscribeWait(
		ctx context.Context,
		pattern string,
	) (models.Event[T], error)

	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn models.ProjectionHandler[T],
	) error

	// WaitPublish blocks until every event queued when it was called has been
	// handled. It is for tests: under live traffic it still returns, but what it
	// waited for is only a moment's worth of events. Commands sent while it waits
	// are neither refused nor waited for.
	WaitPublish()
}

type asynxImpl[T any] struct {
	proc *processor.Processor[T]
	es   *eventstore.EventStore[T]
	bus  models.Bus[T]
}

func (i *asynxImpl[T]) Send(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.Send(ctx, cmd)
}

func (i *asynxImpl[T]) SendWait(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.SendWait(ctx, cmd)
}

func (i *asynxImpl[T]) Shutdown(
	ctx context.Context,
) error {
	return i.proc.Shutdown(ctx)
}

func (i *asynxImpl[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	return i.es.Get(ctx, aggregateID)
}

func (i *asynxImpl[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	return i.es.Exists(ctx, aggregateID)
}

func (i *asynxImpl[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	return i.es.Preload(ctx, aggregateID)
}

func (i *asynxImpl[T]) Subscribe(
	pattern string,
	handler models.ProjectionHandler[T],
	opts ...models.SubscriptionOpt[T],
) (string, error) {
	return i.bus.Subscribe(pattern, handler, opts...)
}

func (i *asynxImpl[T]) Unsubscribe(id string) error {
	return i.bus.Unsubscribe(id)
}

func (i *asynxImpl[T]) Listen(
	pattern string,
	count int,
) (<-chan models.Event[T], func(), error) {
	if pattern == "" {
		return nil, nil, models.ErrEmptyPattern
	}

	l := newListener(i.bus, count)
	id, err := i.bus.Subscribe(Topic(pattern), l.handle)
	if err != nil {
		return nil, nil, err
	}
	l.publishSubscription(id)

	return l.ch, l.unsubscribe, nil
}

// listener feeds a Listen channel. In bounded mode (count > 0) the channel has
// capacity count, so a send never blocks once sendMu is held, and ch is closed
// under sendMu so no handler can send on a closed channel.
type listener[T any] struct {
	bus   models.Bus[T]
	ch    chan models.Event[T]
	count int

	done       chan struct{}
	subIDReady chan struct{}
	received   atomic.Int64
	closed     atomic.Bool

	sendMu    sync.Mutex
	delivered int64
	chClosed  bool
	subID     string
}

func newListener[T any](
	bus models.Bus[T],
	count int,
) *listener[T] {
	capacity := count
	if capacity <= 0 {
		capacity = 16
	}
	return &listener[T]{
		bus:        bus,
		ch:         make(chan models.Event[T], capacity),
		count:      count,
		done:       make(chan struct{}),
		subIDReady: make(chan struct{}),
	}
}

func (l *listener[T]) handle(
	_ context.Context,
	evt models.Event[T],
) {
	if l.closed.Load() {
		return
	}
	if l.count <= 0 {
		l.sendUnbounded(evt)
		return
	}
	l.sendBounded(evt)
}

func (l *listener[T]) sendUnbounded(evt models.Event[T]) {
	select {
	case <-l.done:
	case l.ch <- evt:
	}
}

func (l *listener[T]) sendBounded(evt models.Event[T]) {
	withinCount := l.received.Add(1) <= int64(l.count)
	if !withinCount {
		return
	}
	if !l.deliver(evt) {
		return
	}
	go l.unsubscribeWhenPublished()
}

// deliver sends evt and reports whether it was the last one the listener takes.
func (l *listener[T]) deliver(evt models.Event[T]) (isLast bool) {
	l.sendMu.Lock()
	defer l.sendMu.Unlock()

	if l.closed.Load() {
		return false
	}
	l.ch <- evt
	l.delivered++
	isLast = l.delivered == int64(l.count)
	if isLast {
		l.closed.Store(true)
		l.closeChLocked()
	}
	return isLast
}

func (l *listener[T]) unsubscribeWhenPublished() {
	<-l.subIDReady
	_ = l.bus.Unsubscribe(l.subID)
}

// closeChLocked closes ch exactly once. The caller holds sendMu.
func (l *listener[T]) closeChLocked() {
	if l.chClosed {
		return
	}
	l.chClosed = true
	close(l.ch)
}

func (l *listener[T]) publishSubscription(id string) {
	l.sendMu.Lock()
	l.subID = id
	l.sendMu.Unlock()
	close(l.subIDReady)
}

func (l *listener[T]) unsubscribe() {
	if !l.closed.CompareAndSwap(false, true) {
		return
	}
	close(l.done)
	_ = l.bus.Unsubscribe(l.subID)

	isBounded := l.count > 0
	if !isBounded {
		return
	}
	l.sendMu.Lock()
	l.closeChLocked()
	l.sendMu.Unlock()
}

func (i *asynxImpl[T]) SubscribeWait(
	ctx context.Context,
	pattern string,
) (models.Event[T], error) {
	ch, unsub, err := i.Listen(pattern, 1)
	if err != nil {
		var zero models.Event[T]
		return zero, err
	}
	defer unsub()

	select {
	case evt := <-ch:
		return evt, nil
	case <-ctx.Done():
		var zero models.Event[T]
		return zero, ctx.Err()
	}
}

func (i *asynxImpl[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn models.ProjectionHandler[T],
) error {
	return i.es.Replay(ctx, aggregateID, fromVersion, toVersion, fn)
}

// WaitPublish blocks until all async event publishes complete.
// Only for use in tests; do not call in production code.
func (i *asynxImpl[T]) WaitPublish() {
	i.proc.WaitPublish()
	i.bus.WaitForHandlers()
}

func (i *asynxImpl[T]) Forget(ctx context.Context, aggregateID string) error {
	return i.proc.Forget(ctx, aggregateID)
}

func (i *asynxImpl[T]) PausedAggregates() []string {
	return i.proc.Paused()
}

func (i *asynxImpl[T]) ResumeDeliveries(ctx context.Context) error {
	return i.proc.Resume(ctx, recoveryBatch)
}

func (i *asynxImpl[T]) OnForget(fn models.ForgetHandler[T]) (string, error) {
	// Escape dots so the bus treats this as a literal pattern, not a wildcard.
	return i.bus.Subscribe("asynx\\.aggregate\\.forget", models.ProjectionHandler[T](fn))
}
