package asynx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/char2cs/asynx/internal/commands"
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
	SendWait(
		ctx context.Context,
		cmd models.Command[T],
	) (models.Event[T], error)

	// Forget writes a tombstone event for the aggregate, notifies all ForgetHandlers
	// synchronously, then erases all events, snapshots, and cached state.
	// Returns ErrValidation if the aggregate does not exist.
	Forget(
		ctx context.Context,
		aggregateID string,
	) error

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

	capacity := count
	if capacity <= 0 {
		capacity = 16
	}
	ch := make(chan models.Event[T], capacity)

	var (
		received  atomic.Int64
		delivered int64 // protected by sendMu; counts actual sends so close fires after exactly count deliveries
		closed    atomic.Bool
		// sendMu serialises send+close in bounded mode so close(ch) never races with ch<-evt.
		sendMu sync.Mutex
		subID  string
	)
	done := make(chan struct{})       // signals unbounded-mode handlers to abort on unsub
	subIDReady := make(chan struct{}) // closed once subID is written; gates auto-unsub goroutine

	handler := func(_ context.Context, evt models.Event[T]) {
		// Fast-path short-circuit if already closed/unsubbed.
		if closed.Load() {
			return
		}

		if count <= 0 {
			// Unbounded mode: send may block if consumer doesn't drain.
			// Use the done channel to abort cleanly when unsub() is called.
			select {
			case <-done:
				return
			case ch <- evt:
			}
			return
		}

		// Bounded mode: channel capacity == count, so send never blocks once we
		// hold the guard. Use received to gate exactly count deliveries.
		n := received.Add(1)
		if n > int64(count) {
			return
		}

		sendMu.Lock()
		if closed.Load() {
			sendMu.Unlock()
			return
		}
		ch <- evt // capacity == count, never blocks
		delivered++
		isLast := delivered == int64(count)
		if isLast {
			closed.Store(true)
			close(ch)
		}
		sendMu.Unlock()

		if isLast {
			// Auto-unsubscribe asynchronously — wait for subID to be published, then unsub.
			go func() {
				<-subIDReady
				_ = i.bus.Unsubscribe(subID)
			}()
		}
	}

	id, err := i.bus.Subscribe(Topic(pattern), handler)
	if err != nil {
		return nil, nil, err
	}

	sendMu.Lock()
	subID = id
	sendMu.Unlock()
	close(subIDReady)

	unsub := func() {
		if !closed.CompareAndSwap(false, true) {
			return
		}
		close(done)
		_ = i.bus.Unsubscribe(subID)
	}

	return ch, unsub, nil
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
	_, err := i.proc.SendWait(ctx, commands.NewForgetCommand[T](aggregateID))
	if err != nil {
		return err
	}
	deleteCtx := context.WithoutCancel(ctx)
	if err := i.es.Delete(deleteCtx, aggregateID); err != nil {
		return fmt.Errorf("%w: %w", models.ErrForgetFailed, err)
	}
	return nil
}

func (i *asynxImpl[T]) OnForget(fn models.ForgetHandler[T]) (string, error) {
	// Escape dots so the bus treats this as a literal pattern, not a wildcard.
	return i.bus.Subscribe("asynx\\.aggregate\\.forget", models.ProjectionHandler[T](fn))
}
