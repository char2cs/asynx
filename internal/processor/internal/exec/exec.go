package exec

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/internal/keyedlock"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models/command"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"sync"
)

// WithSyncProjections registers projections that run, in order, inside the
// transaction that appends each event; a failing projection aborts the
// transaction and the command fails with ErrSyncProjection.
func WithSyncProjections[T any](
	p []models.SyncProjection[T],
) Opt[T] {
	return func(
		e *CommandExecutor[T],
	) {
		e.syncs = p
	}
}

// WithDurableDelivery makes the executor record a delivery entry in the
// transaction of every event whose name match accepts, and acknowledge it once
// the event's handlers finish. A nil match disables durable delivery.
func WithDurableDelivery[T any](
	match func(eventName string) bool,
) Opt[T] {
	return func(
		e *CommandExecutor[T],
	) {
		e.durableMatch = match
	}
}

// WithSnapshotErrorHandler sets the callback invoked when a post-commit
// snapshot write fails. The command has already committed, so a snapshot error
// never fails it. When nil, snapshot errors are dropped by design: the
// aggregate is simply rebuilt from its stream next time.
func WithSnapshotErrorHandler[T any](
	fn func(context.Context, event.Event[T], error),
) Opt[T] {
	return func(
		e *CommandExecutor[T],
	) {
		e.onSnapshotError = fn
	}
}

// New builds a CommandExecutor over the event store and the dispatcher, which
// may be nil (then events are committed but never delivered and Execute's wait
// is nil), applying opts in order.
func New[T any](
	es eventstore.EventStore[T],
	d dispatcher.Dispatcher[T],
	opts ...Opt[T],
) *CommandExecutor[T] {
	e := &CommandExecutor[T]{es: es, dispatcher: d, forgetting: make(map[string]struct{})}
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Execute runs the write pipeline under the aggregate's lock and returns once
// the event is committed and queued for delivery; it does not wait for
// handlers. It fails with ErrContextCancelled when ctx ends while waiting for
// the lock, ErrForgetInProgress while the aggregate is being forgotten, a
// validation error unchanged, or ErrPipelineFailed wrapping a storage or
// delivery failure. When only the enqueue after the commit fails, the committed
// event is returned together with the error and the aggregate is parked if its
// event is durable. The processor calls it on the sender's goroutine when the
// command's turn comes.
func (e *CommandExecutor[T]) Execute(
	ctx context.Context,
	cmd command.Command[T],
) (event.Event[T], error) {
	evt, _, err := e.ExecuteAsync(
		ctx,
		cmd,
	)
	return evt,
		err
}

// ExecuteWait runs the pipeline and also waits for the event's handlers to run
// once, giving up when ctx is done: the event is committed by then, so it is
// returned with an error wrapping ErrContextCancelled.
func (e *CommandExecutor[T]) ExecuteWait(
	ctx context.Context,
	cmd command.Command[T],
) (event.Event[T], error) {
	evt, wait, err := e.ExecuteAsync(
		ctx,
		cmd,
	)
	if err != nil {
		return evt,
			err
	}

	if wait != nil {
		return evt,
			wait(ctx)
	}

	return evt,
		nil
}

// ExecuteAsync runs the pipeline and returns once the event is committed and
// queued, releasing the aggregate's lock before it returns so the wait never
// holds it. The returned Wait (nil when there is no dispatcher or the command was
// answered from a receipt) lets the caller, not a worker, do the waiting.
func (e *CommandExecutor[T]) ExecuteAsync(
	ctx context.Context,
	cmd command.Command[T],
) (event.Event[T], func(context.Context) error, error) {
	err := e.locks.Lock(
		ctx,
		cmd.AggregateID(),
	)
	if err != nil {
		return event.Event[T]{},
			nil,
			fmt.Errorf(
				"%w: %w",
				failure.ErrContextCancelled,
				err,
			)
	}

	defer e.locks.Unlock(cmd.AggregateID())

	if e.isForgetting(cmd.AggregateID()) {
		return event.Event[T]{},
			nil,
			fmt.Errorf(
				"%w: %s",
				failure.ErrForgetInProgress,
				cmd.AggregateID(),
			)
	}

	return e.run(
		ctx,
		cmd,
	)
}

// WithLock runs fn holding the aggregate's command lock, so fn is serialised
// against commands, forgets and other WithLock sections of the same aggregate,
// never against other aggregates. It returns ErrContextCancelled (wrapping the
// context error) when ctx ends before the lock is taken, otherwise fn's error.
// fn receives ctx and must not lock the same aggregate again: the lock is not reentrant. Resume
// uses it to redeliver an aggregate without a live event overtaking.
func (e *CommandExecutor[T]) WithLock(
	ctx context.Context,
	id string,
	fn func(context.Context) error,
) error {
	err := e.locks.Lock(
		ctx,
		id,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			failure.ErrContextCancelled,
			err,
		)
	}

	defer e.locks.Unlock(id)
	return fn(ctx)
}

// WithLockIfFree runs fn holding the aggregate's command lock, unless someone
// holds it: then fn is not run and false is returned. It never waits, so it is
// safe to call from code that may itself run inside a command (the event
// store's automatic snapshots use it to skip a snapshot instead of blocking).
// It reports whether fn ran.
func (e *CommandExecutor[T]) WithLockIfFree(
	id string,
	fn func(),
) bool {
	if e.locks.TryLock(id) != nil {
		return false
	}

	defer e.locks.Unlock(id)
	fn()
	return true
}

func (e *CommandExecutor[T]) run(
	ctx context.Context,
	cmd command.Command[T],
) (event.Event[T], func(context.Context) error, error) {
	commandID := idempotencyKey(cmd)
	evt, handled, err := e.answerRetry(
		ctx,
		commandID,
	)
	if handled {
		return evt,
			e.waitForAnswered(
				evt,
				err,
			),
			err
	}

	evt, err = e.es.WriteTx(
		ctx,
		cmd,
		e.txHook(commandID),
	)
	if err != nil {
		return e.failedWrite(
			ctx,
			commandID,
			err,
		)
	}

	return e.publish(
		ctx,
		cmd,
		evt,
	)
}

func (e *CommandExecutor[T]) publish(
	ctx context.Context,
	cmd command.Command[T],
	evt event.Event[T],
) (event.Event[T], func(context.Context) error, error) {
	e.snapshotIfDue(
		ctx,
		cmd,
		evt,
	)

	wait, err := e.enqueue(
		ctx,
		evt,
	)
	if err != nil {
		e.parkIfDurable(
			evt,
			err,
		)
		return evt,
			nil,
			pipelineFailed(err)
	}

	return evt,
		wait,
		nil
}

func (e *CommandExecutor[T]) parkIfDurable(
	evt event.Event[T],
	cause error,
) {
	if !e.isDurable(evt.EventName) {
		return
	}

	e.dispatcher.Park(
		evt.AggregateID,
		cause,
	)
}

func (e *CommandExecutor[T]) answerRetry(
	ctx context.Context,
	commandID string,
) (event.Event[T], bool, error) {
	if commandID == "" {
		return event.Event[T]{},
			false,
			nil
	}

	return e.answerFromReceipt(
		ctx,
		commandID,
	)
}

func (e *CommandExecutor[T]) failedWrite(
	ctx context.Context,
	commandID string,
	cause error,
) (event.Event[T], func(context.Context) error, error) {
	evt, handled, err := e.writeFailed(
		ctx,
		commandID,
		cause,
	)
	if handled {
		return evt,
			e.waitForAnswered(
				evt,
				err,
			),
			err
	}

	return event.Event[T]{},
		nil,
		err
}

func (e *CommandExecutor[T]) snapshotIfDue(
	ctx context.Context,
	cmd command.Command[T],
	evt event.Event[T],
) {
	if !cmd.ShouldSnapshot() {
		return
	}

	err := e.es.Snapshot(
		ctx,
		evt,
	)
	if err != nil &&
		e.onSnapshotError != nil {
		e.onSnapshotError(
			ctx,
			evt,
			err,
		)
	}
}

func (e *CommandExecutor[T]) enqueue(
	ctx context.Context,
	evt event.Event[T],
) (func(context.Context) error, error) {
	if e.dispatcher == nil {
		return nil,
			nil
	}

	t, err := e.dispatcher.Enqueue(
		ctx,
		dispatch.Request[T]{
			Event: evt,
			Ack:   e.ackFor(evt),
			Kind:  dispatch.KindEvent,
		},
	)
	if err != nil {
		return nil,
			err
	}

	return waitOn(
			t,
			evt,
		),
		nil
}

func (e *CommandExecutor[T]) isDurable(
	eventName string,
) bool {
	return e.durableMatch != nil && e.dispatcher != nil && e.durableMatch(eventName)
}

func (e *CommandExecutor[T]) ackFor(
	evt event.Event[T],
) func(context.Context) error {
	if !e.isDurable(evt.EventName) {
		return nil
	}

	return func(
		ctx context.Context,
	) error {
		return e.es.Store().AckDelivery(
			ctx,
			evt.AggregateID,
			evt.Version,
		)
	}
}

// CommandExecutor runs commands through the write pipeline: it takes the
// aggregate's lock, appends the event with its synchronous projections and
// delivery entry in one transaction, snapshots when due and enqueues the event
// on the dispatcher. It also implements Forget and the idempotency receipts.
// All per-aggregate work, from any goroutine, is serialised by one keyed lock;
// the executor is safe for concurrent use. Build it with New.
type CommandExecutor[T any] struct {
	es              eventstore.EventStore[T]
	dispatcher      dispatcher.Dispatcher[T]
	locks           keyedlock.KeyedLock
	forgetMu        sync.Mutex
	forgetting      map[string]struct{}
	syncs           []models.SyncProjection[T]
	durableMatch    func(eventName string) bool
	onSnapshotError func(context.Context, event.Event[T], error)
}
