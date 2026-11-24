package exec

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/internal/keyedlock"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"sync"
)

// WithSyncProjections registers projections that run, in order, inside the
// transaction that appends each event.
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
// snapshot write fails. When nil, snapshot errors are dropped.
func WithSnapshotErrorHandler[T any](
	fn func(context.Context, asynxmd.Event[T], error),
) Opt[T] {
	return func(
		e *CommandExecutor[T],
	) {
		e.onSnapshotError = fn
	}
}

// New builds a CommandExecutor over the event store and the dispatcher, which
// may be nil, applying opts.
func New[T any](
	es eventstore.EventStore[T],
	d asynxmd.Dispatcher[T],
	opts ...Opt[T],
) *CommandExecutor[T] {
	e := &CommandExecutor[T]{es: es, dispatcher: d, forgetting: make(map[string]struct{})}
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Execute runs the pipeline and returns once the event is committed and queued.
func (e *CommandExecutor[T]) Execute(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	event, _, err := e.ExecuteAsync(
		ctx,
		cmd,
	)
	return event,
		err
}

// ExecuteWait runs the pipeline and also waits for the event's handlers to run
// once, giving up when ctx is done: the event is committed by then, so it is
// returned with an error wrapping ErrContextCancelled.
func (e *CommandExecutor[T]) ExecuteWait(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	event, wait, err := e.ExecuteAsync(
		ctx,
		cmd,
	)
	if err != nil {
		return event,
			err
	}

	if wait != nil {
		return event,
			wait(ctx)
	}

	return event,
		nil
}

// ExecuteAsync runs the pipeline and returns once the event is committed and
// queued. The returned Wait (nil when there is no dispatcher or the command was
// answered from a receipt) lets the caller, not a worker, do the waiting.
func (e *CommandExecutor[T]) ExecuteAsync(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], func(context.Context) error, error) {
	unlock, err := e.locks.Lock(
		ctx,
		cmd.AggregateID(),
	)
	if err != nil {
		return asynxmd.Event[T]{},
			nil,
			fmt.Errorf(
				"%w: %w",
				asynxmd.ErrContextCancelled,
				err,
			)
	}

	defer unlock()

	if e.isForgetting(cmd.AggregateID()) {
		return asynxmd.Event[T]{},
			nil,
			fmt.Errorf(
				"%w: %s",
				asynxmd.ErrForgetInProgress,
				cmd.AggregateID(),
			)
	}

	return e.run(
		ctx,
		cmd,
	)
}

// WithLock runs fn holding the aggregate's command lock.
func (e *CommandExecutor[T]) WithLock(
	ctx context.Context,
	id string,
	fn func() error,
) error {
	unlock, err := e.locks.Lock(
		ctx,
		id,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			asynxmd.ErrContextCancelled,
			err,
		)
	}

	defer unlock()
	return fn()
}

// WithLockIfFree runs fn holding the aggregate's command lock, unless someone
// holds it: then fn is not run and false is returned. It never waits.
func (e *CommandExecutor[T]) WithLockIfFree(
	id string,
	fn func(),
) bool {
	unlock, ok := e.locks.TryLock(id)
	if !ok {
		return false
	}

	defer unlock()
	fn()
	return true
}

func (e *CommandExecutor[T]) run(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], func(context.Context) error, error) {
	commandID := idempotencyKey(cmd)
	event, handled, err := e.answerRetry(
		ctx,
		commandID,
	)
	if handled {
		return event,
			e.waitForAnswered(
				event,
				err,
			),
			err
	}

	event, err = e.es.WriteTx(
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
		event,
	)
}

func (e *CommandExecutor[T]) publish(
	ctx context.Context,
	cmd asynxmd.Command[T],
	event asynxmd.Event[T],
) (asynxmd.Event[T], func(context.Context) error, error) {
	e.snapshotIfDue(
		ctx,
		cmd,
		event,
	)

	wait, err := e.enqueue(
		ctx,
		event,
	)
	if err != nil {
		e.parkIfDurable(
			event,
			err,
		)
		return event,
			nil,
			pipelineFailed(err)
	}

	return event,
		wait,
		nil
}

func (e *CommandExecutor[T]) parkIfDurable(
	event asynxmd.Event[T],
	cause error,
) {
	if !e.isDurable(event.EventName) {
		return
	}

	e.dispatcher.Park(
		event.AggregateID,
		cause,
	)
}

func (e *CommandExecutor[T]) answerRetry(
	ctx context.Context,
	commandID string,
) (asynxmd.Event[T], bool, error) {
	if commandID == "" {
		return asynxmd.Event[T]{},
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
) (asynxmd.Event[T], func(context.Context) error, error) {
	event, handled, err := e.writeFailed(
		ctx,
		commandID,
		cause,
	)
	if handled {
		return event,
			e.waitForAnswered(
				event,
				err,
			),
			err
	}

	return asynxmd.Event[T]{},
		nil,
		err
}

func (e *CommandExecutor[T]) snapshotIfDue(
	ctx context.Context,
	cmd asynxmd.Command[T],
	event asynxmd.Event[T],
) {
	if !cmd.ShouldSnapshot() {
		return
	}

	err := e.es.Snapshot(
		ctx,
		event,
	)
	if err != nil &&
		e.onSnapshotError != nil {
		e.onSnapshotError(
			ctx,
			event,
			err,
		)
	}
}

func (e *CommandExecutor[T]) enqueue(
	ctx context.Context,
	event asynxmd.Event[T],
) (func(context.Context) error, error) {
	if e.dispatcher == nil {
		return nil,
			nil
	}

	t, err := e.dispatcher.Enqueue(
		ctx,
		asynxmd.DispatchRequest[T]{
			Event: event,
			Ack:   e.ackFor(event),
			Kind:  asynxmd.DispatchEvent,
		},
	)
	if err != nil {
		return nil,
			err
	}

	return waitOn(
			t,
			event,
		),
		nil
}

func (e *CommandExecutor[T]) isDurable(
	eventName string,
) bool {
	return e.durableMatch != nil && e.dispatcher != nil && e.durableMatch(eventName)
}

func (e *CommandExecutor[T]) ackFor(
	event asynxmd.Event[T],
) func(context.Context) error {
	if !e.isDurable(event.EventName) {
		return nil
	}

	return func(
		ctx context.Context,
	) error {
		return e.es.Store().AckDelivery(
			ctx,
			event.AggregateID,
			event.Version,
		)
	}
}

// CommandExecutor runs commands through the write pipeline under per-aggregate locks.
type CommandExecutor[T any] struct {
	es              eventstore.EventStore[T]
	dispatcher      asynxmd.Dispatcher[T]
	locks           keyedlock.KeyedLock
	forgetMu        sync.Mutex
	forgetting      map[string]struct{}
	syncs           []models.SyncProjection[T]
	durableMatch    func(eventName string) bool
	onSnapshotError func(context.Context, asynxmd.Event[T], error)
}
