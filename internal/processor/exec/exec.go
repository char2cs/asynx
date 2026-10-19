// Package exec implements the command execution pipeline for Asynx.
//
// CommandExecutor[T] runs every command under a per-aggregate lock, so two
// commands for one aggregate never overlap and their events are enqueued in
// version order, whatever the worker count:
//   - Lock     — Take the aggregate's lock (respects ctx)
//   - Receipt  — For idempotent commands, answer a retry from its receipt
//   - Write    — Load, Validate, then in ONE transaction: append the event, run
//     synchronous projections, store the receipt, enqueue the durable delivery
//   - Snapshot — Best-effort, after the commit; failure never blocks dispatch
//   - Enqueue  — Hand the event to the Dispatcher (per-aggregate FIFO), then
//     release the lock; SendWait waits for handlers after the release
//
// Validation errors short-circuit before anything is written. Storage and
// projection errors wrap to ErrPipelineFailed.
package exec

import (
	"context"
	"fmt"
	"sync"

	"github.com/char2cs/asynx/internal/eventstore"
	asynxmd "github.com/char2cs/asynx/models"
)

// SyncProjection is a projection that runs inside the event's transaction.
type SyncProjection[T any] struct {
	// Match reports whether the projection applies to an event name.
	Match func(eventName string) bool
	// Fn writes the projection's own state through tx.Native().
	Fn func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error
}

// Opt configures a CommandExecutor.
type Opt[T any] func(*CommandExecutor[T])

// WithSyncProjections registers projections that run, in order, inside the
// transaction that appends each event.
func WithSyncProjections[T any](p []SyncProjection[T]) Opt[T] {
	return func(e *CommandExecutor[T]) { e.syncs = p }
}

// WithDurableDelivery makes the executor record a delivery entry in the
// transaction of every event whose name match accepts, and acknowledge it once
// the event's handlers finish. A nil match disables durable delivery.
func WithDurableDelivery[T any](match func(eventName string) bool) Opt[T] {
	return func(e *CommandExecutor[T]) { e.durableMatch = match }
}

// WithSnapshotErrorHandler sets the callback invoked when a post-commit
// snapshot write fails. When nil, snapshot errors are dropped.
func WithSnapshotErrorHandler[T any](fn func(context.Context, asynxmd.Event[T], error)) Opt[T] {
	return func(e *CommandExecutor[T]) { e.onSnapshotError = fn }
}

type CommandExecutor[T any] struct {
	es              *eventstore.EventStore[T]
	dispatcher      asynxmd.Dispatcher[T]
	locks           KeyedLock
	forgetMu        sync.Mutex
	forgetting      map[string]struct{} // aggregates between tombstone and erase
	syncs           []SyncProjection[T]
	durableMatch    func(eventName string) bool
	onSnapshotError func(context.Context, asynxmd.Event[T], error)
}

func New[T any](
	es *eventstore.EventStore[T],
	d asynxmd.Dispatcher[T],
	opts ...Opt[T],
) *CommandExecutor[T] {
	e := &CommandExecutor[T]{es: es, dispatcher: d, forgetting: make(map[string]struct{})}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Execute runs the pipeline. With waitHandlers it also waits for the event's
// handlers to run once, giving up when ctx is done: the event is committed by
// then, so it is returned with an error wrapping ErrContextCancelled.
func (e *CommandExecutor[T]) Execute(
	ctx context.Context,
	cmd asynxmd.Command[T],
	waitHandlers bool,
) (asynxmd.Event[T], error) {
	event, wait, err := e.ExecuteAsync(ctx, cmd)
	if err != nil {
		return event, err
	}
	if waitHandlers && wait != nil {
		return event, wait(ctx)
	}
	return event, nil
}

// Wait blocks until an event's handlers have run once. It gives up when ctx is
// done (the command is already committed, so that does not undo it) and fails
// with ErrDeliveryParked when the event's aggregate is parked, which means no
// handler ran.
type Wait func(ctx context.Context) error

// ExecuteAsync runs the pipeline and returns once the event is committed and
// queued. The returned Wait (nil when there is no dispatcher or the command was
// answered from a receipt) lets the caller, not a worker, do the waiting.
func (e *CommandExecutor[T]) ExecuteAsync(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], Wait, error) {
	unlock, err := e.locks.Lock(ctx, cmd.AggregateID())
	if err != nil {
		return asynxmd.Event[T]{}, nil, fmt.Errorf("%w: %w", asynxmd.ErrContextCancelled, err)
	}
	defer unlock()

	if e.isForgetting(cmd.AggregateID()) {
		return asynxmd.Event[T]{}, nil, fmt.Errorf("%w: %s", asynxmd.ErrForgetInProgress, cmd.AggregateID())
	}
	return e.run(ctx, cmd)
}

// WithLock runs fn holding the aggregate's command lock.
func (e *CommandExecutor[T]) WithLock(
	ctx context.Context,
	id string,
	fn func() error,
) error {
	unlock, err := e.locks.Lock(ctx, id)
	if err != nil {
		return fmt.Errorf("%w: %w", asynxmd.ErrContextCancelled, err)
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

// run executes the pipeline. The caller holds the aggregate lock.
func (e *CommandExecutor[T]) run(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], Wait, error) {
	commandID := idempotencyKey(cmd)
	if commandID != "" {
		if event, handled, err := e.answerFromReceipt(ctx, commandID); handled {
			return event, e.waitForAnswered(event, err), err
		}
	}

	event, err := e.es.WriteTx(ctx, cmd, e.txHook(commandID))
	if err != nil {
		event, handled, err := e.writeFailed(ctx, commandID, err)
		if handled {
			return event, e.waitForAnswered(event, err), err
		}
		return asynxmd.Event[T]{}, nil, err
	}

	if cmd.ShouldSnapshot() {
		if err := e.es.Snapshot(ctx, event); err != nil && e.onSnapshotError != nil {
			e.onSnapshotError(ctx, event, err)
		}
	}

	wait, err := e.enqueue(ctx, event)
	if err != nil {
		if e.isDurable(event.EventName) {
			e.dispatcher.Park(event.AggregateID, nil)
		}
		return event, nil, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
	return event, wait, nil
}

// enqueue hands the event to the dispatcher. With durable delivery on, the
// delivery entry is acked once its handlers finish. When the enqueue fails
// after the commit the delivery entry is pending, so run parks the aggregate:
// its later events must not be delivered ahead of it.
func (e *CommandExecutor[T]) enqueue(
	ctx context.Context,
	event asynxmd.Event[T],
) (Wait, error) {
	if e.dispatcher == nil {
		return nil, nil
	}
	t, err := e.dispatcher.Enqueue(ctx, asynxmd.DispatchRequest[T]{
		Event: event,
		Ack:   e.ackFor(event),
		Kind:  asynxmd.DispatchEvent,
	})
	if err != nil {
		return nil, err
	}
	return waitOn(t, event), nil
}

// waitForAnswered gives the retry of an idempotent command the same wait as the
// original: for the handlers of the event the first attempt produced, if its
// delivery is still under way. Once that is over, or when the answer is an
// error, there is nothing to wait for.
func (e *CommandExecutor[T]) waitForAnswered(
	event asynxmd.Event[T],
	answer error,
) Wait {
	if answer != nil || e.dispatcher == nil {
		return nil
	}
	t, underway := e.dispatcher.TicketFor(event.AggregateID, event.Version)
	if underway {
		return waitOn(t, event)
	}
	return unrunWait(e.dispatcher.Unrun(event.AggregateID, event.Version), event)
}

// unrunWait is the Wait of a retry whose event the dispatcher left undelivered
// because its aggregate is parked: no handler took it, which the retry reports
// as the first attempt did. It is nil when there is nothing to report.
func unrunWait[T any](
	unrun error,
	event asynxmd.Event[T],
) Wait {
	if unrun == nil {
		return nil
	}
	return func(context.Context) error {
		return fmt.Errorf("%w: event %s v%d is pending, no handler ran", unrun, event.AggregateID, event.Version)
	}
}

// waitOn is the Wait for the handlers of the event ticket t tracks.
func waitOn[T any](
	t asynxmd.Ticket,
	event asynxmd.Event[T],
) Wait {
	return func(ctx context.Context) error {
		select {
		case <-t.Settled:
			return settledOutcome(t, event)
		case <-ctx.Done():
			return fmt.Errorf("%w: waiting for handlers: %w", asynxmd.ErrContextCancelled, ctx.Err())
		}
	}
}

// settledOutcome reports why a settled delivery ran no handler, if it did not.
func settledOutcome[T any](
	t asynxmd.Ticket,
	event asynxmd.Event[T],
) error {
	err := t.Unrun()
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: event %s v%d is pending, no handler ran", err, event.AggregateID, event.Version)
}

// isDurable reports whether the event gets a delivery entry.
func (e *CommandExecutor[T]) isDurable(eventName string) bool {
	return e.durableMatch != nil && e.dispatcher != nil && e.durableMatch(eventName)
}

func (e *CommandExecutor[T]) ackFor(event asynxmd.Event[T]) func(context.Context) error {
	if !e.isDurable(event.EventName) {
		return nil
	}
	return func(ctx context.Context) error {
		return e.es.Store().AckDelivery(ctx, event.AggregateID, event.Version)
	}
}

// runSyncProjections runs, in order, the projections that match the event,
// inside its transaction. A failure wraps ErrSyncProjection.
func (e *CommandExecutor[T]) runSyncProjections(
	ctx context.Context,
	tx asynxmd.Tx,
	evt asynxmd.Event[T],
) error {
	for _, p := range e.syncs {
		matches := p.Match(evt.EventName)
		if !matches {
			continue
		}
		if err := p.Fn(ctx, tx, evt); err != nil {
			return fmt.Errorf("%w: %w", asynxmd.ErrSyncProjection, err)
		}
	}
	return nil
}

// txHook returns the work that must commit atomically with the event, or nil
// when there is none.
func (e *CommandExecutor[T]) txHook(commandID string) eventstore.TxHook[T] {
	if len(e.syncs) == 0 && e.durableMatch == nil && commandID == "" {
		return nil
	}

	return func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error {
		if err := e.runSyncProjections(ctx, tx, evt); err != nil {
			return err
		}

		if err := putAcceptedReceipt(ctx, tx, commandID, evt); err != nil {
			return err
		}

		if e.isDurable(evt.EventName) {
			return tx.EnqueueDelivery(ctx, evt.AggregateID, evt.Version)
		}
		return nil
	}
}

// putAcceptedReceipt records the command's receipt, unless it has no id.
func putAcceptedReceipt[T any](
	ctx context.Context,
	tx asynxmd.Tx,
	commandID string,
	evt asynxmd.Event[T],
) error {
	if commandID == "" {
		return nil
	}
	receipt := asynxmd.Receipt{
		Accepted:    true,
		AggregateID: evt.AggregateID,
		EventID:     evt.ID,
		Version:     evt.Version,
	}
	return tx.PutReceipt(ctx, commandID, receipt)
}
