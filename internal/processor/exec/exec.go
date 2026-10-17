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
	"errors"
	"fmt"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/commands"
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
	dispatcher      *dispatcher.Dispatcher[T]
	locks           KeyedLock
	syncs           []SyncProjection[T]
	durableMatch    func(eventName string) bool
	onSnapshotError func(context.Context, asynxmd.Event[T], error)
}

func New[T any](
	es *eventstore.EventStore[T],
	d *dispatcher.Dispatcher[T],
	opts ...Opt[T],
) *CommandExecutor[T] {
	e := &CommandExecutor[T]{es: es, dispatcher: d}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *CommandExecutor[T]) Execute(
	ctx context.Context,
	cmd asynxmd.Command[T],
	waitHandlers bool,
) (asynxmd.Event[T], error) {
	unlock, err := e.locks.Lock(ctx, cmd.AggregateID())
	if err != nil {
		return asynxmd.Event[T]{}, fmt.Errorf("%w: %w", asynxmd.ErrContextCancelled, err)
	}
	defer unlock()

	event, done, err := e.run(ctx, cmd)
	unlock()
	if err != nil {
		return event, err
	}

	if waitHandlers && done != nil {
		<-done
	}
	return event, nil
}

// run executes the pipeline. The caller holds the aggregate lock.
func (e *CommandExecutor[T]) run(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], <-chan struct{}, error) {
	commandID := idempotencyKey(cmd)
	if commandID != "" {
		if event, handled, err := e.answerFromReceipt(ctx, commandID); handled {
			return event, nil, err
		}
	}

	event, err := e.es.WriteTx(ctx, cmd, e.txHook(commandID))
	if err != nil {
		event, handled, err := e.writeFailed(ctx, commandID, err)
		if handled {
			return event, nil, err
		}
		return asynxmd.Event[T]{}, nil, err
	}

	if cmd.ShouldSnapshot() {
		if err := e.es.Snapshot(ctx, event); err != nil && e.onSnapshotError != nil {
			e.onSnapshotError(ctx, event, err)
		}
	}

	done, err := e.enqueue(ctx, event)
	if err != nil {
		return event, nil, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
	return event, done, nil
}

// enqueue hands the event to the dispatcher; the returned channel closes when
// its handlers finish. With durable delivery on, the delivery entry is acked
// at that point.
func (e *CommandExecutor[T]) enqueue(
	ctx context.Context,
	event asynxmd.Event[T],
) (<-chan struct{}, error) {
	if e.dispatcher == nil {
		return nil, nil
	}
	return e.dispatcher.Enqueue(ctx, event, e.ackFor(event))
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

// txHook returns the work that must commit atomically with the event, or nil
// when there is none.
func (e *CommandExecutor[T]) txHook(commandID string) eventstore.TxHook[T] {
	if len(e.syncs) == 0 && e.durableMatch == nil && commandID == "" {
		return nil
	}

	return func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error {
		for _, p := range e.syncs {
			if !p.Match(evt.EventName) {
				continue
			}
			if err := p.Fn(ctx, tx, evt); err != nil {
				return fmt.Errorf("%w: %w", asynxmd.ErrSyncProjection, err)
			}
		}

		if commandID != "" {
			receipt := asynxmd.Receipt{
				Accepted:    true,
				AggregateID: evt.AggregateID,
				EventID:     evt.ID,
				Version:     evt.Version,
			}
			if err := tx.PutReceipt(ctx, commandID, receipt); err != nil {
				return err
			}
		}

		if e.isDurable(evt.EventName) {
			return tx.EnqueueDelivery(ctx, evt.AggregateID, evt.Version)
		}
		return nil
	}
}

// writeFailed classifies a WriteTx error. handled is true when the outcome is
// final for the caller (a retry answer); otherwise err is the error to return.
func (e *CommandExecutor[T]) writeFailed(
	ctx context.Context,
	commandID string,
	err error,
) (event asynxmd.Event[T], handled bool, _ error) {
	switch {
	case errors.Is(err, asynxmd.ErrSyncProjection):
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)

	case commandID != "" && errors.Is(err, asynxmd.ErrDuplicateReceipt):
		// Another aggregate already used this command id: answer from its receipt.
		if event, handled, err := e.answerFromReceipt(ctx, commandID); handled {
			return event, true, err
		}
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)

	case errors.Is(err, asynxmd.ErrValidation):
		if commandID == "" {
			return event, false, err
		}
		// The rejection must be durable before the caller hears about it, or a
		// retry of the same id could pass validation later and succeed.
		recErr := e.recordRejection(ctx, commandID, err)
		switch {
		case recErr == nil:
			return event, false, err
		case errors.Is(recErr, asynxmd.ErrDuplicateReceipt):
			// Someone already decided this id: answer from their receipt.
			if event, handled, aerr := e.answerFromReceipt(ctx, commandID); handled {
				return event, true, aerr
			}
		}
		return event, false, fmt.Errorf("%w: recording rejection: %w", asynxmd.ErrPipelineFailed, recErr)

	default:
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
}

// recordRejection stores a rejected receipt so a retry of the same command id
// stays rejected. Failing to store it fails the command with ErrPipelineFailed
// instead of reporting a rejection that could later be overturned; the caller
// retries and re-validates.
func (e *CommandExecutor[T]) recordRejection(ctx context.Context, commandID string, cause error) error {
	return e.es.Store().Atomically(ctx, func(tx asynxmd.Tx) error {
		return tx.PutReceipt(ctx, commandID, asynxmd.Receipt{Rejection: cause.Error()})
	})
}

// answerFromReceipt answers a retry of an already-decided command.
func (e *CommandExecutor[T]) answerFromReceipt(
	ctx context.Context,
	commandID string,
) (event asynxmd.Event[T], handled bool, err error) {
	receipt, found, err := e.es.Store().GetReceipt(ctx, commandID)
	if err != nil {
		return event, true, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
	if !found {
		return event, false, nil
	}
	if !receipt.Accepted {
		return event, true, fmt.Errorf("%w: %s", asynxmd.ErrValidation, receipt.Rejection)
	}

	event = asynxmd.Event[T]{
		ID:          receipt.EventID,
		AggregateID: receipt.AggregateID,
		Version:     receipt.Version,
	}
	// Rebuild the full event when its stream still exists.
	_ = e.es.Replay(ctx, receipt.AggregateID, receipt.Version, receipt.Version,
		func(_ context.Context, evt asynxmd.Event[T]) {
			if evt.Version == receipt.Version {
				event = evt
			}
		})
	return event, true, nil
}

func idempotencyKey[T any](cmd asynxmd.Command[T]) string {
	if ic, ok := cmd.(asynxmd.IdempotentCommand); ok {
		return ic.CommandID()
	}
	return ""
}

// Forget erases an aggregate. The tombstone is written (running synchronous
// projections) and enqueued under the aggregate's lock; the lock is then
// released while the forget handlers observe the tombstone and the aggregate's
// last state — handlers queued ahead of it may legitimately send commands to
// this aggregate, which need the lock. The lock is retaken to erase the stream,
// its pending deliveries and its snapshot, so the erase is atomic with respect
// to every command: a command that slipped in after the tombstone is erased
// with the rest, and one that follows starts a new aggregate at version 1.
//
// A crash between the tombstone and the erase leaves the aggregate in place;
// retrying Forget runs the handlers again (at-least-once).
func (e *CommandExecutor[T]) Forget(ctx context.Context, aggregateID string) error {
	done, err := e.writeTombstone(ctx, aggregateID)
	if err != nil {
		return err
	}
	if done != nil {
		<-done
	}

	// The erase must finish even if the caller gives up.
	eraseCtx := context.WithoutCancel(ctx)
	unlock, err := e.locks.Lock(eraseCtx, aggregateID)
	if err != nil {
		return fmt.Errorf("%w: %w", asynxmd.ErrForgetFailed, err)
	}
	defer unlock()

	// Snapshot first: if the stream erase then fails, the aggregate is intact
	// and readable by cold replay; the other order could leave a stale snapshot
	// that resurrects erased state.
	if err := e.es.DeleteSnapshot(eraseCtx, aggregateID); err != nil {
		return fmt.Errorf("%w: %w", asynxmd.ErrForgetFailed, err)
	}
	erase := func(tx asynxmd.Tx) error {
		if err := tx.DeleteStream(eraseCtx, "events:"+aggregateID); err != nil {
			return err
		}
		return tx.DropDeliveries(eraseCtx, aggregateID)
	}
	if err := e.es.Store().Atomically(eraseCtx, erase); err != nil {
		return fmt.Errorf("%w: %w", asynxmd.ErrForgetFailed, err)
	}
	return nil
}

// writeTombstone commits the tombstone and enqueues it for the forget
// handlers, under the aggregate's lock. The returned channel closes when the
// handlers have finished (nil without a dispatcher).
func (e *CommandExecutor[T]) writeTombstone(ctx context.Context, aggregateID string) (<-chan struct{}, error) {
	unlock, err := e.locks.Lock(ctx, aggregateID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", asynxmd.ErrContextCancelled, err)
	}
	defer unlock()

	tombstone, err := e.es.WriteTx(ctx, commands.NewForgetCommand[T](aggregateID), e.tombstoneHook())
	if err != nil {
		if errors.Is(err, asynxmd.ErrValidation) && !errors.Is(err, asynxmd.ErrSyncProjection) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}

	if e.dispatcher == nil {
		return nil, nil
	}
	done, err := e.dispatcher.Enqueue(ctx, tombstone, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
	return done, nil
}

// tombstoneHook runs synchronous projections for the tombstone. It enqueues no
// delivery: the tombstone is delivered synchronously to the forget handlers and
// the stream is erased right after.
func (e *CommandExecutor[T]) tombstoneHook() eventstore.TxHook[T] {
	if len(e.syncs) == 0 {
		return nil
	}
	return func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error {
		for _, p := range e.syncs {
			if !p.Match(evt.EventName) {
				continue
			}
			if err := p.Fn(ctx, tx, evt); err != nil {
				return fmt.Errorf("%w: %w", asynxmd.ErrSyncProjection, err)
			}
		}
		return nil
	}
}
