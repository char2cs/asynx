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

func (e *CommandExecutor[T]) isForgetting(id string) bool {
	e.forgetMu.Lock()
	defer e.forgetMu.Unlock()
	_, ok := e.forgetting[id]
	return ok
}

// setForgetting marks id as being forgotten; false if it already was.
func (e *CommandExecutor[T]) setForgetting(id string) bool {
	e.forgetMu.Lock()
	defer e.forgetMu.Unlock()
	if _, ok := e.forgetting[id]; ok {
		return false
	}
	e.forgetting[id] = struct{}{}
	return true
}

func (e *CommandExecutor[T]) clearForgetting(id string) {
	e.forgetMu.Lock()
	delete(e.forgetting, id)
	e.forgetMu.Unlock()
}

// Forget erases an aggregate.
//
// The tombstone is written (running synchronous projections) and enqueued under
// the aggregate's lock, and the aggregate is marked as being forgotten until the
// erase finishes: commands for it are rejected with ErrForgetInProgress, so
// nothing can slip in between the tombstone and the erase. (Rejecting rather
// than queueing is what lets handlers ahead of the tombstone, which may react
// by sending commands to this aggregate, finish instead of deadlocking.)
//
// The tombstone's transaction drops the aggregate's older delivery entries, the
// deletion superseding them, and records the tombstone itself as a delivery
// entry. The deliveries still in memory are abandoned too, so a failing handler
// cannot hold the tombstone back. Forget then waits, up to ctx, for the
// tombstone to be delivered: to the forget handlers and to every durable
// subscriber, each of which must succeed. Only then does it erase the snapshot,
// the stream and the delivery entries, atomically, under the lock again.
//
// If Forget fails after the tombstone (ctx done, a durable subscriber parked,
// the erase failing) the aggregate keeps its tombstone and is parked: commands
// that arrive meanwhile are accepted but their durable events wait behind the
// marker, which stays in the delivery log so that ResumeDeliveries or a restart
// delivers it first. Call Forget again.
//
// A handler cannot Forget the aggregate it is handling: the marker queues
// behind that very handler. That fails with ErrForgetFromHandler.
func (e *CommandExecutor[T]) Forget(
	ctx context.Context,
	aggregateID string,
) (err error) {
	if e.dispatcher != nil && e.dispatcher.InHandler(ctx, aggregateID) {
		return fmt.Errorf("%w: %s", asynxmd.ErrForgetFromHandler, aggregateID)
	}
	ticket, err := e.writeTombstone(ctx, aggregateID)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			return
		}
		if e.dispatcher != nil {
			e.dispatcher.Park(aggregateID)
		}
		e.clearForgetting(aggregateID)
	}()

	if err := e.awaitTombstone(ctx, ticket); err != nil {
		return err
	}
	return e.erase(context.WithoutCancel(ctx), aggregateID) // must finish even if the caller gives up
}

// erase deletes the aggregate under its lock. On success it lifts the fence
// before releasing the lock, so no command is refused after the erase has
// completed; on failure the fence stays until Forget has parked the aggregate.
// The snapshot goes first: if the stream erase then fails the aggregate is
// intact and readable by cold replay, whereas the other order could leave a
// stale snapshot that resurrects erased state.
func (e *CommandExecutor[T]) erase(
	ctx context.Context,
	aggregateID string,
) error {
	unlock, err := e.locks.Lock(ctx, aggregateID)
	if err != nil {
		return fmt.Errorf("%w: %w", asynxmd.ErrForgetFailed, err)
	}
	defer unlock()

	if err := e.es.DeleteSnapshot(ctx, aggregateID); err != nil {
		return fmt.Errorf("%w: %w", asynxmd.ErrForgetFailed, err)
	}
	erase := func(tx asynxmd.Tx) error {
		if err := tx.DeleteStream(ctx, "events:"+aggregateID); err != nil {
			return err
		}
		return tx.DropDeliveries(ctx, aggregateID)
	}
	if err := e.es.Store().Atomically(ctx, erase); err != nil {
		return fmt.Errorf("%w: %w", asynxmd.ErrForgetFailed, err)
	}
	e.clearForgetting(aggregateID)
	return nil
}

// awaitTombstone waits for the marker to reach every subscriber. It gives up
// when ctx ends or the processor starts shutting down, so a Forget stuck behind
// a handler that is itself waiting for it (see ErrForgetFromHandler, which is
// only detected through the handler's context) cannot hold the shutdown open.
func (e *CommandExecutor[T]) awaitTombstone(
	ctx context.Context,
	t dispatcher.Ticket,
) error {
	if t.Final == nil {
		return nil
	}
	select {
	case <-t.Final:
	case <-ctx.Done():
		return fmt.Errorf("%w: waiting for handlers: %w", asynxmd.ErrForgetFailed, ctx.Err())
	case <-e.dispatcher.Interrupted():
		select {
		case <-t.Final:
		default:
			return fmt.Errorf("%w: waiting for handlers: %w", asynxmd.ErrForgetFailed, asynxmd.ErrShuttingDown)
		}
	}
	if err := t.Err(); err != nil {
		return fmt.Errorf("%w: tombstone delivery: %w", asynxmd.ErrForgetFailed, err)
	}
	return nil
}

// writeTombstone marks the aggregate as being forgotten, commits the tombstone
// and enqueues it for delivery, under the aggregate's lock. On success the mark
// stays until Forget clears it; on error it is cleared here.
func (e *CommandExecutor[T]) writeTombstone(
	ctx context.Context,
	aggregateID string,
) (dispatcher.Ticket, error) {
	unlock, err := e.locks.Lock(ctx, aggregateID)
	if err != nil {
		return dispatcher.Ticket{}, fmt.Errorf("%w: %w", asynxmd.ErrContextCancelled, err)
	}
	defer unlock()

	if !e.setForgetting(aggregateID) {
		return dispatcher.Ticket{}, fmt.Errorf("%w: %s", asynxmd.ErrForgetInProgress, aggregateID)
	}

	ticket, err := e.commitTombstone(ctx, aggregateID)
	if err != nil {
		e.clearForgetting(aggregateID)
		return dispatcher.Ticket{}, err
	}
	return ticket, nil
}

func (e *CommandExecutor[T]) commitTombstone(
	ctx context.Context,
	aggregateID string,
) (dispatcher.Ticket, error) {
	tombstone, err := e.es.WriteTx(ctx, commands.NewForgetCommand[T](aggregateID), e.tombstoneHook())
	if err != nil {
		if errors.Is(err, asynxmd.ErrValidation) && !errors.Is(err, asynxmd.ErrSyncProjection) {
			return dispatcher.Ticket{}, err
		}
		return dispatcher.Ticket{}, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}

	if e.dispatcher == nil {
		return dispatcher.Ticket{}, nil
	}
	e.dispatcher.Supersede(aggregateID)
	store := e.es.Store()
	ticket, err := e.dispatcher.EnqueueForget(ctx, tombstone, func(ctx context.Context) error {
		return store.AckDelivery(ctx, tombstone.AggregateID, tombstone.Version)
	})
	if err != nil {
		e.dispatcher.Park(aggregateID)
		return dispatcher.Ticket{}, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
	return ticket, nil
}

// tombstoneHook runs synchronous projections for the tombstone, drops the
// aggregate's pending deliveries, which the deletion supersedes, and records the
// tombstone itself as pending, so that a failed Forget leaves the marker ahead
// of anything that follows it.
func (e *CommandExecutor[T]) tombstoneHook() eventstore.TxHook[T] {
	return func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error {
		hasDeliveryLog := e.durableMatch != nil && e.dispatcher != nil
		if hasDeliveryLog {
			if err := supersedeDeliveries(ctx, tx, evt); err != nil {
				return err
			}
		}
		return e.runSyncProjections(ctx, tx, evt)
	}
}

// supersedeDeliveries drops the aggregate's pending deliveries and records the
// tombstone as the only one.
func supersedeDeliveries[T any](
	ctx context.Context,
	tx asynxmd.Tx,
	tombstone asynxmd.Event[T],
) error {
	if err := tx.DropDeliveries(ctx, tombstone.AggregateID); err != nil {
		return err
	}
	return tx.EnqueueDelivery(ctx, tombstone.AggregateID, tombstone.Version)
}
