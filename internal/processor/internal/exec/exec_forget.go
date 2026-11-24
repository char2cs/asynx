package exec

import (
	"context"
	"errors"
	"fmt"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/storage"
)

func (e *CommandExecutor[T]) isForgetting(
	id string,
) bool {
	e.forgetMu.Lock()
	defer e.forgetMu.Unlock()
	_, ok := e.forgetting[id]
	return ok
}

func (e *CommandExecutor[T]) refuseIfForgetting(
	id string,
) error {
	if !e.isForgetting(id) {
		return nil
	}

	return fmt.Errorf(
		"%w: %s",
		failure.ErrForgetInProgress,
		id,
	)
}

func (e *CommandExecutor[T]) markForgetting(
	id string,
) {
	e.forgetMu.Lock()
	defer e.forgetMu.Unlock()
	e.forgetting[id] = struct{}{}
}

func (e *CommandExecutor[T]) clearForgetting(
	id string,
) {
	e.forgetMu.Lock()
	delete(
		e.forgetting,
		id,
	)
	e.forgetMu.Unlock()
}

// Forget erases an aggregate and returns once it is gone. The check that no
// other Forget is running and the mark that this one is running are two steps
// under the aggregate's command lock, which is what makes them atomic.
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
//
// Once the tombstone is delivered the erase runs to completion even if the
// caller's ctx ends.
func (e *CommandExecutor[T]) Forget(
	ctx context.Context,
	aggregateID string,
) (err error) {
	if err := e.refuseFromHandler(
		ctx,
		aggregateID,
	); err != nil {
		return err
	}

	written := &tombstoneWrite[T]{executor: e}
	err = written.commit(
		ctx,
		aggregateID,
	)
	if err != nil {
		return err
	}

	defer func() {
		e.abandonForget(
			aggregateID,
			err,
		)
	}()

	if err := e.awaitTombstone(
		ctx,
		written.ticket,
	); err != nil {
		return err
	}

	return e.erase(
		context.WithoutCancel(ctx),
		aggregateID,
	)
}

func (e *CommandExecutor[T]) refuseFromHandler(
	ctx context.Context,
	aggregateID string,
) error {
	if e.dispatcher == nil ||
		!e.dispatcher.InHandler(
			ctx,
			aggregateID,
		) {
		return nil
	}

	return fmt.Errorf(
		"%w: %s",
		failure.ErrForgetFromHandler,
		aggregateID,
	)
}

func (e *CommandExecutor[T]) abandonForget(
	aggregateID string,
	err error,
) {
	if err == nil {
		return
	}

	if e.dispatcher != nil {
		e.dispatcher.Park(
			aggregateID,
			err,
		)
	}

	e.clearForgetting(aggregateID)
}

func (e *CommandExecutor[T]) erase(
	ctx context.Context,
	aggregateID string,
) error {
	err := e.locks.Lock(
		ctx,
		aggregateID,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			failure.ErrForgetFailed,
			err,
		)
	}

	defer e.locks.Unlock(aggregateID)

	if err := e.eraseStored(
		ctx,
		aggregateID,
	); err != nil {
		return fmt.Errorf(
			"%w: %w",
			failure.ErrForgetFailed,
			err,
		)
	}

	e.clearForgetting(aggregateID)
	return nil
}

func (e *CommandExecutor[T]) eraseStored(
	ctx context.Context,
	aggregateID string,
) error {
	if err := e.es.DeleteSnapshot(
		ctx,
		aggregateID,
	); err != nil {
		return err
	}

	return e.es.Store().Atomically(
		ctx,
		droppingAggregate(
			ctx,
			aggregateID,
		),
	)
}

func droppingAggregate(
	ctx context.Context,
	aggregateID string,
) func(storage.Tx) error {
	return func(
		tx storage.Tx,
	) error {
		return dropAggregate(
			ctx,
			tx,
			aggregateID,
		)
	}
}

func dropAggregate(
	ctx context.Context,
	tx storage.Tx,
	aggregateID string,
) error {
	if err := tx.DeleteStream(
		ctx,
		"events:"+aggregateID,
	); err != nil {
		return err
	}

	return tx.DropDeliveries(
		ctx,
		aggregateID,
	)
}

func (e *CommandExecutor[T]) awaitTombstone(
	ctx context.Context,
	t dispatch.Ticket,
) error {
	if t.Final == nil {
		return nil
	}

	err := e.waitFinal(
		ctx,
		t,
	)
	if err != nil {
		return err
	}

	err = t.Err()
	if err != nil {
		return fmt.Errorf(
			"%w: tombstone delivery: %w",
			failure.ErrForgetFailed,
			err,
		)
	}

	return nil
}

func (e *CommandExecutor[T]) waitFinal(
	ctx context.Context,
	t dispatch.Ticket,
) error {
	select {
	case <-t.Final:
		return nil
	case <-ctx.Done():
		return fmt.Errorf(
			"%w: waiting for handlers: %w",
			failure.ErrForgetFailed,
			ctx.Err(),
		)
	case <-e.dispatcher.Interrupted():
		return interruptedWait(t)
	}
}

func interruptedWait(
	t dispatch.Ticket,
) error {
	select {
	case <-t.Final:
		return nil
	default:
		return fmt.Errorf(
			"%w: waiting for handlers: %w",
			failure.ErrForgetFailed,
			failure.ErrShuttingDown,
		)
	}
}

func tombstoneWriteError(
	err error,
) error {
	if isPlainValidation(err) {
		return err
	}

	return fmt.Errorf(
		"%w: %w",
		failure.ErrPipelineFailed,
		err,
	)
}

func isPlainValidation(
	err error,
) bool {
	if !errors.Is(
		err,
		failure.ErrValidation,
	) {
		return false
	}

	return !errors.Is(
		err,
		failure.ErrSyncProjection,
	)
}

func (e *CommandExecutor[T]) commitTombstone(
	ctx context.Context,
	aggregateID string,
) (dispatch.Ticket, error) {
	tombstone, err := e.es.WriteTx(
		ctx,
		NewForgetCommand[T](aggregateID),
		e.tombstoneHook(),
	)
	if err != nil {
		return dispatch.Ticket{},
			tombstoneWriteError(err)
	}

	if e.dispatcher == nil {
		return dispatch.Ticket{},
			nil
	}

	e.dispatcher.Supersede(aggregateID)
	return e.enqueueTombstone(
		ctx,
		tombstone,
	)
}

func (e *CommandExecutor[T]) enqueueTombstone(
	ctx context.Context,
	tombstone event.Event[T],
) (dispatch.Ticket, error) {
	ticket, err := e.dispatcher.Enqueue(
		ctx,
		dispatch.Request[T]{
			Event: tombstone,
			Ack: ackingTombstone(
				e.es.Store(),
				tombstone,
			),
			Kind: dispatch.KindTombstone,
		},
	)
	if err != nil {
		e.dispatcher.Park(
			tombstone.AggregateID,
			err,
		)
		return dispatch.Ticket{},
			pipelineFailed(err)
	}

	return ticket,
		nil
}

func ackingTombstone[T any](
	store storage.Store,
	tombstone event.Event[T],
) func(context.Context) error {
	return func(
		ctx context.Context,
	) error {
		return store.AckDelivery(
			ctx,
			tombstone.AggregateID,
			tombstone.Version,
		)
	}
}

func (e *CommandExecutor[T]) tombstoneHook() esmodels.TxHook[T] {
	return func(
		ctx context.Context,
		tx storage.Tx,
		evt event.Event[T],
	) error {
		return e.tombstoneSteps(
			ctx,
			tx,
			evt,
		)
	}
}

func (e *CommandExecutor[T]) tombstoneSteps(
	ctx context.Context,
	tx storage.Tx,
	evt event.Event[T],
) error {
	if err := e.supersedeDeliveriesIfLogged(
		ctx,
		tx,
		evt,
	); err != nil {
		return err
	}

	return e.runSyncProjections(
		ctx,
		tx,
		evt,
	)
}

func (e *CommandExecutor[T]) supersedeDeliveriesIfLogged(
	ctx context.Context,
	tx storage.Tx,
	tombstone event.Event[T],
) error {
	hasDeliveryLog := e.durableMatch != nil && e.dispatcher != nil
	if !hasDeliveryLog {
		return nil
	}

	return supersedeDeliveries(
		ctx,
		tx,
		tombstone,
	)
}

func supersedeDeliveries[T any](
	ctx context.Context,
	tx storage.Tx,
	tombstone event.Event[T],
) error {
	if err := tx.DropDeliveries(
		ctx,
		tombstone.AggregateID,
	); err != nil {
		return err
	}

	return tx.EnqueueDelivery(
		ctx,
		tombstone.AggregateID,
		tombstone.Version,
	)
}
