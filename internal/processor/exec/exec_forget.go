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
// The aggregate's pending durable deliveries are abandoned first, so a failing
// handler cannot hold the tombstone back. Forget then waits, up to ctx, for the
// tombstone to be delivered: to the forget handlers and to every durable
// subscriber, each of which must succeed. Only then does it erase the snapshot,
// the stream and the delivery entries, atomically, under the lock again.
//
// If Forget fails before the erase (ctx done, a durable subscriber parked, the
// dispatcher closing) the aggregate keeps its tombstone and its abandoned
// deliveries stay pending until the next recovery; call Forget again.
func (e *CommandExecutor[T]) Forget(ctx context.Context, aggregateID string) error {
	ticket, err := e.writeTombstone(ctx, aggregateID)
	if err != nil {
		return err
	}
	defer e.clearForgetting(aggregateID)

	if ticket.Final != nil {
		select {
		case err := <-ticket.Final:
			if err != nil {
				return fmt.Errorf("%w: tombstone delivery: %w", asynxmd.ErrForgetFailed, err)
			}
		case <-ctx.Done():
			return fmt.Errorf("%w: waiting for handlers: %w", asynxmd.ErrForgetFailed, ctx.Err())
		}
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

// writeTombstone marks the aggregate as being forgotten, commits the tombstone
// and enqueues it for delivery, under the aggregate's lock. On success the mark
// stays until Forget clears it; on error it is cleared here.
func (e *CommandExecutor[T]) writeTombstone(ctx context.Context, aggregateID string) (dispatcher.Ticket, error) {
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

func (e *CommandExecutor[T]) commitTombstone(ctx context.Context, aggregateID string) (dispatcher.Ticket, error) {
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
	// A failing handler must not hold the tombstone back: the entries are
	// dropped by the erase anyway.
	e.dispatcher.Supersede(aggregateID)
	ticket, err := e.dispatcher.EnqueueForget(ctx, tombstone)
	if err != nil {
		return dispatcher.Ticket{}, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
	return ticket, nil
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
