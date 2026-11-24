package core

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/v2/internal/processor/models"
	"github.com/char2cs/asynx/v2/models/command"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
)

// Send runs cmd on the shard that owns its aggregate and returns its event once
// it is committed and queued for delivery; it does not wait for the event's
// handlers.
//
// Commands for one aggregate run one at a time, in the order their senders
// enqueued them, on the shard the router picks for the aggregate id. The
// command itself runs on the calling goroutine, with ctx, when the shard
// worker grants it its turn; ctx is never stored. Errors: ErrShuttingDown
// once Shutdown has begun, ErrContextCancelled when ctx ends before the
// command ran or while it waits for its turn, ErrQueueFull when the shard
// queue or the aggregate's mailbox has no room, and whatever the write
// pipeline returns (validation, storage, pipeline failures). Send is safe for
// concurrent use and Shutdown waits for every Send in flight. If ctx ends
// after the command started, the command still finishes and commits but Send
// returns ErrContextCancelled.
func (p *Processor[T]) Send(
	ctx context.Context,
	cmd command.Command[T],
) (event.Event[T], error) {
	p.lifeMu.Lock()
	refused := p.isShuttingDown()
	if !refused {
		p.sends.Add(1)
	}

	p.lifeMu.Unlock()
	if refused {
		return event.Event[T]{},
			failure.ErrShuttingDown
	}

	defer p.sends.Done()

	if ctx.Err() != nil {
		return event.Event[T]{},
			failure.ErrContextCancelled
	}

	commands, envelope := p.enqueueTarget(cmd)
	return p.sendAndWait(
		ctx,
		commands,
		envelope,
	)
}

// SendWait is Send that also waits for the event's handlers to run once, so the
// caller sees their effects when it returns. The wait happens on the caller's
// goroutine, never on a shard worker, so a slow or failing handler does not
// stall the shard's other aggregates and the aggregate's lock is not held.
// Besides Send's errors it returns ErrSendWaitFromHandler when called from a
// handler of the same aggregate (the handler would wait for itself) and, when
// ctx ends while waiting, the committed event with ErrContextCancelled.
func (p *Processor[T]) SendWait(
	ctx context.Context,
	cmd command.Command[T],
) (event.Event[T], error) {
	p.lifeMu.Lock()
	refused := p.isShuttingDown()
	if !refused {
		p.sends.Add(1)
	}

	p.lifeMu.Unlock()
	if refused {
		return event.Event[T]{},
			failure.ErrShuttingDown
	}

	defer p.sends.Done()

	if err := p.refuseSendWait(
		ctx,
		cmd,
	); err != nil {
		return event.Event[T]{},
			err
	}

	commands, envelope := p.enqueueTarget(cmd)
	envelope.WaitHandlers = true
	return p.sendAndWait(
		ctx,
		commands,
		envelope,
	)
}

func (p *Processor[T]) refuseSendWait(
	ctx context.Context,
	cmd command.Command[T],
) error {
	if p.dispatcher != nil &&
		p.dispatcher.InHandler(
			ctx,
			cmd.AggregateID(),
		) {
		return failure.ErrSendWaitFromHandler
	}

	if ctx.Err() != nil {
		return failure.ErrContextCancelled
	}

	return nil
}

func (p *Processor[T]) enqueueTarget(
	cmd command.Command[T],
) (chan<- *models.CommandEnvelope[T], *models.CommandEnvelope[T]) {
	shardIndex := p.router.Route(
		cmd.AggregateID(),
	)
	commands := p.pool.Shards()[shardIndex].CommandChan()
	return commands,
		models.NewCommandEnvelope(cmd)
}

func (p *Processor[T]) isShuttingDown() bool {
	return p.shuttingDown.Load()
}

func (p *Processor[T]) sendAndWait(
	ctx context.Context,
	commands chan<- *models.CommandEnvelope[T],
	envelope *models.CommandEnvelope[T],
) (event.Event[T], error) {
	select {
	case commands <- envelope:
	case <-ctx.Done():
		return event.Event[T]{},
			failure.ErrContextCancelled
	default:
		return event.Event[T]{},
			failure.ErrQueueFull
	}

	if p.onSendPending != nil {
		p.onSendPending()
	}

	select {
	case result := <-envelope.ResultChan:
		return awaitHandlers(
			ctx,
			result,
		)
	case <-envelope.Turn:
		go envelope.Execute(
			ctx,
			p.executor,
		)
		return awaitResult(
			ctx,
			envelope,
		)
	case <-ctx.Done():
		envelope.Abandon()
		return event.Event[T]{},
			failure.ErrContextCancelled
	}
}

func awaitResult[T any](
	ctx context.Context,
	envelope *models.CommandEnvelope[T],
) (event.Event[T], error) {
	select {
	case result := <-envelope.ResultChan:
		return awaitHandlers(
			ctx,
			result,
		)
	case <-ctx.Done():
		return event.Event[T]{},
			failure.ErrContextCancelled
	}
}

func awaitHandlers[T any](
	ctx context.Context,
	result models.CommandResult[T],
) (event.Event[T], error) {
	if result.Err != nil ||
		result.Wait == nil {
		return result.Event,
			result.Err
	}

	err := result.Wait(ctx)
	if errors.Is(
		err,
		failure.ErrContextCancelled,
	) {
		return result.Event,
			failure.ErrContextCancelled
	}

	return result.Event,
		err
}

// ForTesting: SetOnSendPending installs a hook that Send and SendWait call
// after the command is enqueued and before they block for their turn, letting
// a test know the command is queued. It is not synchronised: set it before
// the first send. Do not call in production code.
func (p *Processor[T]) SetOnSendPending(
	fn func(),
) {
	p.onSendPending = fn
}

// Forget erases the aggregate, its stream, snapshot and delivery entries, after
// delivering a tombstone event to the handlers and durable subscribers; see
// CommandExecutor.Forget for the full protocol. It runs on the caller's
// goroutine and bypasses the shard queues, so it does not occupy a shard worker
// while waiting for handlers and a slow handler cannot stall the shard's other
// aggregates. It returns ErrShuttingDown once Shutdown has begun, and
// Shutdown waits for every Forget in flight.
func (p *Processor[T]) Forget(
	ctx context.Context,
	aggregateID string,
) error {
	p.lifeMu.Lock()
	refused := p.isShuttingDown()
	if !refused {
		p.forgets.Add(1)
	}

	p.lifeMu.Unlock()
	if refused {
		return failure.ErrShuttingDown
	}

	defer p.forgets.Done()

	if ctx.Err() != nil {
		return failure.ErrContextCancelled
	}

	return p.executor.Forget(
		ctx,
		aggregateID,
	)
}
