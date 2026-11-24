package core

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Send runs cmd on the shard that owns its aggregate and returns its event once
// it is committed.
func (p *Processor[T]) Send(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	if !p.beginSend() {
		return asynxmd.Event[T]{},
			asynxmd.ErrShuttingDown
	}

	defer p.sends.Done()

	if err := ctx.Err(); err != nil {
		return asynxmd.Event[T]{},
			asynxmd.ErrContextCancelled
	}

	commands, envelope := p.enqueueTarget(
		ctx,
		cmd,
	)
	return p.sendAndWait(
		ctx,
		commands,
		envelope,
	)
}

// SendWait is Send that also waits for the handlers of the event to run once.
func (p *Processor[T]) SendWait(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	if !p.beginSend() {
		return asynxmd.Event[T]{},
			asynxmd.ErrShuttingDown
	}

	defer p.sends.Done()

	if err := p.refuseSendWait(
		ctx,
		cmd,
	); err != nil {
		return asynxmd.Event[T]{},
			err
	}

	commands, envelope := p.enqueueTarget(
		ctx,
		cmd,
	)
	envelope.WaitHandlers = true
	return p.sendAndWait(
		ctx,
		commands,
		envelope,
	)
}

func (p *Processor[T]) refuseSendWait(
	ctx context.Context,
	cmd asynxmd.Command[T],
) error {
	if p.dispatcher != nil &&
		p.dispatcher.InHandler(
			ctx,
			cmd.AggregateID(),
		) {
		return asynxmd.ErrSendWaitFromHandler
	}

	if err := ctx.Err(); err != nil {
		return asynxmd.ErrContextCancelled
	}

	return nil
}

func (p *Processor[T]) enqueueTarget(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (chan<- *models.CommandEnvelope[T], *models.CommandEnvelope[T]) {
	shardIndex := p.router.Route(
		cmd.AggregateID(),
	)
	commands := p.pool.Shards()[shardIndex].CommandChan()
	envelope := &models.CommandEnvelope[T]{
		Cmd: cmd,
		Ctx: ctx,
		ResultChan: make(
			chan models.CommandResult[T],
			1,
		),
	}

	return commands,
		envelope
}

func (p *Processor[T]) isShuttingDown() bool {
	return p.shuttingDown.Load()
}

func (p *Processor[T]) beginSend() bool {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if p.isShuttingDown() {
		return false
	}

	p.sends.Add(1)
	return true
}

func (p *Processor[T]) sendAndWait(
	ctx context.Context,
	commands chan<- *models.CommandEnvelope[T],
	envelope *models.CommandEnvelope[T],
) (asynxmd.Event[T], error) {
	select {
	case commands <- envelope:
	case <-ctx.Done():
		return asynxmd.Event[T]{},
			asynxmd.ErrContextCancelled
	default:
		return asynxmd.Event[T]{},
			asynxmd.ErrQueueFull
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
	case <-ctx.Done():
		return asynxmd.Event[T]{},
			asynxmd.ErrContextCancelled
	}
}

func awaitHandlers[T any](
	ctx context.Context,
	result models.CommandResult[T],
) (asynxmd.Event[T], error) {
	if result.Err != nil ||
		result.Wait == nil {
		return result.Event,
			result.Err
	}

	err := result.Wait(ctx)
	if errors.Is(
		err,
		asynxmd.ErrContextCancelled,
	) {
		return result.Event,
			asynxmd.ErrContextCancelled
	}

	return result.Event,
		err
}

// ForTesting: SetOnSendPending sets a callback invoked after a command is
// enqueued but before Send or SendWait blocks waiting for its result.
// Do not call in production code.
func (p *Processor[T]) SetOnSendPending(
	fn func(),
) {
	p.onSendPending = fn
}

// Forget erases the aggregate; see CommandExecutor.Forget. It does not occupy a
// shard worker while waiting for handlers, so a slow handler cannot stall the
// shard's other aggregates.
func (p *Processor[T]) Forget(
	ctx context.Context,
	aggregateID string,
) error {
	p.lifeMu.Lock()
	if p.isShuttingDown() {
		p.lifeMu.Unlock()
		return asynxmd.ErrShuttingDown
	}

	p.forgets.Add(1)
	p.lifeMu.Unlock()
	defer p.forgets.Done()

	if err := ctx.Err(); err != nil {
		return asynxmd.ErrContextCancelled
	}

	return p.executor.Forget(
		ctx,
		aggregateID,
	)
}
