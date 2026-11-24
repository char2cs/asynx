package exec

import (
	"context"
	"fmt"
	asynxmd "github.com/char2cs/asynx/models"
)

func (e *CommandExecutor[T]) waitForAnswered(
	event asynxmd.Event[T],
	answer error,
) func(context.Context) error {
	if answer != nil ||
		e.dispatcher == nil {
		return nil
	}

	t, underway := e.dispatcher.TicketFor(
		event.AggregateID,
		event.Version,
	)
	if underway {
		return waitOn(
			t,
			event,
		)
	}

	return unrunWait(
		e.dispatcher.Unrun(
			event.AggregateID,
			event.Version,
		),
		event,
	)
}

func unrunWait[T any](
	unrun error,
	event asynxmd.Event[T],
) func(context.Context) error {
	if unrun == nil {
		return nil
	}

	return func(
		context.Context,
	) error {
		return fmt.Errorf(
			"%w: event %s v%d is pending, no handler ran",
			unrun,
			event.AggregateID,
			event.Version,
		)
	}
}

func waitOn[T any](
	t asynxmd.Ticket,
	event asynxmd.Event[T],
) func(context.Context) error {
	return func(
		ctx context.Context,
	) error {
		select {
		case <-t.Settled:
			return settledOutcome(
				t,
				event,
			)
		case <-ctx.Done():
			return fmt.Errorf(
				"%w: waiting for handlers: %w",
				asynxmd.ErrContextCancelled,
				ctx.Err(),
			)
		}
	}
}

func settledOutcome[T any](
	t asynxmd.Ticket,
	event asynxmd.Event[T],
) error {
	err := t.Unrun()
	if err == nil {
		return nil
	}

	return fmt.Errorf(
		"%w: event %s v%d is pending, no handler ran",
		err,
		event.AggregateID,
		event.Version,
	)
}
