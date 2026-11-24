package exec

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/v2/models/dispatch"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
)

func (e *CommandExecutor[T]) waitForAnswered(
	evt event.Event[T],
	answer error,
) func(context.Context) error {
	if answer != nil ||
		e.dispatcher == nil {
		return nil
	}

	t, underway := e.dispatcher.TicketFor(
		evt.AggregateID,
		evt.Version,
	)
	if underway {
		return waitOn(
			t,
			evt,
		)
	}

	return unrunWait(
		e.dispatcher.Unrun(
			evt.AggregateID,
			evt.Version,
		),
		evt,
	)
}

func unrunWait[T any](
	unrun error,
	evt event.Event[T],
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
			evt.AggregateID,
			evt.Version,
		)
	}
}

func waitOn[T any](
	t dispatch.Ticket,
	evt event.Event[T],
) func(context.Context) error {
	return func(
		ctx context.Context,
	) error {
		return awaitTicket(
			ctx,
			t,
			evt,
		)
	}
}

func awaitTicket[T any](
	ctx context.Context,
	t dispatch.Ticket,
	evt event.Event[T],
) error {
	select {
	case <-t.Settled:
		return settledOutcome(
			t,
			evt,
		)
	case <-ctx.Done():
		return fmt.Errorf(
			"%w: waiting for handlers: %w",
			failure.ErrContextCancelled,
			ctx.Err(),
		)
	}
}

func settledOutcome[T any](
	t dispatch.Ticket,
	evt event.Event[T],
) error {
	err := t.Unrun()
	if err == nil {
		return nil
	}

	return fmt.Errorf(
		"%w: event %s v%d is pending, no handler ran",
		err,
		evt.AggregateID,
		evt.Version,
	)
}
