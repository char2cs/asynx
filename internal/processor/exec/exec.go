// Package exec implements the command execution pipeline for Asynx.
//
// CommandExecutor[T] applies the three-phase command processing pattern:
//   - Load    — Fetch current aggregate state from EventStore; nil if not found
//   - Validate — Call Command.Validate(state); return raw error if rejected
//   - Write   — Call EventStore.Write; wrap storage errors as ErrPipelineFailed
//   - Dispatch — Call Dispatcher.Dispatch to deliver the event through the bus
//
// Validation errors short-circuit at phase 2; no event is written. Storage errors
// at phase 3 wrap to ErrPipelineFailed. Event dispatching uses the Dispatcher's
// per-aggregate ordered delivery via context.WithoutCancel to survive caller's deadline.
package exec

import (
	"context"
	"errors"
	"fmt"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	asynxmd "github.com/char2cs/asynx/models"
)

type CommandExecutor[T any] struct {
	es         *eventstore.EventStore[T]
	dispatcher *dispatcher.Dispatcher[T]
}

func New[T any](
	es *eventstore.EventStore[T],
	d *dispatcher.Dispatcher[T],
) *CommandExecutor[T] {
	return &CommandExecutor[T]{es: es, dispatcher: d}
}

func (e *CommandExecutor[T]) Execute(
	ctx context.Context,
	cmd asynxmd.Command[T],
	waitHandlers bool,
) (asynxmd.Event[T], error) {
	event, err := e.es.Write(ctx, cmd)
	if err != nil {
		if errors.Is(err, asynxmd.ErrValidation) {
			return asynxmd.Event[T]{}, err
		}

		return asynxmd.Event[T]{}, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}

	if e.dispatcher != nil {
		if err := e.dispatcher.Dispatch(ctx, event, waitHandlers); err != nil {
			return event, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
		}
	}

	return event, nil
}
