// Package exec implements the command execution pipeline for Asynx.
//
// CommandExecutor[T] applies the three-phase command processing pattern:
//   - Load    — Fetch current aggregate state from EventStore; nil if not found
//   - Validate — Call Command.Validate(state); return raw error if rejected
//   - Write   — Call EventStore.Write; wrap storage errors as ErrPipelineFailed
//   - Publish — Call Bus.Publish asynchronously with detached context
//
// Validation errors short-circuit at phase 2; no event is written. Storage errors
// at phase 3 wrap to ErrPipelineFailed. Event publishing is always async and
// always uses context.WithoutCancel to survive caller's deadline.
package exec

import (
	"context"

	"github.com/char2cs/asynx/internal/eventstore"
	asynxmd "github.com/char2cs/asynx/models"
)

type CommandExecutor[T any] struct {
	es  *eventstore.EventStore[T]
	bus asynxmd.Bus[T]
}

func New[T any](
	es *eventstore.EventStore[T],
	bus asynxmd.Bus[T],
) *CommandExecutor[T] {
	return &CommandExecutor[T]{
		es:  es,
		bus: bus,
	}
}

func (e *CommandExecutor[T]) Execute(
	ctx context.Context,
	cmd asynxmd.Command[T],
	nextVersion int64,
) error {
	state, err := e.loadState(
		ctx,
		cmd,
	)
	if err != nil {
		return err
	}

	if err := cmd.Validate(state); err != nil {
		return err
	}

	event, err := e.es.Write(
		ctx,
		state,
		cmd,
	)
	if err != nil {
		return asynxmd.ErrPipelineFailed
	}

	e.publishAsync(
		ctx,
		event,
	)
	return nil
}

func (e *CommandExecutor[T]) loadState(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (*T, error) {
	state, err := e.es.Get(
		ctx,
		cmd.AggregateID(),
	)
	if err == asynxmd.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, asynxmd.ErrPipelineFailed
	}
	return &state, nil
}

func (e *CommandExecutor[T]) publishAsync(
	ctx context.Context,
	event asynxmd.Event[T],
) {
	if e.bus == nil {
		return
	}

	go func() {
		publishCtx := context.WithoutCancel(ctx)
		_ = e.bus.Publish(
			publishCtx,
			event,
		)
	}()
}
