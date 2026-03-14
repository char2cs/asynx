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
	"errors"
	"fmt"
	"sync"

	"github.com/char2cs/asynx/internal/eventstore"
	asynxmd "github.com/char2cs/asynx/models"
)

type CommandExecutor[T any] struct {
	es        *eventstore.EventStore[T]
	bus       asynxmd.Bus[T]
	publishWg sync.WaitGroup
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
	event, err := e.es.Write(ctx, cmd)
	if err != nil {
		if errors.Is(err, asynxmd.ErrValidation) {
			return err
		}

		return fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}

	e.publishAsync(ctx, event)
	return nil
}

func (e *CommandExecutor[T]) publishAsync(
	ctx context.Context,
	event asynxmd.Event[T],
) {
	if e.bus == nil {
		return
	}

	e.publishWg.Add(1)
	go func() {
		defer e.publishWg.Done()
		publishCtx := context.WithoutCancel(ctx)
		_ = e.bus.Publish(
			publishCtx,
			event,
		)
	}()
}

// ForTesting: WaitPublish blocks until all publishAsync goroutines complete.
// Do not call in production code.
func (e *CommandExecutor[T]) WaitPublish() {
	e.publishWg.Wait()
}
