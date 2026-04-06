// Package exec implements the command execution pipeline for Asynx.
//
// CommandExecutor[T] applies the three-phase command processing pattern:
//   - Load    — Fetch current aggregate state from EventStore; nil if not found
//   - Validate — Call Command.Validate(state); return raw error if rejected
//   - Write   — Call EventStore.Write; wrap storage errors as ErrPipelineFailed
//   - Publish — Call Bus.Publish asynchronously (waitHandlers=false) or
//               Bus.PublishSync (waitHandlers=true) with detached context
//
// Validation errors short-circuit at phase 2; no event is written. Storage errors
// at phase 3 wrap to ErrPipelineFailed. Event publishing always uses
// context.WithoutCancel to survive caller's deadline.
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
	nextVersion int64, // reserved for future optimistic-locking enforcement
	waitHandlers bool,
) (asynxmd.Event[T], error) {
	event, err := e.es.Write(ctx, cmd)
	if err != nil {
		if errors.Is(err, asynxmd.ErrValidation) {
			return asynxmd.Event[T]{}, err
		}

		return asynxmd.Event[T]{}, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}

	if waitHandlers {
		e.publishSync(ctx, event)
	} else {
		e.publishAsync(ctx, event)
	}

	return event, nil
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

func (e *CommandExecutor[T]) publishSync(
	ctx context.Context,
	event asynxmd.Event[T],
) {
	if e.bus == nil {
		return
	}

	publishCtx := context.WithoutCancel(ctx)
	// PublishSync uses a WithoutCancel context so ctx.Err() errors are impossible.
	// ErrBusClosed means Shutdown raced with this command; the event is already
	// durably written so we proceed.
	_ = e.bus.PublishSync(publishCtx, event)
}

// ForTesting: WaitPublish blocks until all publishAsync goroutines complete.
// Do not call in production code.
func (e *CommandExecutor[T]) WaitPublish() {
	e.publishWg.Wait()
}
