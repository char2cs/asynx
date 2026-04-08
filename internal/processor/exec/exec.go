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
	es  *eventstore.EventStore[T]
	bus asynxmd.Bus[T]

	// publishMu serialises access to pending and wakes WaitPublish callers.
	// We cannot use sync.WaitGroup here: its docs prohibit a new Add when the
	// counter is zero and a concurrent Wait is returning, which is exactly what
	// happens when a projection handler fires a new command from inside
	// bus.Publish (i.e. the Quiver pattern).
	publishMu       sync.Mutex
	pending         int
	publishCv       *sync.Cond
	onPublishError  asynxmd.PublishErrorHandler[T]
}

// CommandExecutorOpt is a functional option for New.
type CommandExecutorOpt[T any] func(*CommandExecutor[T])

// WithPublishErrorHandler sets a callback invoked when Bus.Publish returns a
// non-nil error inside an async publish goroutine. When not set, publish
// errors are silently dropped.
func WithPublishErrorHandler[T any](fn asynxmd.PublishErrorHandler[T]) CommandExecutorOpt[T] {
	return func(e *CommandExecutor[T]) {
		e.onPublishError = fn
	}
}

func New[T any](
	es *eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	opts ...CommandExecutorOpt[T],
) *CommandExecutor[T] {
	e := &CommandExecutor[T]{es: es, bus: bus}
	e.publishCv = sync.NewCond(&e.publishMu)
	for _, opt := range opts {
		opt(e)
	}
	return e
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

	// Increment before spawning: WaitPublish must never see pending==0 while
	// the goroutine is still in flight.
	e.publishMu.Lock()
	e.pending++
	e.publishMu.Unlock()

	go func() {
		defer func() {
			e.publishMu.Lock()
			e.pending--
			if e.pending == 0 {
				e.publishCv.Broadcast()
			}
			e.publishMu.Unlock()
		}()
		publishCtx := context.WithoutCancel(ctx)
		if err := e.bus.Publish(publishCtx, event); err != nil && e.onPublishError != nil {
			e.onPublishError(publishCtx, event, err)
		}
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
	e.publishMu.Lock()
	defer e.publishMu.Unlock()
	for e.pending > 0 {
		e.publishCv.Wait()
	}
}
