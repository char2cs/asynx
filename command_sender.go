package asynx

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// CommandSender sends commands and forgets aggregates.
type CommandSender[T any] interface {
	// Send validates and persists the command, then returns the resulting Event[T]
	// once it is durably written. Projection handlers are fired asynchronously —
	// when Send returns, handlers may not have run yet.
	// Use SendWait when the caller needs projections to be up to date before proceeding.
	Send(
		ctx context.Context,
		cmd models.Command[T],
	) (models.Event[T], error)

	// SendWait behaves like Send but additionally blocks until all matching
	// projection handlers have completed. When SendWait returns without error,
	// the event is persisted and every projection subscribed to it has finished.
	// Use Send when projection consistency is not required at the call site.
	// The retry of an idempotent command (see IdempotentCommand) waits too, for
	// the handlers of the event its first attempt produced, if they are still
	// running, or reports ErrDeliveryParked if the event is still pending behind
	// a parked aggregate.
	SendWait(
		ctx context.Context,
		cmd models.Command[T],
	) (models.Event[T], error)

	// Forget writes a tombstone event for the aggregate, delivers it to all
	// ForgetHandlers and to every durable subscriber (each must succeed), then
	// erases all events, pending deliveries and snapshots. From the tombstone
	// until the erase finishes, commands for the aggregate are rejected with
	// ErrForgetInProgress, so none can slip in. Returns ErrValidation if the
	// aggregate does not exist. The tombstone's transaction also drops the
	// aggregate's pending deliveries: the deletion supersedes them. If Forget
	// fails before the erase (ctx done, a durable subscriber parked) the
	// aggregate keeps its tombstone and is parked (see PausedAggregates); call
	// Forget again, which delivers the handlers' notifications again. The
	// tombstone also stays in the delivery log until acknowledged, so
	// ResumeDeliveries or a restart delivers it ahead of anything newer. A handler
	// cannot Forget the aggregate it is handling (ErrForgetFromHandler); that is
	// recognised through the handler's ctx, so pass it on: a Forget made with an
	// unrelated context (context.Background()) waits behind the very handler that
	// called it, and only ctx, or the shutdown (which then fails it with
	// ErrShuttingDown), ends the wait.
	Forget(
		ctx context.Context,
		aggregateID string,
	) error
}
