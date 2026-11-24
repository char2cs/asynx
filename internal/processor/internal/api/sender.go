package api

import (
	"context"

	"github.com/char2cs/asynx/v2/models/command"
	"github.com/char2cs/asynx/v2/models/event"
)

// Sender is the write side of the processor: commands in, committed events out.
// Commands for the same aggregate are serialised; those for different
// aggregates may run in parallel. All methods are safe for concurrent use and
// refuse new work with ErrShuttingDown once Shutdown has begun.
type Sender[T any] interface {
	// Send runs cmd and returns its event once it is committed and queued for
	// delivery, without waiting for handlers. It returns ErrQueueFull when the
	// shard has no room and ErrContextCancelled when ctx ends first.
	Send(
		ctx context.Context,
		cmd command.Command[T],
	) (event.Event[T], error)
	// SendWait is Send that also waits for the event's handlers to run once,
	// returning ErrSendWaitFromHandler when called from a handler of the same
	// aggregate. When ctx ends while waiting it returns the committed event
	// with ErrContextCancelled.
	SendWait(
		ctx context.Context,
		cmd command.Command[T],
	) (event.Event[T], error)
	// Forget delivers a tombstone for the aggregate and then erases its
	// stream, snapshot and delivery entries. A later command for it is
	// rejected with ErrForgetInProgress while the erase runs.
	Forget(
		ctx context.Context,
		aggregateID string,
	) error
}
