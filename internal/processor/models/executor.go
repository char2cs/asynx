package models

import (
	"context"

	"github.com/char2cs/asynx/v2/models/command"
	"github.com/char2cs/asynx/v2/models/event"
)

// Executor is the write pipeline an envelope runs its command through. The
// exec package's CommandExecutor satisfies it; the interface keeps models
// independent of exec and lets tests substitute a double. Implementations must
// be safe for concurrent use: every sender calls it from its own goroutine.
type Executor[T any] interface {
	// ExecuteAsync runs the pipeline and returns once the event is committed
	// and queued; the returned Wait lets the caller do the waiting.
	ExecuteAsync(
		ctx context.Context,
		cmd command.Command[T],
	) (event.Event[T], func(context.Context) error, error)
}
