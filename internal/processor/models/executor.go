package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// Executor runs commands through the write pipeline.
type Executor[T any] interface {
	// ExecuteAsync runs the pipeline and returns once the event is committed
	// and queued; the returned Wait lets the caller do the waiting.
	ExecuteAsync(
		ctx context.Context,
		cmd asynxmd.Command[T],
	) (asynxmd.Event[T], func(context.Context) error, error)
}
