package api

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// Sender runs commands and returns their events.
type Sender[T any] interface {
	// Send runs cmd and returns its event once it is committed.
	Send(
		ctx context.Context,
		cmd asynxmd.Command[T],
	) (asynxmd.Event[T], error)
	// SendWait is Send that also waits for the event's handlers to run once.
	SendWait(
		ctx context.Context,
		cmd asynxmd.Command[T],
	) (asynxmd.Event[T], error)
	// Forget erases the aggregate.
	Forget(
		ctx context.Context,
		aggregateID string,
	) error
}
