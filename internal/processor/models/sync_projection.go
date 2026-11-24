package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// SyncProjection is a projection that runs inside the event's transaction.
type SyncProjection[T any] struct {
	// Match reports whether the projection applies to an event name.
	Match func(eventName string) bool
	// Fn writes the projection's own state through tx.Native().
	Fn func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error
}
