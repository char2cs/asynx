package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// SyncProjection is a read-model update that runs inside the transaction that
// appends an event, so projection and event commit or roll back together. The
// executor calls Match with each event name and, when it accepts, Fn with the
// transaction; an error from Fn aborts the whole write.
type SyncProjection[T any] struct {
	// Match reports whether the projection applies to an event name.
	Match func(eventName string) bool
	// Fn writes the projection's own state through tx.Native().
	Fn func(ctx context.Context, tx asynxmd.Tx, evt asynxmd.Event[T]) error
}
