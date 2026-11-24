package models

import (
	"context"

	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
)

// TxHook runs inside the append's transaction, after the event is appended.
// Returning an error rolls the transaction back, discarding the event.
type TxHook[T any] func(
	ctx context.Context,
	tx storage.Tx,
	evt event.Event[T],
) error
