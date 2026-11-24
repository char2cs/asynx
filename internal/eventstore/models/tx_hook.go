package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// TxHook runs inside the append's transaction, after the event is appended.
// Returning an error rolls the transaction back, discarding the event.
type TxHook[T any] func(
	ctx context.Context,
	tx asynxmd.Tx,
	evt asynxmd.Event[T],
) error
