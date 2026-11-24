package asynx

import (
	"context"

	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/storage"
)

type syncProjectionDef[T any] struct {
	pattern string
	fn      func(context.Context, storage.Tx, event.Event[T]) error
}
