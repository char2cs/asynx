package asynx

import (
	"context"

	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
)

type syncProjectionDef[T any] struct {
	pattern string
	fn      func(context.Context, storage.Tx, event.Event[T]) error
}
