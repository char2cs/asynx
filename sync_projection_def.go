package asynx

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type syncProjectionDef[T any] struct {
	pattern string
	fn      func(context.Context, models.Tx, models.Event[T]) error
}
