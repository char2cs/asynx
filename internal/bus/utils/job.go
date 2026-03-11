package utils

import (
	"context"

	"github.com/char2cs/asynx/internal/bus/exec"
	"github.com/char2cs/asynx/internal/bus/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// MakeJob builds an exec.HandlerJob from a models.Subscription, an event,
// a context, and optional per-job callback overrides.
func MakeJob[T any](
	sub *models.Subscription[T],
	event asynxmd.Event[T],
	ctx context.Context,
	onPanic asynxmd.PanicHandler[T],
	onTimeout asynxmd.TimeoutHandler[T],
) *exec.HandlerJob[T] {
	return &exec.HandlerJob[T]{
		Handler:         sub.Handler,
		FallbackHandler: sub.FallbackHandler,
		Timeout:         sub.Timeout,
		Event:           event,
		Ctx:             ctx,
		OnPanic:         onPanic,
		OnTimeout:       onTimeout,
	}
}
