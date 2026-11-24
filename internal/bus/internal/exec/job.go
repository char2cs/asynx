package exec

import (
	"github.com/char2cs/asynx/internal/bus/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// MakeJob builds an exec.HandlerJob from a models.Subscription, an event,
// and optional per-job callback overrides.
func MakeJob[T any](
	sub *models.Subscription[T],
	event asynxmd.Event[T],
	onPanic asynxmd.PanicHandler[T],
	onTimeout asynxmd.TimeoutHandler[T],
) *HandlerJob[T] {
	return &HandlerJob[T]{
		Handler:         sub.Handler,
		FallbackHandler: sub.FallbackHandler,
		Timeout:         sub.Timeout,
		Event:           event,
		OnPanic:         onPanic,
		OnTimeout:       onTimeout,
	}
}
