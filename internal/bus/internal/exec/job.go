package exec

import (
	"github.com/char2cs/asynx/internal/bus/models"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/handler"
)

// MakeJob builds the HandlerJob that delivers event to the handler registered
// by sub. The handler, fallback and timeout are copied from the subscription;
// onPanic and onTimeout are the bus-level observers and may be nil, in which
// case Handler falls back to its default stderr reporters. The returned job is
// fresh on every call, so the caller may adjust it (for instance set
// WaitAfterTimeout) before handing it to Handler.
func MakeJob[T any](
	sub *models.Subscription[T],
	evt event.Event[T],
	onPanic handler.Panic[T],
	onTimeout handler.Timeout[T],
) *HandlerJob[T] {
	return &HandlerJob[T]{
		Handler:         sub.Handler,
		FallbackHandler: sub.FallbackHandler,
		Timeout:         sub.Timeout,
		Event:           evt,
		OnPanic:         onPanic,
		OnTimeout:       onTimeout,
	}
}
