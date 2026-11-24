package subscription

import (
	"time"

	"github.com/char2cs/asynx/v2/models/handler"
)

// Config holds the options of one subscription. It is what a
// Opt writes into; pubsub.Bus implementations read it when they register
// the subscription.
//
// Fallback, when set, is invoked with the same event after the primary handler
// panicked. Timeout, when positive, is the
// duration after which a still-running handler is reported to the
// handler.Timeout; zero means no timeout.
type Config[T any] struct {
	Fallback handler.Projection[T]
	Timeout  time.Duration
}
