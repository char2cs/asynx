package handler

import (
	"context"
	"time"

	"github.com/char2cs/asynx/v2/models/event"
)

// Timeout is called when a Projection exceeds its timeout (see
// subscription.WithHandlerTimeout). It receives the same context that was passed to
// Publish, the event and the timeout that was exceeded. It is an alarm, not a
// cancellation: the slow handler is not stopped and, on the dispatcher path,
// is still waited for.
type Timeout[T any] func(
	context.Context,
	event.Event[T],
	time.Duration,
)
