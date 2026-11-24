package models

import (
	"context"
	"time"
)

// TimeoutHandler is called when a ProjectionHandler exceeds its timeout (see
// WithHandlerTimeout). It receives the same context that was passed to
// Publish, the event and the timeout that was exceeded. It is an alarm, not a
// cancellation: the slow handler is not stopped and, on the dispatcher path,
// is still waited for.
type TimeoutHandler[T any] func(
	context.Context,
	Event[T],
	time.Duration,
)
