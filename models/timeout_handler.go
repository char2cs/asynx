package models

import (
	"context"
	"time"
)

// TimeoutHandler is called when a ProjectionHandler exceeds its timeout.
// It receives the same context that was passed to Publish.
type TimeoutHandler[T any] func(
	context.Context,
	Event[T],
	time.Duration,
)
