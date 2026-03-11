package models

import (
	"context"
	"time"
)

// ProjectionHandler is a subscription callback that receives the publishing
// context and the event that triggered it.
type ProjectionHandler[T any] func(
	context.Context,
	Event[T],
)

// PanicHandler is called when a ProjectionHandler panics.
// It receives the same context that was passed to Publish.
type PanicHandler[T any] func(
	context.Context,
	Event[T],
	any,
)

// TimeoutHandler is called when a ProjectionHandler exceeds its timeout.
// It receives the same context that was passed to Publish.
type TimeoutHandler[T any] func(
	context.Context,
	Event[T],
	time.Duration,
)
