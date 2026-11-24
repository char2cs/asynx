package models

import (
	"context"
)

// PublishErrorHandler is called when Bus.Publish returns a non-nil error
// inside an async publish goroutine. The event has already been durably
// written to the event store; this callback is for observability only.
// When nil, publish errors are silently dropped.
type PublishErrorHandler[T any] func(
	context.Context,
	Event[T],
	error,
)
