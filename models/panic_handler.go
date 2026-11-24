package models

import (
	"context"
)

// PanicHandler is called when a ProjectionHandler panics.
// It receives the same context that was passed to Publish.
type PanicHandler[T any] func(
	context.Context,
	Event[T],
	any,
)
