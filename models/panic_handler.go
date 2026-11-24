package models

import (
	"context"
)

// PanicHandler is called when a ProjectionHandler panics. It receives the
// same context that was passed to Publish, the event the handler was given and
// the value the handler panicked with. The panic has been recovered by then, so
// the publisher never sees it; the handler is for observability, and is
// passed the panic value as the recover call returned it.
type PanicHandler[T any] func(
	context.Context,
	Event[T],
	any,
)
