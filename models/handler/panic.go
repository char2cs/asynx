package handler

import (
	"context"

	"github.com/char2cs/asynx/v2/models/event"
)

// Panic is called when a Projection panics. It receives the
// same context that was passed to Publish, the event the handler was given and
// the value the handler panicked with. The panic has been recovered by then, so
// the publisher never sees it; the handler is for observability, and is
// passed the panic value as the recover call returned it.
type Panic[T any] func(
	context.Context,
	event.Event[T],
	any,
)
