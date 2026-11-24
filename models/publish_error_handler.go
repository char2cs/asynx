package models

import (
	"context"
)

// PublishErrorHandler is called when a publish of a committed event fails
// inside the goroutine asynx started for it, when the acknowledgement of its
// delivery fails, or when the dispatcher itself panics while handling it. The
// event has already been durably written to the store, so the error never
// means the event was lost; the callback is for observability only. It
// receives the context of the publish, the event and the error. When nil,
// publish errors are silently dropped. A panic in it is swallowed.
type PublishErrorHandler[T any] func(
	context.Context,
	Event[T],
	error,
)
