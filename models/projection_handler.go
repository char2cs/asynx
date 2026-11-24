package models

import (
	"context"
)

// ProjectionHandler is a subscription callback that receives the publishing
// context and the event that triggered it.
type ProjectionHandler[T any] func(
	context.Context,
	Event[T],
)
