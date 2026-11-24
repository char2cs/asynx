package models

import (
	"context"
)

// ForgetHandler is called when an aggregate is forgotten.
// It receives the tombstone event; Event.Aggregate holds the aggregate's last known state.
type ForgetHandler[T any] func(
	context.Context,
	Event[T],
)
