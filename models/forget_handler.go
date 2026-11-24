package models

import (
	"context"
)

// ForgetHandler is called when an aggregate is forgotten, before its data is
// erased, so that a live subscriber can drop what it derived from it. It
// receives the tombstone event, whose EventName is ForgetEventName;
// Event.Aggregate holds the aggregate's last known state. The tombstone is
// delivered again if Forget is retried after a failure, so a ForgetHandler must
// tolerate being called more than once for one aggregate.
type ForgetHandler[T any] func(
	context.Context,
	Event[T],
)
