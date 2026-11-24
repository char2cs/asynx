package handler

import (
	"context"

	"github.com/char2cs/asynx/models/event"
)

// Forget is called when an aggregate is forgotten, before its data is
// erased, so that a live subscriber can drop what it derived from it. It
// receives the tombstone event, whose EventName is event.ForgetEventName;
// event.Event.Aggregate holds the aggregate's last known state. The tombstone is
// delivered again if Forget is retried after a failure, so a Forget must
// tolerate being called more than once for one aggregate.
type Forget[T any] func(
	context.Context,
	event.Event[T],
)
