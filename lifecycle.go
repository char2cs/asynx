package asynx

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// Lifecycle registers forget handlers, shuts the instance down and drains pending events.
type Lifecycle[T any] interface {
	// OnForget registers a handler invoked when any aggregate is forgotten.
	// The handler receives the tombstone event; Event.Aggregate holds the last known state.
	// Returns a subscription ID that can be passed to Unsubscribe.
	OnForget(
		fn models.ForgetHandler[T],
	) (string, error)

	Shutdown(
		ctx context.Context,
	) error

	// WaitPublish blocks until every event queued when it was called has been
	// handled. It is for tests: under live traffic it still returns, but what it
	// waited for is only a moment's worth of events. Commands sent while it waits
	// are neither refused nor waited for.
	WaitPublish()
}
