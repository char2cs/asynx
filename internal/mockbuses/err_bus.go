package mockbuses

import (
	"context"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/handler"
	"github.com/char2cs/asynx/models/subscription"
)

// ErrBus is a Bus implementation that returns Err for Publish and PublishSync.
// All other operations succeed silently.
type ErrBus[T any] struct{ Err error }

// Publish ignores the event and context and returns Err on every call, so
// tests can exercise the failure path of an asynchronous publish. Nothing is
// delivered to any subscriber.
func (b *ErrBus[T]) Publish(
	_ context.Context,
	_ event.Event[T],
) error {
	return b.Err
}

// PublishSync ignores the event and context and returns Err on every call, so
// tests can exercise the failure path of a synchronous publish. Nothing is
// delivered to any subscriber.
func (b *ErrBus[T]) PublishSync(
	_ context.Context,
	_ event.Event[T],
) error {
	return b.Err
}

// Subscribe registers nothing and returns an empty ID.
func (b *ErrBus[T]) Subscribe(
	_ string,
	_ handler.Projection[T],
	_ ...subscription.Opt[T],
) (string, error) {
	return "",
		nil
}

// Unsubscribe does nothing and succeeds.
func (b *ErrBus[T]) Unsubscribe(
	_ string,
) error {
	return nil
}

// Close does nothing and succeeds.
func (b *ErrBus[T]) Close(
	_ context.Context,
) error {
	return nil
}

// WaitForHandlers returns immediately.
func (b *ErrBus[T]) WaitForHandlers() {}
