package mockbuses

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// Bus is a no-op models.Bus: every operation succeeds and nothing is delivered.
type Bus[T any] struct{}

// Publish discards the event and succeeds.
func (b *Bus[T]) Publish(
	_ context.Context,
	_ models.Event[T],
) error {
	return nil
}

// PublishSync discards the event and succeeds.
func (b *Bus[T]) PublishSync(
	_ context.Context,
	_ models.Event[T],
) error {
	return nil
}

// Subscribe registers nothing and returns a fixed ID.
func (b *Bus[T]) Subscribe(
	_ string,
	_ models.ProjectionHandler[T],
	_ ...models.SubscriptionOpt[T],
) (string, error) {
	return "id",
		nil
}

// Unsubscribe does nothing and succeeds.
func (b *Bus[T]) Unsubscribe(
	_ string,
) error {
	return nil
}

// Close does nothing and succeeds.
func (b *Bus[T]) Close(
	_ context.Context,
) error {
	return nil
}

// WaitForHandlers returns immediately.
func (b *Bus[T]) WaitForHandlers() {
}
