package mocks

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type Bus[T any] struct{}

func (b *Bus[T]) Publish(
	_ context.Context,
	_ models.Event[T],
) error {
	return nil
}

func (b *Bus[T]) PublishSync(
	_ context.Context,
	_ models.Event[T],
) error {
	return nil
}

func (b *Bus[T]) Subscribe(
	_ string,
	_ models.ProjectionHandler[T],
	_ ...models.SubscriptionOpt[T],
) (string, error) {
	return "id", nil
}

func (b *Bus[T]) Unsubscribe(_ string) error {
	return nil
}

func (b *Bus[T]) Close(_ context.Context) error {
	return nil
}

func (b *Bus[T]) WaitForHandlers() {
	// No-op for mock bus
}
