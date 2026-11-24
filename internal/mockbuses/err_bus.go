package mockbuses

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// ErrBus is a Bus implementation that returns Err for Publish and PublishSync.
// All other operations succeed silently.
type ErrBus[T any] struct{ Err error }

func (b *ErrBus[T]) Publish(
	_ context.Context,
	_ models.Event[T],
) error {
	return b.Err
}

func (b *ErrBus[T]) PublishSync(
	_ context.Context,
	_ models.Event[T],
) error {
	return b.Err
}

func (b *ErrBus[T]) Subscribe(
	_ string,
	_ models.ProjectionHandler[T],
	_ ...models.SubscriptionOpt[T],
) (string, error) {
	return "",
		nil
}

func (b *ErrBus[T]) Unsubscribe(
	_ string,
) error {
	return nil
}

func (b *ErrBus[T]) Close(
	_ context.Context,
) error {
	return nil
}

func (b *ErrBus[T]) WaitForHandlers() {}
