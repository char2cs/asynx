package mocks

import (
	"context"

	"github.com/char2cs/asynx"
)

type Bus[T any] struct{}

func (b *Bus[T]) Publish(
	_ context.Context,
	_ asynx.Event[T],
) error {
	return nil
}

func (b *Bus[T]) Subscribe(
	_ string,
	_ func(asynx.Event[T]),
) (string, error) {
	return "id", nil
}

func (b *Bus[T]) Unsubscribe(_ string) error {
	return nil
}

func (b *Bus[T]) Close(_ context.Context) error {
	return nil
}
