package dispatcher

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type callbackBus struct {
	fn func(
		context.Context,
		asynxmd.Event[int],
	) error
}

func (b *callbackBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	return b.fn(ctx, e)
}

func (b *callbackBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *callbackBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *callbackBus) Unsubscribe(string) error { return nil }

func (b *callbackBus) Close(context.Context) error { return nil }

func (b *callbackBus) WaitForHandlers() {}
