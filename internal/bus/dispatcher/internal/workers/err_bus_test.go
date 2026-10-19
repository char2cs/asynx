package workers_test

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type errBus struct {
	err error
}

func (b *errBus) PublishSync(
	context.Context,
	asynxmd.Event[int],
) error {
	return b.err
}

func (b *errBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *errBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *errBus) Unsubscribe(string) error { return nil }

func (b *errBus) Close(context.Context) error { return nil }

func (b *errBus) WaitForHandlers() {}
