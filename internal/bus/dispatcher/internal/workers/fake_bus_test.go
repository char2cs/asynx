package workers

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type fakeBus struct {
	journal *journal
	publish func(event asynxmd.Event[int]) error
}

func (b *fakeBus) PublishSync(
	_ context.Context,
	event asynxmd.Event[int],
) error {
	b.journal.add("live")
	if b.publish == nil {
		return nil
	}
	return b.publish(event)
}

func (b *fakeBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *fakeBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *fakeBus) Unsubscribe(
	string,
) error {
	return nil
}

func (b *fakeBus) Close(
	context.Context,
) error {
	return nil
}

func (b *fakeBus) WaitForHandlers() {
}
