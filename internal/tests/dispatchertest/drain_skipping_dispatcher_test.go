package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type drainSkippingDispatcher struct {
	models.Dispatcher[Aggregate]
}

func skipDrain(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return drainSkippingDispatcher{Dispatcher: inner}
}

func (d drainSkippingDispatcher) Close(
	context.Context,
) error {
	go d.Dispatcher.Close(context.Background())
	return nil
}
