package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type tombstoneDeadLetteringDispatcher struct {
	models.Dispatcher[Aggregate]
}

func deadLetterTombstones(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return tombstoneDeadLetteringDispatcher{Dispatcher: inner}
}

func (d tombstoneDeadLetteringDispatcher) Enqueue(
	ctx context.Context,
	r models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	if r.Kind != models.DispatchTombstone || r.Ack == nil {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	r.Kind = models.DispatchEvent
	return d.Dispatcher.Enqueue(ctx, r)
}
