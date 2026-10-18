package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type parkIgnoringDispatcher struct {
	models.Dispatcher[Aggregate]
}

func queueWhileParked(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return parkIgnoringDispatcher{Dispatcher: inner}
}

func (d parkIgnoringDispatcher) Enqueue(
	ctx context.Context,
	r models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	_, isParked := d.Parks()[r.Event.AggregateID]
	if !isParked || r.Kind != models.DispatchEvent || r.Ack == nil {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	r.Ack = nil
	return d.Dispatcher.Enqueue(ctx, r)
}
