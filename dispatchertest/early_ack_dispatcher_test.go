package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type earlyAckDispatcher struct {
	models.Dispatcher[Aggregate]
}

func ackEarly(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return earlyAckDispatcher{Dispatcher: inner}
}

func (d earlyAckDispatcher) Enqueue(
	ctx context.Context,
	r models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	if r.Ack == nil {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	_ = r.Ack(ctx)
	r.Ack = acknowledge
	return d.Dispatcher.Enqueue(ctx, r)
}
