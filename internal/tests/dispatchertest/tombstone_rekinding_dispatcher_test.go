package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type tombstoneRekindingDispatcher struct {
	models.Dispatcher[Aggregate]
}

func treatForgetNamesAsTombstones(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return tombstoneRekindingDispatcher{Dispatcher: inner}
}

func (d tombstoneRekindingDispatcher) Enqueue(
	ctx context.Context,
	r models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	if r.Kind != models.DispatchEvent || r.Event.EventName != models.ForgetEventName {
		return d.Dispatcher.Enqueue(ctx, r)
	}
	r.Kind = models.DispatchTombstone
	return d.Dispatcher.Enqueue(ctx, r)
}
