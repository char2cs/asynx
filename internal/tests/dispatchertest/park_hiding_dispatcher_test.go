package dispatchertest

import (
	"context"
	"errors"

	"github.com/char2cs/asynx/models"
)

type parkHidingDispatcher struct {
	models.Dispatcher[Aggregate]
}

func hideParks(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return parkHidingDispatcher{Dispatcher: inner}
}

func (d parkHidingDispatcher) Enqueue(
	ctx context.Context,
	r models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	tk, err := d.Dispatcher.Enqueue(ctx, r)
	return hideParked(tk), err
}

func (d parkHidingDispatcher) TicketFor(
	aggregateID string,
	version int64,
) (models.Ticket, bool) {
	tk, found := d.Dispatcher.TicketFor(aggregateID, version)
	return hideParked(tk), found
}

func (d parkHidingDispatcher) Unrun(
	aggregateID string,
	version int64,
) error {
	return unparked(d.Dispatcher.Unrun(aggregateID, version))
}

func hideParked(
	tk models.Ticket,
) models.Ticket {
	errOf, unrunOf := tk.Err, tk.Unrun
	tk.Err = func() error { return unparked(errOf()) }
	tk.Unrun = func() error { return unparked(unrunOf()) }
	return tk
}

func unparked(
	err error,
) error {
	if errors.Is(err, models.ErrDeliveryParked) {
		return nil
	}
	return err
}
