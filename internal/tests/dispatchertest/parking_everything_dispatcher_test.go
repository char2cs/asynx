package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type parkingEverythingDispatcher struct {
	hollowDispatcher
}

func parkingEverything(
	models.DispatcherConfig[Aggregate],
) (models.Dispatcher[Aggregate], error) {
	return parkingEverythingDispatcher{}, nil
}

func (parkingEverythingDispatcher) Enqueue(
	context.Context,
	models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	done := make(chan struct{})
	close(done)
	return models.Ticket{
		Settled: done,
		Final:   done,
		Err:     func() error { return models.ErrDeliveryParked },
		Unrun:   func() error { return models.ErrDeliveryParked },
	}, nil
}

func (parkingEverythingDispatcher) TicketFor(
	string,
	int64,
) (models.Ticket, bool) {
	return models.Ticket{}, true
}

func (parkingEverythingDispatcher) Unrun(
	string,
	int64,
) error {
	return models.ErrDeliveryParked
}

func (parkingEverythingDispatcher) Parks() map[string]uint64 {
	return map[string]uint64{"a": 1, "ghost": 1}
}

func (parkingEverythingDispatcher) ResumeUndelivered(
	string,
	uint64,
) (bool, []int64) {
	return true, []int64{1}
}

func (parkingEverythingDispatcher) InHandler(
	context.Context,
	string,
) bool {
	return true
}

func (parkingEverythingDispatcher) Interrupted() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
