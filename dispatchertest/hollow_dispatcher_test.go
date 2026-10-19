package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type hollowDispatcher struct{}

func hollow(
	models.DispatcherConfig[Aggregate],
) (models.Dispatcher[Aggregate], error) {
	return hollowDispatcher{}, nil
}

func (hollowDispatcher) Enqueue(
	context.Context,
	models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	done := make(chan struct{})
	close(done)
	return models.Ticket{
		Settled: done,
		Final:   done,
		Err:     func() error { return nil },
		Unrun:   func() error { return nil },
	}, nil
}

func (hollowDispatcher) TicketFor(
	string,
	int64,
) (models.Ticket, bool) {
	return models.Ticket{}, false
}

func (hollowDispatcher) Unrun(
	string,
	int64,
) error {
	return nil
}

func (hollowDispatcher) Park(
	string,
	error,
	...int64,
) {
}

func (hollowDispatcher) Inspect(
	string,
) models.DeliveryStatus {
	return models.DeliveryStatus{}
}

func (hollowDispatcher) Supersede(
	string,
) {
}

func (hollowDispatcher) Parks() map[string]uint64 {
	return map[string]uint64{}
}

func (hollowDispatcher) ResumeUndelivered(
	string,
	uint64,
) (bool, []int64) {
	return false, nil
}

func (hollowDispatcher) InHandler(
	context.Context,
	string,
) bool {
	return false
}

func (hollowDispatcher) Interrupt() {}

func (hollowDispatcher) Interrupted() <-chan struct{} {
	return nil
}

func (hollowDispatcher) Close(
	context.Context,
) error {
	return nil
}

func (hollowDispatcher) WaitIdle() {}
