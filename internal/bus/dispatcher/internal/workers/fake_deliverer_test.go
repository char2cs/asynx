package workers

import (
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type fakeDeliverer struct {
	journal  *journal
	registry aggregates.Registry[jobs.Job[int]]
	deliver  func(job jobs.Job[int]) error
}

func (d *fakeDeliverer) Deliver(
	_ string,
	_ <-chan struct{},
	job jobs.Job[int],
) error {
	d.journal.add("deliver")
	if d.deliver == nil {
		return nil
	}
	return d.deliver(job)
}

func (d *fakeDeliverer) ParkThenReport(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[int],
	cause error,
	report func(),
) {
	d.journal.add("park")
	d.registry.ParkIfCurrent(aggregateID, halt, job.Acked(), cause)
	report()
}
