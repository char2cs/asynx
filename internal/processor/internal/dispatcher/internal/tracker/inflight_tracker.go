package tracker

import (
	"sync"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

type inflightTracker[T any] struct {
	inflight sync.Map
	tickets  sync.Map
}

// Track registers job until it completes.
func (t *inflightTracker[T]) Track(
	job contracts.Job[T],
) {
	key := ticketKey{job.AggregateID(), job.Version()}
	t.inflight.Store(
		job,
		struct{}{},
	)
	t.tickets.Store(
		key,
		job,
	)
	job.OnComplete(func() {
		t.inflight.Delete(job)
		t.tickets.CompareAndDelete(
			key,
			job,
		)
	})
}

// For returns the ticket of the job tracked for (aggregateID, version).
func (t *inflightTracker[T]) For(
	aggregateID string,
	version int64,
) (asynxmd.Ticket, bool) {
	found, ok := t.tickets.Load(ticketKey{aggregateID, version})
	if !ok {
		return asynxmd.Ticket{},
			false
	}

	job, isJob := found.(contracts.Job[T])
	if !isJob {
		return asynxmd.Ticket{},
			false
	}

	return job.Ticket(),
		true
}

// Idle blocks until every job tracked when it was called has completed.
func (t *inflightTracker[T]) Idle() {
	for _, job := range t.tracked() {
		<-job.Ticket().Final
	}
}

func (t *inflightTracker[T]) tracked() (jobs []contracts.Job[T]) {
	t.inflight.Range(func(
		key any,
		_ any,
	) bool {
		jobs = appendJob(
			jobs,
			key,
		)
		return true
	})
	return jobs
}

func appendJob[T any](
	jobs []contracts.Job[T],
	key any,
) []contracts.Job[T] {
	job, isJob := key.(contracts.Job[T])
	if !isJob {
		return jobs
	}

	return append(
		jobs,
		job,
	)
}
