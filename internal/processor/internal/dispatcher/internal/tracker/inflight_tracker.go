package tracker

import (
	"sync"

	"github.com/char2cs/asynx/models/dispatch"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

type inflightTracker[T any] struct {
	mu       sync.Mutex
	inflight map[contracts.Job[T]]struct{}
	tickets  map[ticketKey]contracts.Job[T]
}

// Track registers job until it completes, under its (aggregate, version) key,
// and takes over the job's completion callback. When the job completes it
// removes itself, and never a job that has replaced it under the same key.
func (t *inflightTracker[T]) Track(
	job contracts.Job[T],
) {
	key := ticketKey{job.AggregateID(), job.Version()}
	t.mu.Lock()
	t.inflight[job] = struct{}{}
	t.tickets[key] = job
	t.mu.Unlock()
	job.OnComplete(func() {
		t.forget(
			key,
			job,
		)
	})
}

// For returns the ticket of the job tracked for (aggregateID, version), and
// false when there is none: it was never tracked or its delivery is over.
func (t *inflightTracker[T]) For(
	aggregateID string,
	version int64,
) (dispatch.Ticket, bool) {
	t.mu.Lock()
	job, ok := t.tickets[ticketKey{aggregateID, version}]
	t.mu.Unlock()
	if !ok {
		return dispatch.Ticket{},
			false
	}

	return job.Ticket(),
		true
}

// Idle blocks until every job tracked when it was called has completed. Jobs
// tracked while it waits are not waited for.
func (t *inflightTracker[T]) Idle() {
	for _, job := range t.tracked() {
		<-job.Ticket().Final
	}
}

func (t *inflightTracker[T]) tracked() (jobs []contracts.Job[T]) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for job := range t.inflight {
		jobs = append(
			jobs,
			job,
		)
	}

	return jobs
}

func (t *inflightTracker[T]) forget(
	key ticketKey,
	job contracts.Job[T],
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(
		t.inflight,
		job,
	)
	if t.tickets[key] == job {
		delete(
			t.tickets,
			key,
		)
	}
}
