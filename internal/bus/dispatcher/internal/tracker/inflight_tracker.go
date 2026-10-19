package tracker

import (
	"sync"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type inflightTracker[T any] struct {
	inflight sync.Map
	tickets  sync.Map
}

func (t *inflightTracker[T]) Track(
	job jobs.Job[T],
) {
	key := ticketKey{job.AggregateID(), job.Version()}
	t.inflight.Store(job, struct{}{})
	t.tickets.Store(key, job)
	job.OnComplete(func() {
		t.inflight.Delete(job)
		t.tickets.CompareAndDelete(key, job)
	})
}

func (t *inflightTracker[T]) For(
	aggregateID string,
	version int64,
) (asynxmd.Ticket, bool) {
	found, ok := t.tickets.Load(ticketKey{aggregateID, version})
	if !ok {
		return asynxmd.Ticket{}, false
	}
	return found.(jobs.Job[T]).Ticket(), true
}

func (t *inflightTracker[T]) Idle() {
	var tracked []jobs.Job[T]
	t.inflight.Range(func(
		key any,
		_ any,
	) bool {
		tracked = append(tracked, key.(jobs.Job[T]))
		return true
	})
	for _, job := range tracked {
		<-job.Ticket().Final
	}
}
