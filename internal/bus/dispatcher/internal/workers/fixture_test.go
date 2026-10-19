package workers

import (
	"context"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type fixture struct {
	journal   *journal
	registry  aggregates.Registry[jobs.Job[int]]
	deliverer *fakeDeliverer
	reports   *fakeReporter
	bus       *fakeBus
	pool      Pool[int]
}

func newFixture(
	t *testing.T,
	idleTimeout time.Duration,
) *fixture {
	f := &fixture{journal: &journal{}}
	f.registry = aggregates.New(aggregates.Hooks[jobs.Job[int]]{Spawn: f.spawn})
	f.deliverer = &fakeDeliverer{journal: f.journal, registry: f.registry}
	f.reports = &fakeReporter{journal: f.journal}
	f.bus = &fakeBus{journal: f.journal}
	f.pool = New[int](f.bus, f.registry, f.deliverer, f.reports, idleTimeout)
	t.Cleanup(f.stop)
	return f
}

func (f *fixture) spawn(
	aggregateID string,
) {
	f.pool.Spawn(aggregateID)
}

func (f *fixture) stop() {
	if !f.registry.Close() {
		return
	}
	f.pool.Stop()
	f.pool.Wait()
}

func (f *fixture) enqueue(
	t *testing.T,
	request asynxmd.DispatchRequest[int],
) asynxmd.Ticket {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	job := jobs.New(ctx, request, cancel)
	if _, err := f.registry.Enqueue(job); err != nil {
		t.Fatal(err)
	}
	return job.Ticket()
}

func event(
	version int64,
	ack func(context.Context) error,
) asynxmd.DispatchRequest[int] {
	return asynxmd.DispatchRequest[int]{
		Event: asynxmd.Event[int]{AggregateID: "a", Version: version, EventName: "test.event"},
		Ack:   ack,
	}
}

func final(
	t *testing.T,
	ticket asynxmd.Ticket,
) error {
	t.Helper()
	select {
	case <-ticket.Final:
		return ticket.Err()
	case <-time.After(3 * time.Second):
		t.Fatal("the job never completed")
		return nil
	}
}
