package workers

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(
	entry string,
) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, entry)
}

func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.entries)
}

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
	report func(),
) {
	d.journal.add("park")
	d.registry.ParkIfCurrent(aggregateID, halt, job.Acked())
	report()
}

type fakeReporter struct {
	journal *journal
	mu      sync.Mutex
	errs    []error
}

func (r *fakeReporter) PublishError(
	_ jobs.Job[int],
	err error,
) {
	r.journal.add("report")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *fakeReporter) DeliveryError(
	jobs.Job[int],
	error,
	int,
) {
}

func (r *fakeReporter) Panic(
	jobs.Job[int],
	any,
) {
}

func (r *fakeReporter) published() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errs)
}

type fakeBus struct {
	journal *journal
	publish func(event asynxmd.Event[int]) error
}

func (b *fakeBus) PublishSync(
	_ context.Context,
	event asynxmd.Event[int],
) error {
	b.journal.add("live")
	if b.publish == nil {
		return nil
	}
	return b.publish(event)
}

func (b *fakeBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *fakeBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *fakeBus) Unsubscribe(
	string,
) error {
	return nil
}

func (b *fakeBus) Close(
	context.Context,
) error {
	return nil
}

func (b *fakeBus) WaitForHandlers() {
}

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
	job := jobs.New(request, ctx, cancel)
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
