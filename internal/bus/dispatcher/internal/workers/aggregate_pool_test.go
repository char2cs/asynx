package workers

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

func TestPool_PublishesLiveThenDeliversThenAcks(t *testing.T) {
	f := newFixture(t, time.Second)
	ticket := f.enqueue(t, event(1, func(context.Context) error {
		f.journal.add("ack")
		return nil
	}))

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live", "deliver", "ack"}) {
		t.Fatalf("steps = %v, want live, deliver, ack", got)
	}
}

func TestPool_NeverAcksADeliveryThatDidNotSucceed(t *testing.T) {
	f := newFixture(t, time.Second)
	down := errors.New("down")
	f.deliverer.deliver = func(jobs.Job[int]) error { return down }
	var acks atomic.Int32
	ticket := f.enqueue(t, event(1, func(context.Context) error {
		acks.Add(1)
		return nil
	}))

	if err := final(t, ticket); !errors.Is(err, down) {
		t.Fatalf("final = %v, want the delivery error", err)
	}
	if acks.Load() != 0 {
		t.Fatal("a delivery that failed was acknowledged")
	}
}

func TestPool_AnAckFailureParksBeforeItIsReported(t *testing.T) {
	f := newFixture(t, time.Second)
	storeDown := errors.New("store down")
	ticket := f.enqueue(t, event(1, func(context.Context) error { return storeDown }))

	if err := final(t, ticket); !errors.Is(err, storeDown) {
		t.Fatalf("final = %v, want the ack error", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live", "deliver", "park", "report"}) {
		t.Fatalf("steps = %v, want the park before the report", got)
	}
	if _, parked := f.registry.Parks()["a"]; !parked {
		t.Fatal("the aggregate was not parked after its ack failed")
	}
}

func TestPool_ALiveJobRunsNoDurableDelivery(t *testing.T) {
	f := newFixture(t, time.Second)
	ticket := f.enqueue(t, event(1, nil))

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("steps = %v, want only the live publish", got)
	}
}

func TestPool_ATombstoneWithoutAnAckIsDeliveredAndNeverAcked(t *testing.T) {
	f := newFixture(t, time.Second)
	request := event(1, nil)
	request.Kind = asynxmd.DispatchTombstone
	ticket := f.enqueue(t, request)

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"live", "deliver"}) {
		t.Fatalf("steps = %v, want the live publish and the durable delivery", got)
	}
}

func TestPool_ARedeliverySkipsTheLiveSubscribers(t *testing.T) {
	f := newFixture(t, time.Second)
	request := event(1, func(context.Context) error { return nil })
	request.Kind = asynxmd.DispatchRedelivery
	ticket := f.enqueue(t, request)

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := f.journal.list(); !slices.Equal(got, []string{"deliver"}) {
		t.Fatalf("steps = %v, want only the durable delivery", got)
	}
}

func TestPool_ABusPanicIsReportedAndTheDeliveryGoesOn(t *testing.T) {
	f := newFixture(t, time.Second)
	f.bus.publish = func(asynxmd.Event[int]) error { panic("bus exploded") }
	ticket := f.enqueue(t, event(1, func(context.Context) error { return nil }))

	if err := final(t, ticket); err != nil {
		t.Fatalf("final = %v", err)
	}
	published := f.reports.published()
	if len(published) != 1 || !strings.Contains(published[0].Error(), "bus panic") {
		t.Fatalf("published errors = %v, want the bus panic", published)
	}
	if got := f.journal.list(); !slices.Contains(got, "deliver") {
		t.Fatalf("steps = %v: the bus panic skipped the durable delivery", got)
	}
}

func TestPool_ADispatcherPanicIsReportedAndEndsTheJob(t *testing.T) {
	f := newFixture(t, time.Second)
	f.deliverer.deliver = func(jobs.Job[int]) error { panic("dispatcher bug") }
	ticket := f.enqueue(t, event(1, func(context.Context) error { return nil }))

	err := final(t, ticket)

	if err == nil || !strings.Contains(err.Error(), "dispatcher panic") {
		t.Fatalf("final = %v, want the dispatcher panic", err)
	}
	if published := f.reports.published(); len(published) != 1 {
		t.Fatalf("published errors = %v, want the panic reported once", published)
	}
	if next := f.enqueue(t, event(2, nil)); final(t, next) != nil {
		t.Fatal("the worker did not survive the panic")
	}
}

func TestPool_AParkedAckedJobRunsNothing(t *testing.T) {
	f := newFixture(t, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	job := jobs.New(ctx, event(1, func(context.Context) error { return nil }), cancel)
	halt := make(chan struct{})
	close(halt)

	f.pool.(*aggregatePool[int]).handle("a", halt, job)

	if err := final(t, job.Ticket()); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if err := job.Ticket().Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("unrun = %v, want ErrDeliveryParked", err)
	}
	if got := f.journal.list(); len(got) != 0 {
		t.Fatalf("steps = %v: a job popped under a closed halt ran", got)
	}
}

func TestPool_HandlesAnAggregatesJobsInQueueOrder(t *testing.T) {
	f := newFixture(t, time.Second)
	var order []int64
	f.bus.publish = func(e asynxmd.Event[int]) error {
		order = append(order, e.Version)
		return nil
	}
	var last asynxmd.Ticket
	for v := int64(1); v <= 20; v++ {
		last = f.enqueue(t, event(v, nil))
	}

	if err := final(t, last); err != nil {
		t.Fatalf("final = %v", err)
	}
	for i, v := range order {
		assertVersion(t, i, v)
	}
}

func TestPool_AnIdleWorkerRetires(t *testing.T) {
	f := newFixture(t, time.Millisecond)
	if err := final(t, f.enqueue(t, event(1, nil))); err != nil {
		t.Fatal(err)
	}

	f.pool.Wait()

	if f.registry.Wake("a") != nil {
		t.Fatal("the record of a retired, unparked aggregate survived")
	}
	if err := final(t, f.enqueue(t, event(2, nil))); err != nil {
		t.Fatalf("a job after the retirement = %v", err)
	}
}

func TestPool_StopDrainsWhatIsQueued(t *testing.T) {
	f := newFixture(t, time.Hour)
	gate := make(chan struct{})
	f.bus.publish = func(asynxmd.Event[int]) error {
		<-gate
		return nil
	}
	tickets := []asynxmd.Ticket{f.enqueue(t, event(1, nil)), f.enqueue(t, event(2, nil))}
	f.registry.Close()
	f.pool.Stop()
	close(gate)

	f.pool.Wait()

	for _, ticket := range tickets {
		assertCompleted(t, ticket)
	}
}

func assertVersion(
	t *testing.T,
	index int,
	version int64,
) {
	t.Helper()
	if version != int64(index+1) {
		t.Fatalf("job %d handled as version %d: out of order", index, version)
	}
}

func assertCompleted(
	t *testing.T,
	ticket asynxmd.Ticket,
) {
	t.Helper()
	select {
	case <-ticket.Final:
	default:
		t.Fatal("Stop let a worker exit with a job still queued")
	}
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
