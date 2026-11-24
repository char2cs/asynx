package workers

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	asynxmd "github.com/char2cs/asynx/models"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew_KeepsItsCollaborators(
	t *testing.T,
) {
	f := newFixture(t)
	pool := f.pool.(*aggregatePool[int])

	collaborators := [4]any{pool.bus, pool.registry, pool.deliverer, pool.reports}
	if collaborators != [4]any{f.bus, f.registry, f.deliverer, f.reports} {
		t.Fatal("New did not keep its collaborators")
	}
}
func TestPool_PublishesLiveThenDeliversThenAcks(
	t *testing.T,
) {
	f := newFixture(t)
	ack := journaledAck{journal: f.journal}
	ticket := f.enqueue(
		t,
		event(
			1,
			ack.recordAck,
		),
	)

	if err := final(
		t,
		ticket,
	); err != nil {
		t.Fatalf(
			"final = %v",
			err,
		)
	}

	if got := f.journal.list(); !slices.Equal(
		got,
		[]string{"live", "deliver", "ack"},
	) {
		t.Fatalf(
			"steps = %v, want live, deliver, ack",
			got,
		)
	}
}

func TestPool_NeverAcksADeliveryThatDidNotSucceed(
	t *testing.T,
) {
	f := newFixture(t)
	down := errors.New("down")
	f.deliverer.deliver = failingDelivery{err: down}.failDelivery
	var acks countingAck
	ticket := f.enqueue(
		t,
		event(
			1,
			acks.countAck,
		),
	)

	if err := final(
		t,
		ticket,
	); !errors.Is(
		err,
		down,
	) {
		t.Fatalf(
			"final = %v, want the delivery error",
			err,
		)
	}

	if acks.calls.Load() != 0 {
		t.Fatal("a delivery that failed was acknowledged")
	}
}

func TestPool_AnAckFailureParksBeforeItIsReported(
	t *testing.T,
) {
	f := newFixture(t)
	storeDown := errors.New("store down")
	ack := failingAck{err: storeDown}
	ticket := f.enqueue(
		t,
		event(
			1,
			ack.failAck,
		),
	)

	if err := final(
		t,
		ticket,
	); !errors.Is(
		err,
		storeDown,
	) {
		t.Fatalf(
			"final = %v, want the ack error",
			err,
		)
	}

	if got := f.journal.list(); !slices.Equal(
		got,
		[]string{"live", "deliver", "park", "report"},
	) {
		t.Fatalf(
			"steps = %v, want the park before the report",
			got,
		)
	}

	if _, parked := f.registry.Parks()["a"]; !parked {
		t.Fatal("the aggregate was not parked after its ack failed")
	}
}

func TestPool_ALiveJobRunsNoDurableDelivery(
	t *testing.T,
) {
	f := newFixture(t)
	ticket := f.enqueue(
		t,
		event(
			1,
			nil,
		),
	)

	if err := final(
		t,
		ticket,
	); err != nil {
		t.Fatalf(
			"final = %v",
			err,
		)
	}

	if got := f.journal.list(); !slices.Equal(
		got,
		[]string{"live"},
	) {
		t.Fatalf(
			"steps = %v, want only the live publish",
			got,
		)
	}
}

func TestPool_ATombstoneWithoutAnAckIsDeliveredAndNeverAcked(
	t *testing.T,
) {
	f := newFixture(t)
	request := event(
		1,
		nil,
	)
	request.Kind = asynxmd.DispatchTombstone
	ticket := f.enqueue(
		t,
		request,
	)

	if err := final(
		t,
		ticket,
	); err != nil {
		t.Fatalf(
			"final = %v",
			err,
		)
	}

	if got := f.journal.list(); !slices.Equal(
		got,
		[]string{"live", "deliver"},
	) {
		t.Fatalf(
			"steps = %v, want the live publish and the durable delivery",
			got,
		)
	}
}

func TestPool_ARedeliverySkipsTheLiveSubscribers(
	t *testing.T,
) {
	f := newFixture(t)
	request := event(
		1,
		acknowledgeAtOnce,
	)
	request.Kind = asynxmd.DispatchRedelivery
	ticket := f.enqueue(
		t,
		request,
	)

	if err := final(
		t,
		ticket,
	); err != nil {
		t.Fatalf(
			"final = %v",
			err,
		)
	}

	if got := f.journal.list(); !slices.Equal(
		got,
		[]string{"deliver"},
	) {
		t.Fatalf(
			"steps = %v, want only the durable delivery",
			got,
		)
	}
}

func TestPool_ABusPanicIsReportedAndTheDeliveryGoesOn(
	t *testing.T,
) {
	f := newFixture(t)
	f.bus.publish = panicOnPublish
	ticket := f.enqueue(
		t,
		event(
			1,
			acknowledgeAtOnce,
		),
	)

	if err := final(
		t,
		ticket,
	); err != nil {
		t.Fatalf(
			"final = %v",
			err,
		)
	}

	published := f.reports.published()
	if len(published) != 1 ||
		!strings.Contains(
			published[0].Error(),
			"bus panic",
		) {
		t.Fatalf(
			"published errors = %v, want the bus panic",
			published,
		)
	}

	if got := f.journal.list(); !slices.Contains(
		got,
		"deliver",
	) {
		t.Fatalf(
			"steps = %v: the bus panic skipped the durable delivery",
			got,
		)
	}
}

func TestPool_ADispatcherPanicIsReportedAndEndsTheJob(
	t *testing.T,
) {
	f := newFixture(t)
	f.deliverer.deliver = panicOnDelivery
	ticket := f.enqueue(
		t,
		event(
			1,
			acknowledgeAtOnce,
		),
	)

	err := final(
		t,
		ticket,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"dispatcher panic",
		) {
		t.Fatalf(
			"final = %v, want the dispatcher panic",
			err,
		)
	}

	if published := f.reports.published(); len(published) != 1 {
		t.Fatalf(
			"published errors = %v, want the panic reported once",
			published,
		)
	}

	if next := f.enqueue(
		t,
		event(
			2,
			nil,
		),
	); final(
		t,
		next,
	) != nil {
		t.Fatal("the worker did not survive the panic")
	}
}

func TestPool_AParkedAckedJobRunsNothing(
	t *testing.T,
) {
	f := newFixture(t)
	job := jobs.New(
		event(
			1,
			acknowledgeAtOnce,
		),
		func() {},
	)
	halt := make(chan struct{})
	close(halt)

	f.pool.(*aggregatePool[int]).handle(
		context.Background(),
		"a",
		halt,
		job,
	)

	if err := final(
		t,
		job.Ticket(),
	); !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"final = %v, want ErrDeliveryParked",
			err,
		)
	}

	if err := job.Ticket().Unrun(); !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"unrun = %v, want ErrDeliveryParked",
			err,
		)
	}

	if got := f.journal.list(); len(got) != 0 {
		t.Fatalf(
			"steps = %v: a job popped under a closed halt ran",
			got,
		)
	}
}

func TestPool_HandlesAnAggregatesJobsInQueueOrder(
	t *testing.T,
) {
	f := newFixture(t)
	var order []int64
	f.bus.publish = func(
		e asynxmd.Event[int],
	) error {
		order = append(
			order,
			e.Version,
		)
		return nil
	}
	var last asynxmd.Ticket
	for v := int64(1); v <= 20; v++ {
		last = f.enqueue(
			t,
			event(
				v,
				nil,
			),
		)
	}

	if err := final(
		t,
		last,
	); err != nil {
		t.Fatalf(
			"final = %v",
			err,
		)
	}

	for i, v := range order {
		assertVersion(
			t,
			i,
			v,
		)
	}
}

type versionGate struct {
	gate <-chan struct{}
}

func (g versionGate) publish(
	e asynxmd.Event[int],
) error {
	if e.Version == 1 {
		<-g.gate
	}

	return nil
}

func TestPool_AJobAParkDroppedRunsNothingAndStillHandsItsTurnOn(
	t *testing.T,
) {
	f := newFixture(t)
	gate := make(chan struct{})
	f.bus.publish = versionGate{gate: gate}.publish
	running := f.enqueue(
		t,
		event(
			1,
			nil,
		),
	)
	dropped := f.enqueue(
		t,
		event(
			2,
			acknowledgeAtOnce,
		),
	)
	behind := f.enqueue(
		t,
		event(
			3,
			nil,
		),
	)

	f.registry.Park(
		"a",
		errors.New("halted"),
	)
	close(gate)

	if err := final(
		t,
		behind,
	); err != nil {
		t.Fatalf(
			"the job behind the dropped one = %v",
			err,
		)
	}

	if err := final(
		t,
		dropped,
	); !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"the dropped job = %v, want ErrDeliveryParked",
			err,
		)
	}

	assertCompleted(
		t,
		running,
	)
	if got := f.journal.list(); !slices.Equal(
		got,
		[]string{"live", "live"},
	) {
		t.Fatalf(
			"steps = %v, want only the two live jobs to run",
			got,
		)
	}
}
func TestPool_ClosingTheRegistryStillDeliversWhatIsQueued(
	t *testing.T,
) {
	f := newFixture(t)
	gate := make(chan struct{})
	f.bus.publish = func(
		asynxmd.Event[int],
	) error {
		<-gate
		return nil
	}
	tickets := []asynxmd.Ticket{f.enqueue(
		t,
		event(
			1,
			nil,
		),
	), f.enqueue(
		t,
		event(
			2,
			nil,
		),
	)}
	f.registry.Close()
	close(gate)

	f.runs.Wait()

	for _, ticket := range tickets {
		assertCompleted(
			t,
			ticket,
		)
	}
}

func TestPool_RunPassesItsContextToEveryStep(
	t *testing.T,
) {
	f := newFixture(t)
	var ack contextKeepingAck
	ticket := f.enqueue(
		t,
		event(
			1,
			ack.keepContext,
		),
	)
	if err := final(
		t,
		ticket,
	); err != nil {
		t.Fatalf(
			"final = %v",
			err,
		)
	}

	assertSender(
		t,
		"bus",
		f.bus.seen(),
	)
	assertSender(
		t,
		"deliverer",
		f.deliverer.seen(),
	)
	assertSender(
		t,
		"ack",
		ack.ctx,
	)
}

func assertSender(
	t *testing.T,
	step string,
	ctx context.Context,
) {
	t.Helper()
	if ctx == nil ||
		ctx.Value(ctxKey{}) != "sender" {
		t.Fatalf(
			"%s did not get the context Run was given",
			step,
		)
	}
}

func TestPool_AReportedErrorCarriesTheContext(
	t *testing.T,
) {
	f := newFixture(t)
	ack := failingAck{err: errors.New("store down")}
	if err := final(
		t,
		f.enqueue(
			t,
			event(
				1,
				ack.failAck,
			),
		),
	); err == nil {
		t.Fatal("final = nil, want the ack error")
	}

	assertSender(
		t,
		"reporter",
		f.reports.seen(),
	)
}
func assertVersion(
	t *testing.T,
	index int,
	version int64,
) {
	t.Helper()
	if version != int64(index+1) {
		t.Fatalf(
			"job %d handled as version %d: out of order",
			index,
			version,
		)
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

type ctxKey struct{}

type fixture struct {
	journal   *journal
	registry  aggregates.Registry[jobs.Job[int]]
	deliverer *fakeDeliverer
	reports   *fakeReporter
	bus       *fakeBus
	pool      Pool[int]
	runs      sync.WaitGroup
}

func newFixture(
	t *testing.T,
) *fixture {
	f := &fixture{journal: &journal{}}
	f.registry = aggregates.New(aggregates.Hooks[jobs.Job[int]]{})
	f.deliverer = &fakeDeliverer{journal: f.journal, registry: f.registry}
	f.reports = &fakeReporter{journal: f.journal}
	f.bus = &fakeBus{journal: f.journal}
	f.pool = New[int](
		f.bus,
		f.registry,
		f.deliverer,
		f.reports,
	)
	t.Cleanup(f.stop)
	return f
}

func (f *fixture) stop() {
	f.registry.Close()
	f.runs.Wait()
}

func (f *fixture) enqueue(
	t *testing.T,
	request asynxmd.DispatchRequest[int],
) asynxmd.Ticket {
	t.Helper()
	job := jobs.New(
		request,
		func() {},
	)
	var admission contracts.Admission
	f.registry.Enqueue(
		job,
		&admission,
	)
	if admission.Err != nil {
		t.Fatal(admission.Err)
	}

	if admission.Parked {
		return job.Ticket()
	}

	ctx := context.WithValue(
		context.Background(),
		ctxKey{},
		"sender",
	)
	f.runs.Add(1)
	go f.run(
		ctx,
		job,
		admission.Turn,
		admission.Done,
	)
	return job.Ticket()
}

func (f *fixture) run(
	ctx context.Context,
	job jobs.Job[int],
	turn <-chan struct{},
	done chan<- struct{},
) {
	defer f.runs.Done()
	f.pool.Run(
		ctx,
		job,
		turn,
		done,
	)
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
	mu      sync.Mutex
	ctx     context.Context
}

func (b *fakeBus) seen() context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ctx
}

// PublishSync records the context it was called with, notes "live" in the
// shared journal to fix the order of the publish against the delivery, the
// park and the report, and returns the result of the scripted publish
// function, nil without one.
func (b *fakeBus) PublishSync(
	ctx context.Context,
	event asynxmd.Event[int],
) error {
	b.mu.Lock()
	b.ctx = ctx
	b.mu.Unlock()
	b.journal.add("live")
	if b.publish == nil {
		return nil
	}

	return b.publish(event)
}

// Publish does nothing and reports success: the dispatcher publishes through
// PublishSync only.
func (b *fakeBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

// Subscribe registers nothing and returns an empty subscription id: the
// dispatcher never subscribes through the bus.
func (b *fakeBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "",
		nil
}

// Unsubscribe does nothing and reports success: nothing was ever subscribed.
func (b *fakeBus) Unsubscribe(
	string,
) error {
	return nil
}

// Close does nothing and reports success: the double holds no resources.
func (b *fakeBus) Close(
	context.Context,
) error {
	return nil
}

// WaitForHandlers returns at once: the double runs no handlers of its own.
func (b *fakeBus) WaitForHandlers() {
}

type fakeDeliverer struct {
	mu       sync.Mutex
	ctx      context.Context
	journal  *journal
	registry aggregates.Registry[jobs.Job[int]]
	deliver  func(job jobs.Job[int]) error
}

func (d *fakeDeliverer) seen() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ctx
}

// Deliver records the context it received, notes "deliver" in the shared
// journal and returns the result of the scripted deliver function, nil
// without one.
func (d *fakeDeliverer) Deliver(
	ctx context.Context,
	_ string,
	_ <-chan struct{},
	job jobs.Job[int],
) error {
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()
	d.journal.add("deliver")
	if d.deliver == nil {
		return nil
	}

	return d.deliver(job)
}

// Park records the park in the journal and parks the registry as the real
// deliverer does.
func (d *fakeDeliverer) Park(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[int],
	cause error,
) {
	d.journal.add("park")
	if job.Acked() {
		d.registry.ParkIfCurrent(
			aggregateID,
			halt,
			cause,
		)
	}
}

type fakeReporter struct {
	journal *journal
	ctx     context.Context
	mu      sync.Mutex
	errs    []error
}

// PublishError notes "report" in the shared journal and records the context
// and the error it was given.
func (r *fakeReporter) PublishError(
	ctx context.Context,
	_ jobs.Job[int],
	err error,
) {
	r.journal.add("report")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ctx = ctx
	r.errs = append(
		r.errs,
		err,
	)
}

// DeliveryError does nothing: the pool under test reports through
// PublishError only.
func (r *fakeReporter) DeliveryError(
	context.Context,
	jobs.Job[int],
	error,
	int,
) {
}

// Panic does nothing: the pool under test reports through PublishError only.
func (r *fakeReporter) Panic(
	context.Context,
	jobs.Job[int],
	any,
) {
}

func (r *fakeReporter) seen() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ctx
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
	j.entries = append(
		j.entries,
		entry,
	)
}

func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.entries)
}

func acknowledgeAtOnce(
	context.Context,
) error {
	return nil
}

type journaledAck struct {
	journal *journal
}

func (a journaledAck) recordAck(
	context.Context,
) error {
	a.journal.add("ack")
	return nil
}

type countingAck struct {
	calls atomic.Int32
}

func (a *countingAck) countAck(
	context.Context,
) error {
	a.calls.Add(1)
	return nil
}

type failingAck struct {
	err error
}

func (a failingAck) failAck(
	context.Context,
) error {
	return a.err
}

type contextKeepingAck struct {
	ctx context.Context
}

func (a *contextKeepingAck) keepContext(
	ctx context.Context,
) error {
	a.ctx = ctx
	return nil
}

type failingDelivery struct {
	err error
}

func (d failingDelivery) failDelivery(
	jobs.Job[int],
) error {
	return d.err
}

func panicOnDelivery(
	jobs.Job[int],
) error {
	panic("dispatcher bug")
}

func panicOnPublish(
	asynxmd.Event[int],
) error {
	panic("bus exploded")
}
