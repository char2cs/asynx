package delivery

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/subscription"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew_TakesTheConfigAsFinal(
	t *testing.T,
) {
	cfg := dispatcher.Config[int]{
		DurableSubs:  []subscription.Durable[int]{failing(errors.New("down"))},
		RetryInitial: time.Second,
		RetryMax:     time.Minute,
		MaxAttempts:  7,
	}
	registry := aggregates.New(aggregates.Hooks[jobs.Job[int]]{})
	interrupted := make(chan struct{})

	d := New(
		cfg,
		registry,
		&recordingReporter{registry: registry},
		interrupted,
	).(*durableDeliverer[int])

	retry := retrySettings{d.retryInitial, d.retryMax, d.maxAttempts}
	if retry != (retrySettings{time.Second, time.Minute, 7}) {
		t.Fatalf(
			"retry settings = %+v; want the config's",
			retry,
		)
	}

	kept := len(d.subs) == 1 && d.deadLetter == nil
	if !kept {
		t.Fatal("New did not keep the subscribers and the dead-letter handler")
	}

	if d.interrupted != interrupted {
		t.Fatal("New did not keep the interrupt channel")
	}
}

type retrySettings struct {
	initial     time.Duration
	max         time.Duration
	maxAttempts int
}

func TestPark_ParksTheAggregateOfAnAckedJob(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)

	f.deliverer.Park(
		"a",
		halt,
		job,
		errStopped,
	)

	parked := slices.Sorted(maps.Keys(f.registry.Parks()))
	if !slices.Equal(
		parked,
		[]string{"a"},
	) {
		t.Fatalf(
			"parked = %v, want [a]",
			parked,
		)
	}
}

var errStopped = errors.New("stopped")

func TestPark_RecordsTheCause(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)

	f.deliverer.Park(
		"a",
		halt,
		job,
		errStopped,
	)

	if got := f.registry.Inspect("a"); !got.Parked ||
		!errors.Is(
			got.Cause,
			errStopped,
		) {
		t.Fatalf(
			"Inspect = %+v, want parked with %v",
			got,
			errStopped,
		)
	}
}

func TestPark_LeavesASettledAggregateAlone(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	f.registry.Supersede("a")

	f.deliverer.Park(
		"a",
		halt,
		job,
		errStopped,
	)

	if len(f.registry.Parks()) != 0 {
		t.Fatalf(
			"parks = %v: a superseded aggregate was parked again",
			f.registry.Parks(),
		)
	}
}

func TestPark_NeverParksForAJobWithoutAnAck(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{})
	job, halt := f.begin(
		t,
		tombstone(nil),
	)

	f.deliverer.Park(
		"a",
		halt,
		job,
		errStopped,
	)

	if len(f.registry.Parks()) != 0 {
		t.Fatalf(
			"parks = %v, want none for a nil-Ack job",
			f.registry.Parks(),
		)
	}
}

func TestDeliver_GiveUpParksBeforeTheParkedErrorIsReported(
	t *testing.T,
) {
	down := errors.New("down")
	f := newFixture(dispatcher.Config[int]{
		DurableSubs: []subscription.Durable[int]{failing(down)},
		MaxAttempts: 2,
	})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)

	err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	)
	if !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) ||
		!errors.Is(
			err,
			down,
		) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked wrapping the handler error",
			err,
		)
	}

	reports := f.reports.all()
	last := reports[len(reports)-1]
	if !errors.Is(
		last.err,
		failure.ErrDeliveryParked,
	) ||
		!slices.Equal(
			last.parked,
			[]string{"a"},
		) {
		t.Fatalf(
			"last report = %+v, want the park reported once the aggregate was parked",
			last,
		)
	}

	if len(reports) != 3 {
		t.Fatalf(
			"reports = %d, want two failed attempts and the park",
			len(reports),
		)
	}
}

func TestDeliver_AnAckedJobUnderAClosedHaltRunsNothing(
	t *testing.T,
) {
	var counted countingHandler
	f := newFixture(dispatcher.Config[int]{DurableSubs: []subscription.Durable[int]{{
		Match:   matchAnyEvent,
		Handler: counted.countRun,
	}}})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	f.registry.Supersede("a")

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if counted.runs.Load() != 0 {
		t.Fatalf(
			"handler ran %d times under a closed halt",
			counted.runs.Load(),
		)
	}
}

func TestDeliver_ATombstoneIsNeverDeadLettered(
	t *testing.T,
) {
	var deadLetters countingDeadLetter
	f := newFixture(dispatcher.Config[int]{
		DurableSubs: []subscription.Durable[int]{failing(errors.New("cannot forget"))},
		MaxAttempts: 1,
		DeadLetter:  deadLetters.countCall,
	})
	job, halt := f.begin(
		t,
		tombstone(acknowledgeAtOnce),
	)

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if deadLetters.calls.Load() != 0 {
		t.Fatalf(
			"dead-letter handler called %d times for a tombstone",
			deadLetters.calls.Load(),
		)
	}
}

func TestDeliver_AJobWithoutAnAckIsNeverDeadLetteredNorParked(
	t *testing.T,
) {
	var deadLetters countingDeadLetter
	f := newFixture(dispatcher.Config[int]{
		DurableSubs: []subscription.Durable[int]{failing(errors.New("down"))},
		MaxAttempts: 1,
		DeadLetter:  deadLetters.countCall,
	})
	job, halt := f.begin(
		t,
		tombstone(nil),
	)

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if deadLetters.calls.Load() != 0 ||
		len(f.registry.Parks()) != 0 {
		t.Fatalf(
			"dead letters = %d, parks = %v; want neither for a nil-Ack job",
			deadLetters.calls.Load(),
			f.registry.Parks(),
		)
	}
}

func TestDeliver_ATombstoneReachesEverySubscriber(
	t *testing.T,
) {
	var counted countingHandler
	f := newFixture(dispatcher.Config[int]{DurableSubs: []subscription.Durable[int]{{
		Match:   matchNoEvent,
		Handler: counted.countRun,
	}}})
	job, halt := f.begin(
		t,
		tombstone(nil),
	)

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); err != nil {
		t.Fatalf(
			"Deliver = %v",
			err,
		)
	}

	if counted.runs.Load() != 1 {
		t.Fatalf(
			"a non-matching subscriber ran %d times for a tombstone, want 1",
			counted.runs.Load(),
		)
	}
}

func TestDeliver_InterruptParksWithoutReportingAndEndsClosed(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{
		DurableSubs:  []subscription.Durable[int]{failing(errors.New("down"))},
		MaxAttempts:  100,
		RetryInitial: time.Hour,
		RetryMax:     time.Hour,
	})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	close(f.interrupted)

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); !errors.Is(
		err,
		failure.ErrDispatcherClosed,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDispatcherClosed",
			err,
		)
	}

	if _, parked := f.registry.Parks()["a"]; !parked {
		t.Fatal("an interrupted acked delivery did not park its aggregate")
	}

	if got := len(f.reports.all()); got != 1 {
		t.Fatalf(
			"reports = %d, want only the failed attempt",
			got,
		)
	}
}

func TestDeliver_AParkDuringTheBackoffEndsTheDelivery(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{
		DurableSubs:  []subscription.Durable[int]{failing(errors.New("down"))},
		MaxAttempts:  100,
		RetryInitial: time.Hour,
		RetryMax:     time.Hour,
	})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	done := make(
		chan error,
		1,
	)
	go func() {
		done <- f.deliverer.Deliver(
			context.Background(),
			"a",
			halt,
			job,
		)
	}()

	waitUntil(
		t,
		func() bool { return len(f.reports.all()) == 1 },
	)
	f.registry.Park(
		"a",
		errStopped,
	)

	if err := <-done; !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}
}

func TestDeliver_ADeadLetterHandlerPanicParksTheAggregate(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{
		DurableSubs: []subscription.Durable[int]{failing(errors.New("down"))},
		MaxAttempts: 1,
		DeadLetter:  panicOnDeadLetter,
	})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	reports := f.reports.all()
	if len(reports) < 2 ||
		!errors.Is(
			reports[1].err,
			failure.ErrHandlerPanic,
		) {
		t.Fatalf(
			"reports = %+v, want the dead-letter panic reported",
			reports,
		)
	}
}

func TestDeliver_AHandlerPanicIsReportedAndRetried(
	t *testing.T,
) {
	var panicking panicsOnFirstRun
	f := newFixture(dispatcher.Config[int]{DurableSubs: []subscription.Durable[int]{{
		Match:   matchAnyEvent,
		Handler: panicking.panicThenSucceed,
	}}})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); err != nil {
		t.Fatalf(
			"Deliver = %v",
			err,
		)
	}

	if len(f.reports.panics) != 1 ||
		panicking.runs.Load() != 2 {
		t.Fatalf(
			"panics = %v, runs = %d; want one panic then a successful retry",
			f.reports.panics,
			panicking.runs.Load(),
		)
	}
}

func TestDeliver_ATimedOutHandlerIsReportedAndWaitedFor(
	t *testing.T,
) {
	blocked := blockedUntilReleased{release: make(chan struct{})}
	f := newFixture(dispatcher.Config[int]{DurableSubs: []subscription.Durable[int]{{
		Name:    "slow",
		Match:   matchAnyEvent,
		Timeout: time.Millisecond,
		Handler: blocked.waitForRelease,
	}}})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	done := make(
		chan error,
		1,
	)
	go func() {
		done <- f.deliverer.Deliver(
			context.Background(),
			"a",
			halt,
			job,
		)
	}()

	waitUntil(
		t,
		func() bool { return len(f.reports.all()) == 1 },
	)
	select {
	case err := <-done:
		t.Fatalf(
			"Deliver returned (%v) while the timed-out handler still ran",
			err,
		)
	default:
	}

	close(blocked.release)
	if err := <-done; err != nil {
		t.Fatalf(
			"Deliver = %v",
			err,
		)
	}

	if got := f.reports.all()[0].err; !errors.Is(
		got,
		failure.ErrHandlerTimeout,
	) {
		t.Fatalf(
			"reported %v, want ErrHandlerTimeout",
			got,
		)
	}
}

func waitUntil(
	t *testing.T,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		failPast(
			t,
			deadline,
		)
		time.Sleep(time.Millisecond)
	}
}

func failPast(
	t *testing.T,
	deadline time.Time,
) {
	t.Helper()
	if time.Now().After(deadline) {
		t.Fatal("timed out waiting for the condition")
	}
}

type fixture struct {
	registry    aggregates.Registry[jobs.Job[int]]
	reports     *recordingReporter
	interrupted chan struct{}
	deliverer   Deliverer[int]
}

func newFixture(
	cfg dispatcher.Config[int],
) *fixture {
	registry := aggregates.New(aggregates.Hooks[jobs.Job[int]]{})
	f := &fixture{
		registry:    registry,
		reports:     &recordingReporter{registry: registry},
		interrupted: make(chan struct{}),
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}

	if cfg.RetryInitial == 0 {
		cfg.RetryInitial, cfg.RetryMax = time.Millisecond, time.Millisecond
	}

	f.deliverer = New(
		cfg,
		registry,
		f.reports,
		f.interrupted,
	)
	return f
}

func (f *fixture) begin(
	t *testing.T,
	request dispatch.Request[int],
) (jobs.Job[int], <-chan struct{}) {
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

	halt, ok := f.registry.Begin(job)
	if !ok {
		t.Fatal("the job was not queued")
	}

	return job,
		halt
}

func acked(
	name string,
) dispatch.Request[int] {
	return dispatch.Request[int]{
		Event: event.Event[int]{AggregateID: "a", Version: 1, EventName: name},
		Ack:   acknowledgeAtOnce,
	}
}

func tombstone(
	ack func(context.Context) error,
) dispatch.Request[int] {
	return dispatch.Request[int]{
		Event: event.Event[int]{AggregateID: "a", Version: 2, EventName: event.ForgetEventName},
		Ack:   ack,
		Kind:  dispatch.KindTombstone,
	}
}

func failing(
	err error,
) subscription.Durable[int] {
	failer := &countingHandler{err: err}
	return subscription.Durable[int]{
		Name:    "failing",
		Match:   matchAnyEvent,
		Handler: failer.countRun,
	}
}

type recordingReporter struct {
	mu       sync.Mutex
	registry aggregates.Registry[jobs.Job[int]]
	reports  []report
	panics   []any
}

func (r *recordingReporter) record(
	kind string,
	err error,
) {
	parked := slices.Sorted(maps.Keys(r.registry.Parks()))
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(
		r.reports,
		report{kind: kind, err: err, parked: parked},
	)
}

// PublishError records a publish report with the aggregates parked at that
// moment, so a test can assert what was parked when the error handler was
// told.
func (r *recordingReporter) PublishError(
	_ context.Context,
	_ jobs.Job[int],
	err error,
) {
	r.record(
		"publish",
		err,
	)
}

// DeliveryError records a delivery report with the aggregates parked at that
// moment, so a test can assert what was parked when the error handler was
// told.
func (r *recordingReporter) DeliveryError(
	_ context.Context,
	_ jobs.Job[int],
	err error,
	_ int,
) {
	r.record(
		"delivery",
		err,
	)
}

// Panic keeps the recovered value for the test to read back.
func (r *recordingReporter) Panic(
	_ context.Context,
	_ jobs.Job[int],
	recovered any,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.panics = append(
		r.panics,
		recovered,
	)
}

func (r *recordingReporter) all() []report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reports)
}

type report struct {
	kind   string
	err    error
	parked []string
}

func matchAnyEvent(
	string,
) bool {
	return true
}

func matchNoEvent(
	string,
) bool {
	return false
}

func acknowledgeAtOnce(
	context.Context,
) error {
	return nil
}

type countingHandler struct {
	runs atomic.Int32
	err  error
}

func (c *countingHandler) countRun(
	context.Context,
	event.Event[int],
) error {
	c.runs.Add(1)
	return c.err
}

type countingDeadLetter struct {
	calls atomic.Int32
}

func (c *countingDeadLetter) countCall(
	context.Context,
	event.Event[int],
	error,
) error {
	c.calls.Add(1)
	return nil
}

func panicOnDeadLetter(
	context.Context,
	event.Event[int],
	error,
) error {
	panic("dead-letter store down")
}

type blockedUntilReleased struct {
	release chan struct{}
}

func (b *blockedUntilReleased) waitForRelease(
	context.Context,
	event.Event[int],
) error {
	<-b.release
	return nil
}

type panicsOnFirstRun struct {
	runs atomic.Int32
}

func (p *panicsOnFirstRun) panicThenSucceed(
	context.Context,
	event.Event[int],
) error {
	if p.runs.Add(1) == 1 {
		panic("boom")
	}

	return nil
}
