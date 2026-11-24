package delivery

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	asynxmd "github.com/char2cs/asynx/models"
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
	cfg := asynxmd.DispatcherConfig[int]{
		DurableSubs:  []asynxmd.DurableSub[int]{failing(errors.New("down"))},
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

func TestParkThenReport_ParksTheAggregateBeforeReporting(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)

	var parkedWhenTold []string
	report2 := func(
		context.Context,
	) {
		parkedWhenTold = slices.Sorted(maps.Keys(f.registry.Parks()))
	}
	f.deliverer.ParkThenReport(
		context.Background(),
		"a",
		halt,
		job,
		errStopped,
		report2,
	)

	if !slices.Equal(
		parkedWhenTold,
		[]string{"a"},
	) {
		t.Fatalf(
			"parked when told = %v, want [a]",
			parkedWhenTold,
		)
	}
}

var errStopped = errors.New("stopped")

type reportFlag struct {
	called bool
}

func (r *reportFlag) report(
	context.Context,
) {
	r.called = true
}

func TestParkThenReport_RecordsTheCause(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)

	f.deliverer.ParkThenReport(
		context.Background(),
		"a",
		halt,
		job,
		errStopped,
		noReport,
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

func TestParkThenReport_LeavesASettledAggregateAloneButStillReports(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	f.registry.Supersede("a")

	reported := &reportFlag{}
	f.deliverer.ParkThenReport(
		context.Background(),
		"a",
		halt,
		job,
		errStopped,
		reported.report,
	)

	if len(f.registry.Parks()) != 0 {
		t.Fatalf(
			"parks = %v: a superseded aggregate was parked again",
			f.registry.Parks(),
		)
	}

	if !reported.called {
		t.Fatal("the report was skipped")
	}
}

func TestParkThenReport_NeverParksForAJobWithoutAnAck(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, halt := f.begin(
		t,
		tombstone(nil),
	)

	f.deliverer.ParkThenReport(
		context.Background(),
		"a",
		halt,
		job,
		errStopped,
		noReport,
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
	f := newFixture(asynxmd.DispatcherConfig[int]{
		DurableSubs: []asynxmd.DurableSub[int]{failing(down)},
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
		asynxmd.ErrDeliveryParked,
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
		asynxmd.ErrDeliveryParked,
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
	var runs atomic.Int32
	match := func(
		string,
	) bool {
		return true
	}
	handler := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		runs.Add(1)
		return nil
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{DurableSubs: []asynxmd.DurableSub[int]{{
		Match:   match,
		Handler: handler,
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
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if runs.Load() != 0 {
		t.Fatalf(
			"handler ran %d times under a closed halt",
			runs.Load(),
		)
	}
}

func TestDeliver_ATombstoneIsNeverDeadLettered(
	t *testing.T,
) {
	var dead atomic.Int32
	deadLetter := func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		dead.Add(1)
		return nil
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{
		DurableSubs: []asynxmd.DurableSub[int]{failing(errors.New("cannot forget"))},
		MaxAttempts: 1,
		DeadLetter:  deadLetter,
	})
	ack := func(
		context.Context,
	) error {
		return nil
	}
	job, halt := f.begin(
		t,
		tombstone(ack),
	)

	if err := f.deliverer.Deliver(
		context.Background(),
		"a",
		halt,
		job,
	); !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if dead.Load() != 0 {
		t.Fatalf(
			"dead-letter handler called %d times for a tombstone",
			dead.Load(),
		)
	}
}

func TestDeliver_AJobWithoutAnAckIsNeverDeadLetteredNorParked(
	t *testing.T,
) {
	var dead atomic.Int32
	deadLetter2 := func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		dead.Add(1)
		return nil
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{
		DurableSubs: []asynxmd.DurableSub[int]{failing(errors.New("down"))},
		MaxAttempts: 1,
		DeadLetter:  deadLetter2,
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
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if dead.Load() != 0 ||
		len(f.registry.Parks()) != 0 {
		t.Fatalf(
			"dead letters = %d, parks = %v; want neither for a nil-Ack job",
			dead.Load(),
			f.registry.Parks(),
		)
	}
}

func TestDeliver_ATombstoneReachesEverySubscriber(
	t *testing.T,
) {
	var runs atomic.Int32
	match2 := func(
		string,
	) bool {
		return false
	}
	handler2 := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		runs.Add(1)
		return nil
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{DurableSubs: []asynxmd.DurableSub[int]{{
		Match:   match2,
		Handler: handler2,
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

	if runs.Load() != 1 {
		t.Fatalf(
			"a non-matching subscriber ran %d times for a tombstone, want 1",
			runs.Load(),
		)
	}
}

func TestDeliver_InterruptParksWithoutReportingAndEndsClosed(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{
		DurableSubs:  []asynxmd.DurableSub[int]{failing(errors.New("down"))},
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
		asynxmd.ErrDispatcherClosed,
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
	f := newFixture(asynxmd.DispatcherConfig[int]{
		DurableSubs:  []asynxmd.DurableSub[int]{failing(errors.New("down"))},
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
		asynxmd.ErrDeliveryParked,
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
	deadLetter3 := func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		panic("dead-letter store down")
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{
		DurableSubs: []asynxmd.DurableSub[int]{failing(errors.New("down"))},
		MaxAttempts: 1,
		DeadLetter:  deadLetter3,
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
		asynxmd.ErrDeliveryParked,
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
			asynxmd.ErrHandlerPanic,
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
	var runs atomic.Int32
	match3 := func(
		string,
	) bool {
		return true
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{DurableSubs: []asynxmd.DurableSub[int]{{
		Match:   match3,
		Handler: makeHandler(&runs),
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
		runs.Load() != 2 {
		t.Fatalf(
			"panics = %v, runs = %d; want one panic then a successful retry",
			f.reports.panics,
			runs.Load(),
		)
	}
}

func TestDeliver_ATimedOutHandlerIsReportedAndWaitedFor(
	t *testing.T,
) {
	release := make(chan struct{})
	match4 := func(
		string,
	) bool {
		return true
	}
	handler3 := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		<-release
		return nil
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{DurableSubs: []asynxmd.DurableSub[int]{{
		Name:    "slow",
		Match:   match4,
		Timeout: time.Millisecond,
		Handler: handler3,
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

	close(release)
	if err := <-done; err != nil {
		t.Fatalf(
			"Deliver = %v",
			err,
		)
	}

	if got := f.reports.all()[0].err; !errors.Is(
		got,
		asynxmd.ErrHandlerTimeout,
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
	cfg asynxmd.DispatcherConfig[int],
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
	request asynxmd.DispatchRequest[int],
) (jobs.Job[int], <-chan struct{}) {
	t.Helper()
	job := jobs.New(
		request,
		func() {},
	)
	if _, err := f.registry.Enqueue(job); err != nil {
		t.Fatal(err)
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
) asynxmd.DispatchRequest[int] {
	ack2 := func(
		context.Context,
	) error {
		return nil
	}
	return asynxmd.DispatchRequest[int]{
		Event: asynxmd.Event[int]{AggregateID: "a", Version: 1, EventName: name},
		Ack:   ack2,
	}
}

func tombstone(
	ack func(context.Context) error,
) asynxmd.DispatchRequest[int] {
	return asynxmd.DispatchRequest[int]{
		Event: asynxmd.Event[int]{AggregateID: "a", Version: 2, EventName: asynxmd.ForgetEventName},
		Ack:   ack,
		Kind:  asynxmd.DispatchTombstone,
	}
}

func failing(
	err error,
) asynxmd.DurableSub[int] {
	match5 := func(
		string,
	) bool {
		return true
	}
	handler4 := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return err
	}
	return asynxmd.DurableSub[int]{
		Name:    "failing",
		Match:   match5,
		Handler: handler4,
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

// PublishError satisfies the interface that recordingReporter fakes.
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

// DeliveryError satisfies the interface that recordingReporter fakes.
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

// Panic satisfies the interface that recordingReporter fakes.
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

func makeHandler(
	runs *atomic.Int32,
) func(context.Context, asynxmd.Event[int]) error {
	return func(
		p0 context.Context,
		p1 asynxmd.Event[int],
	) error {
		return runHandler(
			runs,
			p0,
			p1,
		)
	}
}

func runHandler(
	runs *atomic.Int32,
	_ context.Context,
	_ asynxmd.Event[int],
) error {
	if (*runs).Add(1) == 1 {
		panic("boom")
	}

	return nil
}
