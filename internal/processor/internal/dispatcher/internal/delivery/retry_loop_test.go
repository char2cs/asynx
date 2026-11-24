package delivery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
)

func TestRetryLoop_SettlesTheJobAfterTheFirstAttempt(
	t *testing.T,
) {
	var runs atomic.Int32
	var job jobs.Job[int]
	settledAtSecond := make(
		chan bool,
		1,
	)
	match := func(
		string,
	) bool {
		return true
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{DurableSubs: []asynxmd.DurableSub[int]{{
		Match: match,
		Handler: makeHandler2(
			&runs,
			settledAtSecond,
			&job,
		),
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

	if !<-settledAtSecond {
		t.Fatal("the job was not settled after its first, failed attempt")
	}
}

func TestRetryLoop_RetriesOnlyTheFailedSubscribers(
	t *testing.T,
) {
	var goodRuns, flakyRuns atomic.Int32
	match2 := func(
		string,
	) bool {
		return true
	}
	handler := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		goodRuns.Add(1)
		return nil
	}
	match3 := func(
		string,
	) bool {
		return true
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{MaxAttempts: 5, DurableSubs: []asynxmd.DurableSub[int]{
		{Match: match2, Handler: handler},
		{Match: match3, Handler: makeHandler3(&flakyRuns)},
	}})
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

	if goodRuns.Load() != 1 ||
		flakyRuns.Load() != 3 {
		t.Fatalf(
			"good ran %d, flaky %d; want 1 and 3",
			goodRuns.Load(),
			flakyRuns.Load(),
		)
	}
}

func TestRetryLoop_StopsAtTheAttemptCap(
	t *testing.T,
) {
	var runs atomic.Int32
	match4 := func(
		string,
	) bool {
		return true
	}
	handler2 := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		runs.Add(1)
		return errors.New("down")
	}
	f := newFixture(asynxmd.DispatcherConfig[int]{MaxAttempts: 4, DurableSubs: []asynxmd.DurableSub[int]{{
		Match:   match4,
		Handler: handler2,
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
	); !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if runs.Load() != 4 {
		t.Fatalf(
			"runs = %d, want exactly the cap (4)",
			runs.Load(),
		)
	}
}

func stepRetrying(
	t *testing.T,
	loop *retryLoop[int],
) {
	t.Helper()
	err := loop.step(context.Background())
	if errors.Is(
		err,
		errRetrying,
	) {
		return
	}

	t.Fatalf(
		"step = %v, want %v",
		err,
		errRetrying,
	)
}

func TestRetryLoop_DoublesTheBackoffUpToTheMaximum(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{RetryInitial: time.Millisecond, RetryMax: 3 * time.Millisecond})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	loop := &retryLoop[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		halt:      halt,
		job:       job,
		pending:   []asynxmd.DurableSub[int]{failing(errors.New("down"))},
		attempt:   1,
		backoff:   time.Millisecond,
	}
	loop.deliverer.maxAttempts = 10

	var backoffs []time.Duration
	for range 3 {
		stepRetrying(
			t,
			loop,
		)
		backoffs = append(
			backoffs,
			loop.backoff,
		)
	}

	want := []time.Duration{2 * time.Millisecond, 3 * time.Millisecond, 3 * time.Millisecond}
	for i := range want {
		assertBackoff(
			t,
			backoffs[i],
			want[i],
		)
	}
}

func assertBackoff(
	t *testing.T,
	got time.Duration,
	want time.Duration,
) {
	t.Helper()
	if got != want {
		t.Fatalf(
			"backoff = %v, want %v",
			got,
			want,
		)
	}
}

func TestRetryLoop_AParkedJobIsNotRetriedWhenItsBackoffEnds(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, _ := f.begin(
		t,
		acked("test.event"),
	)
	deliverer := f.deliverer.(*durableDeliverer[int])
	parked := make(chan struct{})
	close(parked)

	for range 500 {
		requireParkedWait(
			t,
			deliverer,
			job,
			parked,
		)
	}
}

func requireParkedWait(
	t *testing.T,
	deliverer *durableDeliverer[int],
	job jobs.Job[int],
	parked <-chan struct{},
) {
	t.Helper()
	err := deliverer.wait(
		context.Background(),
		"a",
		parked,
		job,
		0,
	)
	if !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"wait on a parked job = %v, want ErrDeliveryParked: its backoff ended after the park and it would run its handlers again",
			err,
		)
	}
}

func makeHandler2(
	runs *atomic.Int32,
	settledAtSecond chan bool,
	job *jobs.Job[int],
) func(context.Context, asynxmd.Event[int]) error {
	return func(
		p0 context.Context,
		p1 asynxmd.Event[int],
	) error {
		return runHandler2(
			runs,
			settledAtSecond,
			job,
			p0,
			p1,
		)
	}
}

func runHandler2(
	runs *atomic.Int32,
	settledAtSecond chan bool,
	job *jobs.Job[int],
	_ context.Context,
	_ asynxmd.Event[int],
) error {
	if (*runs).Add(1) == 1 {
		return errors.New("not yet")
	}

	settledAtSecond <- isClosed((*job).Ticket().Settled)
	return nil
}

func makeHandler3(
	flakyRuns *atomic.Int32,
) func(context.Context, asynxmd.Event[int]) error {
	return func(
		p0 context.Context,
		p1 asynxmd.Event[int],
	) error {
		return runHandler3(
			flakyRuns,
			p0,
			p1,
		)
	}
}

func runHandler3(
	flakyRuns *atomic.Int32,
	_ context.Context,
	_ asynxmd.Event[int],
) error {
	if (*flakyRuns).Add(1) < 3 {
		return errors.New("flaky")
	}

	return nil
}
