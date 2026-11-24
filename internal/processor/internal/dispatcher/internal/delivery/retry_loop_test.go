package delivery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/subscription"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
)

func TestRetryLoop_SettlesTheJobAfterTheFirstAttempt(
	t *testing.T,
) {
	var job jobs.Job[int]
	recorder := &recordsSettlementAtSecondRun{
		settledAtSecond: make(
			chan bool,
			1,
		),
		job: &job,
	}
	f := newFixture(dispatcher.Config[int]{DurableSubs: []subscription.Durable[int]{{
		Match:   matchAnyEvent,
		Handler: recorder.recordSettlement,
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

	if !<-recorder.settledAtSecond {
		t.Fatal("the job was not settled after its first, failed attempt")
	}
}

func TestRetryLoop_RetriesOnlyTheFailedSubscribers(
	t *testing.T,
) {
	var good countingHandler
	var flaky failsTwiceThenSucceeds
	f := newFixture(dispatcher.Config[int]{MaxAttempts: 5, DurableSubs: []subscription.Durable[int]{
		{Match: matchAnyEvent, Handler: good.countRun},
		{Match: matchAnyEvent, Handler: flaky.failTwiceThenSucceed},
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

	if good.runs.Load() != 1 ||
		flaky.runs.Load() != 3 {
		t.Fatalf(
			"good ran %d, flaky %d; want 1 and 3",
			good.runs.Load(),
			flaky.runs.Load(),
		)
	}
}

func TestRetryLoop_StopsAtTheAttemptCap(
	t *testing.T,
) {
	down := countingHandler{err: errors.New("down")}
	f := newFixture(dispatcher.Config[int]{MaxAttempts: 4, DurableSubs: []subscription.Durable[int]{{
		Match:   matchAnyEvent,
		Handler: down.countRun,
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
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Deliver = %v, want ErrDeliveryParked",
			err,
		)
	}

	if down.runs.Load() != 4 {
		t.Fatalf(
			"runs = %d, want exactly the cap (4)",
			down.runs.Load(),
		)
	}
}

func TestRetryLoop_DoublesTheBackoffUpToTheMaximum(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{RetryInitial: time.Millisecond, RetryMax: 3 * time.Millisecond})
	job, halt := f.begin(
		t,
		acked("test.event"),
	)
	loop := &retryLoop[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		halt:      halt,
		job:       job,
		pending:   []subscription.Durable[int]{failing(errors.New("down"))},
		attempt:   1,
		backoff:   time.Millisecond,
	}
	loop.deliverer.maxAttempts = 10

	var backoffs []time.Duration
	for range 3 {
		loop.advance()
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
	f := newFixture(dispatcher.Config[int]{})
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
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"wait on a parked job = %v, want ErrDeliveryParked: its backoff ended after the park and it would run its handlers again",
			err,
		)
	}
}

type recordsSettlementAtSecondRun struct {
	runs            atomic.Int32
	settledAtSecond chan bool
	job             *jobs.Job[int]
}

func (r *recordsSettlementAtSecondRun) recordSettlement(
	context.Context,
	event.Event[int],
) error {
	if r.runs.Add(1) == 1 {
		return errors.New("not yet")
	}

	r.settledAtSecond <- isClosed((*r.job).Ticket().Settled)
	return nil
}

type failsTwiceThenSucceeds struct {
	runs atomic.Int32
}

func (f *failsTwiceThenSucceeds) failTwiceThenSucceed(
	context.Context,
	event.Event[int],
) error {
	if f.runs.Add(1) < 3 {
		return errors.New("flaky")
	}

	return nil
}
