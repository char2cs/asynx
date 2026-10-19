package delivery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

func TestRetryLoop_SettlesTheJobAfterTheFirstAttempt(t *testing.T) {
	var runs atomic.Int32
	var job jobs.Job[int]
	settledAtSecond := make(chan bool, 1)
	f := newFixture(asynxmd.DispatcherConfig[int]{DurableSubs: []asynxmd.DurableSub[int]{{
		Match: func(string) bool { return true },
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if runs.Add(1) == 1 {
				return errors.New("not yet")
			}
			settledAtSecond <- isClosed(job.Ticket().Settled)
			return nil
		},
	}}})
	job, halt := f.pop(t, acked("test.event"))

	if err := f.deliverer.Deliver("a", halt, job); err != nil {
		t.Fatalf("Deliver = %v", err)
	}
	if !<-settledAtSecond {
		t.Fatal("the job was not settled after its first, failed attempt")
	}
}

func TestRetryLoop_RetriesOnlyTheFailedSubscribers(t *testing.T) {
	var goodRuns, flakyRuns atomic.Int32
	f := newFixture(asynxmd.DispatcherConfig[int]{MaxAttempts: 5, DurableSubs: []asynxmd.DurableSub[int]{
		{Match: func(string) bool { return true }, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			goodRuns.Add(1)
			return nil
		}},
		{Match: func(string) bool { return true }, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if flakyRuns.Add(1) < 3 {
				return errors.New("flaky")
			}
			return nil
		}},
	}})
	job, halt := f.pop(t, acked("test.event"))

	if err := f.deliverer.Deliver("a", halt, job); err != nil {
		t.Fatalf("Deliver = %v", err)
	}
	if goodRuns.Load() != 1 || flakyRuns.Load() != 3 {
		t.Fatalf("good ran %d, flaky %d; want 1 and 3", goodRuns.Load(), flakyRuns.Load())
	}
}

func TestRetryLoop_StopsAtTheAttemptCap(t *testing.T) {
	var runs atomic.Int32
	f := newFixture(asynxmd.DispatcherConfig[int]{MaxAttempts: 4, DurableSubs: []asynxmd.DurableSub[int]{{
		Match: func(string) bool { return true },
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			runs.Add(1)
			return errors.New("down")
		},
	}}})
	job, halt := f.pop(t, acked("test.event"))

	if err := f.deliverer.Deliver("a", halt, job); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Deliver = %v, want ErrDeliveryParked", err)
	}
	if runs.Load() != 4 {
		t.Fatalf("runs = %d, want exactly the cap (4)", runs.Load())
	}
}

func TestRetryLoop_DoublesTheBackoffUpToTheMaximum(t *testing.T) {
	f := newFixture(asynxmd.DispatcherConfig[int]{RetryInitial: time.Millisecond, RetryMax: 3 * time.Millisecond})
	job, halt := f.pop(t, acked("test.event"))
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
		loop.step()
		backoffs = append(backoffs, loop.backoff)
	}

	want := []time.Duration{2 * time.Millisecond, 3 * time.Millisecond, 3 * time.Millisecond}
	for i := range want {
		assertBackoff(t, backoffs[i], want[i])
	}
}

func assertBackoff(
	t *testing.T,
	got time.Duration,
	want time.Duration,
) {
	t.Helper()
	if got != want {
		t.Fatalf("backoff = %v, want %v", got, want)
	}
}

func TestRetryLoop_AParkedJobIsNotRetriedWhenItsBackoffEnds(t *testing.T) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, _ := f.pop(t, acked("test.event"))
	deliverer := f.deliverer.(*durableDeliverer[int])
	parked := make(chan struct{})
	close(parked)

	for range 500 {
		requireParkedWait(t, deliverer, job, parked)
	}
}

func requireParkedWait(
	t *testing.T,
	deliverer *durableDeliverer[int],
	job jobs.Job[int],
	parked <-chan struct{},
) {
	t.Helper()
	err := deliverer.wait("a", parked, job, 0)
	if !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("wait on a parked job = %v, want ErrDeliveryParked: its backoff ended after the park and it would run its handlers again", err)
	}
}
