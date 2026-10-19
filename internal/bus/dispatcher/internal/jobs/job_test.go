package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	asynxmd "github.com/char2cs/asynx/models"
)

func newJob(
	t *testing.T,
	request asynxmd.DispatchRequest[string],
) (jobs.Job[string], *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var cancels atomic.Int32
	job := jobs.New(ctx, request, func() {
		cancels.Add(1)
		cancel()
	})
	return job, &cancels
}

func closed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func eventRequest() asynxmd.DispatchRequest[string] {
	return asynxmd.DispatchRequest[string]{
		Event: asynxmd.Event[string]{AggregateID: "agg", Version: 3, EventName: "Created"},
	}
}

func TestNew_ExposesTheEvent(t *testing.T) {
	job, _ := newJob(t, eventRequest())

	if job.AggregateID() != "agg" || job.Version() != 3 {
		t.Fatalf("got (%q, %d), want (agg, 3)", job.AggregateID(), job.Version())
	}
	if job.Event().EventName != "Created" {
		t.Fatalf("event name = %q", job.Event().EventName)
	}
	if job.Context() == nil {
		t.Fatal("context is nil")
	}
}

func TestNew_KindsDecideRouting(t *testing.T) {
	ack := func(context.Context) error { return nil }
	forgetEvent := asynxmd.Event[string]{AggregateID: "agg", EventName: asynxmd.ForgetEventName}
	tests := []struct {
		name     string
		request  asynxmd.DispatchRequest[string]
		durable  bool
		forget   bool
		skipLive bool
	}{
		{"live event", eventRequest(), false, false, false},
		{"durable event", asynxmd.DispatchRequest[string]{Ack: ack}, true, false, false},
		{"tombstone", asynxmd.DispatchRequest[string]{Kind: asynxmd.DispatchTombstone}, true, true, false},
		{"redelivery", asynxmd.DispatchRequest[string]{Ack: ack, Kind: asynxmd.DispatchRedelivery}, true, false, true},
		{"forget redelivery", asynxmd.DispatchRequest[string]{Event: forgetEvent, Kind: asynxmd.DispatchRedelivery}, true, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertKindRouting(t, tt.request, [3]bool{tt.durable, tt.forget, tt.skipLive})
		})
	}
}

func assertKindRouting(
	t *testing.T,
	request asynxmd.DispatchRequest[string],
	want [3]bool,
) {
	t.Helper()
	job, _ := newJob(t, request)

	got := [3]bool{job.Durable(), job.Forget(), job.SkipLive()}
	if got != want {
		t.Fatalf("got durable=%v forget=%v skipLive=%v", job.Durable(), job.Forget(), job.SkipLive())
	}
	if (job.Ack() != nil) != (request.Ack != nil) {
		t.Fatalf("ack presence mismatch")
	}
	if job.Acked() != (request.Ack != nil) {
		t.Fatalf("acked = %v, want %v", job.Acked(), request.Ack != nil)
	}
}

func TestEnqueue_TheKindSelectsWhoReceivesTheEvent(
	t *testing.T,
) {
	type routing struct {
		forget   bool
		skipLive bool
	}
	cases := []struct {
		kind asynxmd.DispatchKind
		name string
		want routing
	}{
		{asynxmd.DispatchEvent, "test.event", routing{}},
		{asynxmd.DispatchEvent, asynxmd.ForgetEventName, routing{}},
		{asynxmd.DispatchRedelivery, "test.event", routing{skipLive: true}},
		{asynxmd.DispatchRedelivery, asynxmd.ForgetEventName, routing{skipLive: true, forget: true}},
		{asynxmd.DispatchTombstone, asynxmd.ForgetEventName, routing{forget: true}},
	}
	for _, c := range cases {
		request := asynxmd.DispatchRequest[string]{Event: asynxmd.Event[string]{EventName: c.name}, Kind: c.kind}
		job, _ := newJob(t, request)
		got := routing{forget: job.Forget(), skipLive: job.SkipLive()}
		t.Run(fmt.Sprintf("kind %d, event %q", c.kind, c.name), func(
			t *testing.T,
		) {
			assertRouting(t, got, c.want)
		})
	}
}

func assertRouting[R comparable](
	t *testing.T,
	got R,
	want R,
) {
	t.Helper()
	if got != want {
		t.Errorf("opts = %+v, want %+v", got, want)
	}
}

func TestJob_MarkSettledClosesSettledOnce(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	if closed(ticket.Settled) {
		t.Fatal("settled before MarkSettled")
	}
	job.MarkSettled()
	job.MarkSettled()

	if !closed(ticket.Settled) {
		t.Fatal("not settled after MarkSettled")
	}
	if ticket.Unrun() != nil {
		t.Fatalf("unrun = %v, want nil", ticket.Unrun())
	}
}

func TestJob_MarkUnrunRecordsReason(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	job.MarkUnrun(asynxmd.ErrDeliveryParked)

	if !closed(ticket.Settled) {
		t.Fatal("not settled after MarkUnrun")
	}
	if !errors.Is(ticket.Unrun(), asynxmd.ErrDeliveryParked) {
		t.Fatalf("unrun = %v", ticket.Unrun())
	}
}

func TestJob_SettleHappensOnce(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	job.MarkSettled()
	job.MarkUnrun(asynxmd.ErrDeliveryParked)

	if ticket.Unrun() != nil {
		t.Fatalf("a settled job was marked unrun: %v", ticket.Unrun())
	}
}

func TestJob_MarkUnrunFirstWins(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	job.MarkUnrun(asynxmd.ErrDeliveryParked)
	job.MarkSettled()
	job.MarkUnrun(errors.New("later"))

	if !errors.Is(ticket.Unrun(), asynxmd.ErrDeliveryParked) {
		t.Fatalf("unrun = %v", ticket.Unrun())
	}
}

func TestJob_CompleteRunsOnce(t *testing.T) {
	job, cancels := newJob(t, eventRequest())
	ticket := job.Ticket()
	var callbacks atomic.Int32
	job.OnComplete(func() { callbacks.Add(1) })
	first := errors.New("first")

	job.Complete(first)
	job.Complete(errors.New("second"))

	if !closed(ticket.Final) {
		t.Fatal("final not closed")
	}
	if !errors.Is(ticket.Err(), first) {
		t.Fatalf("err = %v, want first", ticket.Err())
	}
	if callbacks.Load() != 1 || cancels.Load() != 1 {
		t.Fatalf("callbacks=%d cancels=%d, want 1 and 1", callbacks.Load(), cancels.Load())
	}
	if job.Context().Err() == nil {
		t.Fatal("context not cancelled")
	}
}

func TestJob_CompleteWithoutCallback(t *testing.T) {
	job, _ := newJob(t, eventRequest())

	job.Complete(nil)

	if !closed(job.Ticket().Final) {
		t.Fatal("final not closed")
	}
}

func TestJob_ConcurrentCompleteRunsOnce(t *testing.T) {
	job, cancels := newJob(t, eventRequest())
	var callbacks atomic.Int32
	job.OnComplete(func() { callbacks.Add(1) })
	var wg sync.WaitGroup

	for range 16 {
		wg.Go(func() { job.Complete(nil) })
	}
	wg.Wait()

	if callbacks.Load() != 1 || cancels.Load() != 1 {
		t.Fatalf("callbacks=%d cancels=%d, want 1 and 1", callbacks.Load(), cancels.Load())
	}
}
