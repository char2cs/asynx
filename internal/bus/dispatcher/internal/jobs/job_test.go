package jobs_test

import (
	"context"
	"fmt"
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
