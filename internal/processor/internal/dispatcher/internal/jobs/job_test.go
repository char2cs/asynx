package jobs_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/event"
)

func newJob(
	t *testing.T,
	request dispatch.Request[string],
) (jobs.Job[string], *cancelCounter) {
	t.Helper()
	cancels := &cancelCounter{}
	job := jobs.New(
		request,
		cancels.countCancel,
	)
	return job,
		cancels
}

type cancelCounter struct {
	calls atomic.Int32
}

func (c *cancelCounter) countCancel() {
	c.calls.Add(1)
}

func acknowledgeAtOnce(
	context.Context,
) error {
	return nil
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

func eventRequest() dispatch.Request[string] {
	return dispatch.Request[string]{
		Event: event.Event[string]{AggregateID: "agg", Version: 3, EventName: "Created"},
	}
}

func TestNew_ExposesTheEvent(
	t *testing.T,
) {
	job, _ := newJob(
		t,
		eventRequest(),
	)
	if job.AggregateID() != "agg" ||
		job.Version() != 3 {
		t.Fatalf(
			"got (%q, %d), want (agg, 3)",
			job.AggregateID(),
			job.Version(),
		)
	}

	if job.Event().EventName != "Created" {
		t.Fatalf(
			"event name = %q",
			job.Event().EventName,
		)
	}
}

func TestNew_KindsDecideRouting(
	t *testing.T,
) {
	forgetEvent := event.Event[string]{AggregateID: "agg", EventName: event.ForgetEventName}
	tests := []struct {
		name     string
		request  dispatch.Request[string]
		durable  bool
		forget   bool
		skipLive bool
	}{
		{"live event", eventRequest(), false, false, false},
		{"durable event", dispatch.Request[string]{Ack: acknowledgeAtOnce}, true, false, false},
		{"tombstone", dispatch.Request[string]{Kind: dispatch.KindTombstone}, true, true, false},
		{"redelivery", dispatch.Request[string]{Ack: acknowledgeAtOnce, Kind: dispatch.KindRedelivery}, true, false, true},
		{"forget redelivery", dispatch.Request[string]{Event: forgetEvent, Kind: dispatch.KindRedelivery}, true, true, true},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			kindRoutingCase(
				tt.request,
				[3]bool{tt.durable, tt.forget, tt.skipLive},
			),
		)
	}
}

func kindRoutingCase(
	request dispatch.Request[string],
	want [3]bool,
) func(*testing.T) {
	return func(
		t *testing.T,
	) {
		assertKindRouting(
			t,
			request,
			want,
		)
	}
}

func routingCase[R comparable](
	got R,
	want R,
) func(*testing.T) {
	return func(
		t *testing.T,
	) {
		assertRouting(
			t,
			got,
			want,
		)
	}
}

func assertKindRouting(
	t *testing.T,
	request dispatch.Request[string],
	want [3]bool,
) {
	t.Helper()
	job, _ := newJob(
		t,
		request,
	)

	got := [3]bool{job.Durable(), job.Forget(), job.SkipLive()}
	if got != want {
		t.Fatalf(
			"got durable=%v forget=%v skipLive=%v",
			job.Durable(),
			job.Forget(),
			job.SkipLive(),
		)
	}

	if (job.Ack() != nil) != (request.Ack != nil) {
		t.Fatalf("ack presence mismatch")
	}

	if job.Acked() != (request.Ack != nil) {
		t.Fatalf(
			"acked = %v, want %v",
			job.Acked(),
			request.Ack != nil,
		)
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
		kind dispatch.Kind
		name string
		want routing
	}{
		{dispatch.KindEvent, "test.event", routing{}},
		{dispatch.KindEvent, event.ForgetEventName, routing{}},
		{dispatch.KindRedelivery, "test.event", routing{skipLive: true}},
		{dispatch.KindRedelivery, event.ForgetEventName, routing{skipLive: true, forget: true}},
		{dispatch.KindTombstone, event.ForgetEventName, routing{forget: true}},
	}
	for _, c := range cases {
		request := dispatch.Request[string]{Event: event.Event[string]{EventName: c.name}, Kind: c.kind}
		job, _ := newJob(
			t,
			request,
		)
		got := routing{forget: job.Forget(), skipLive: job.SkipLive()}
		t.Run(
			fmt.Sprintf(
				"kind %d, event %q",
				c.kind,
				c.name,
			),
			routingCase(
				got,
				c.want,
			),
		)
	}
}

func assertRouting[R comparable](
	t *testing.T,
	got R,
	want R,
) {
	t.Helper()
	if got != want {
		t.Errorf(
			"opts = %+v, want %+v",
			got,
			want,
		)
	}
}
