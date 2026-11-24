package reporter_test

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/reporter"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/event"
)

type ctxKey struct{}

func markedContext() context.Context {
	return context.WithValue(
		context.Background(),
		ctxKey{},
		"marker",
	)
}

func newJob() jobs.Job[string] {
	request := dispatch.Request[string]{
		Event: event.Event[string]{AggregateID: "agg", Version: 1},
	}
	return jobs.New(
		request,
		func() {},
	)
}

func TestReporter_PublishErrorCallsHandler(
	t *testing.T,
) {
	var calls recordedCalls
	want := errors.New("boom")
	rep := reporter.New(
		calls.recordPublishError,
		nil,
		nil,
	)

	rep.PublishError(
		markedContext(),
		newJob(),
		want,
	)

	if !errors.Is(
		calls.err,
		want,
	) ||
		calls.event.AggregateID != "agg" {
		t.Fatalf(
			"got err=%v event=%+v",
			calls.err,
			calls.event,
		)
	}

	if calls.ctx.Value(ctxKey{}) != "marker" {
		t.Fatal("handler did not get the job context")
	}
}

func TestReporter_DeliveryErrorCallsHandler(
	t *testing.T,
) {
	var calls recordedCalls
	want := errors.New("boom")
	rep := reporter.New(
		nil,
		calls.recordDeliveryError,
		nil,
	)

	rep.DeliveryError(
		markedContext(),
		newJob(),
		want,
		4,
	)

	if !errors.Is(
		calls.err,
		want,
	) ||
		calls.attempt != 4 {
		t.Fatalf(
			"got err=%v attempt=%d",
			calls.err,
			calls.attempt,
		)
	}
}

func TestReporter_PanicCallsHandler(
	t *testing.T,
) {
	var calls recordedCalls
	rep := reporter.New(
		nil,
		nil,
		calls.recordPanic,
	)

	rep.Panic(
		markedContext(),
		newJob(),
		"oops",
	)

	if len(calls.panics) != 1 ||
		calls.panics[0] != "oops" {
		t.Fatalf(
			"got %v",
			calls.panics,
		)
	}
}

func TestReporter_NilHandlersAreNoOps(
	t *testing.T,
) {
	rep := reporter.New[string](
		nil,
		nil,
		nil,
	)
	job := newJob()

	rep.PublishError(
		markedContext(),
		job,
		errors.New("x"),
	)
	rep.DeliveryError(
		markedContext(),
		job,
		errors.New("x"),
		1,
	)
	rep.Panic(
		markedContext(),
		job,
		"x",
	)
}

func TestReporter_ForwardsAnErrorHandlerPanicToThePanicHandler(
	t *testing.T,
) {
	var calls recordedCalls
	rep := reporter.New(
		panicOnPublishError,
		panicOnDeliveryError,
		calls.recordPanic,
	)
	job := newJob()

	rep.PublishError(
		markedContext(),
		job,
		errors.New("x"),
	)
	rep.DeliveryError(
		markedContext(),
		job,
		errors.New("x"),
		1,
	)

	if len(calls.panics) != 2 ||
		calls.panics[0] != "publish" ||
		calls.panics[1] != "delivery" {
		t.Fatalf(
			"panic handler saw %v, want the publish and delivery panics",
			calls.panics,
		)
	}
}

func TestReporter_DropsAPanicOfThePanicHandler(
	t *testing.T,
) {
	rep := reporter.New[string](
		nil,
		nil,
		panicOnPanic,
	)

	rep.Panic(
		markedContext(),
		newJob(),
		"x",
	)
}

func TestReporter_RecoversErrorHandlerPanicsWithoutAPanicHandler(
	t *testing.T,
) {
	rep := reporter.New(
		panicOnPublishError,
		nil,
		nil,
	)

	rep.PublishError(
		markedContext(),
		newJob(),
		errors.New("x"),
	)
}

type recordedCalls struct {
	ctx        context.Context
	event      event.Event[string]
	err        error
	attempt    int
	publishes  int
	deliveries int
	panics     []any
}

func (c *recordedCalls) recordPublishError(
	ctx context.Context,
	evt event.Event[string],
	err error,
) {
	c.publishes++
	c.ctx = ctx
	c.event = evt
	c.err = err
}

func (c *recordedCalls) recordDeliveryError(
	ctx context.Context,
	evt event.Event[string],
	err error,
	attempt int,
) {
	c.deliveries++
	c.ctx = ctx
	c.event = evt
	c.err = err
	c.attempt = attempt
}

func (c *recordedCalls) recordPanic(
	ctx context.Context,
	evt event.Event[string],
	recovered any,
) {
	c.ctx = ctx
	c.event = evt
	c.panics = append(
		c.panics,
		recovered,
	)
}

func panicOnPublishError(
	context.Context,
	event.Event[string],
	error,
) {
	panic("publish")
}

func panicOnDeliveryError(
	context.Context,
	event.Event[string],
	error,
	int,
) {
	panic("delivery")
}

func panicOnPanic(
	context.Context,
	event.Event[string],
	any,
) {
	panic("panic")
}
