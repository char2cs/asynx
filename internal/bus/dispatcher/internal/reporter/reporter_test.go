package reporter_test

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/reporter"
	asynxmd "github.com/char2cs/asynx/models"
)

type ctxKey struct{}

func newJob() jobs.Job[string] {
	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
	request := asynxmd.DispatchRequest[string]{
		Event: asynxmd.Event[string]{AggregateID: "agg", Version: 1},
	}
	return jobs.New(ctx, request, func() {})
}

func TestReporter_PublishErrorCallsHandler(t *testing.T) {
	var gotErr error
	var gotEvent asynxmd.Event[string]
	var gotCtx context.Context
	want := errors.New("boom")
	rep := reporter.New(
		func(ctx context.Context, e asynxmd.Event[string], err error) {
			gotCtx, gotEvent, gotErr = ctx, e, err
		},
		nil,
		nil,
	)

	rep.PublishError(newJob(), want)

	if !errors.Is(gotErr, want) || gotEvent.AggregateID != "agg" {
		t.Fatalf("got err=%v event=%+v", gotErr, gotEvent)
	}
	if gotCtx.Value(ctxKey{}) != "marker" {
		t.Fatal("handler did not get the job context")
	}
}

func TestReporter_DeliveryErrorCallsHandler(t *testing.T) {
	var gotErr error
	var gotAttempt int
	want := errors.New("boom")
	rep := reporter.New(
		nil,
		func(_ context.Context, _ asynxmd.Event[string], err error, attempt int) {
			gotErr, gotAttempt = err, attempt
		},
		nil,
	)

	rep.DeliveryError(newJob(), want, 4)

	if !errors.Is(gotErr, want) || gotAttempt != 4 {
		t.Fatalf("got err=%v attempt=%d", gotErr, gotAttempt)
	}
}

func TestReporter_PanicCallsHandler(t *testing.T) {
	var got any
	rep := reporter.New(
		nil,
		nil,
		func(_ context.Context, _ asynxmd.Event[string], recovered any) { got = recovered },
	)

	rep.Panic(newJob(), "oops")

	if got != "oops" {
		t.Fatalf("got %v", got)
	}
}

func TestReporter_NilHandlersAreNoOps(t *testing.T) {
	rep := reporter.New[string](nil, nil, nil)
	job := newJob()

	rep.PublishError(job, errors.New("x"))
	rep.DeliveryError(job, errors.New("x"), 1)
	rep.Panic(job, "x")
}

func TestReporter_SwallowsHandlerPanics(t *testing.T) {
	rep := reporter.New(
		func(context.Context, asynxmd.Event[string], error) { panic("publish") },
		func(context.Context, asynxmd.Event[string], error, int) { panic("delivery") },
		func(context.Context, asynxmd.Event[string], any) { panic("panic") },
	)
	job := newJob()

	rep.PublishError(job, errors.New("x"))
	rep.DeliveryError(job, errors.New("x"), 1)
	rep.Panic(job, "x")
}
