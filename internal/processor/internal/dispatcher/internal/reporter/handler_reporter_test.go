package reporter_test

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/reporter"
	asynxmd "github.com/char2cs/asynx/models"
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
	request := asynxmd.DispatchRequest[string]{
		Event: asynxmd.Event[string]{AggregateID: "agg", Version: 1},
	}
	return jobs.New(
		request,
		func() {},
	)
}

func TestReporter_PublishErrorCallsHandler(
	t *testing.T,
) {
	var gotErr error
	var gotEvent asynxmd.Event[string]
	var gotCtx context.Context
	want := errors.New("boom")
	onPublishError := func(
		ctx context.Context,
		e asynxmd.Event[string],
		err error,
	) {
		gotCtx, gotEvent, gotErr = ctx, e, err
	}
	rep := reporter.New(
		onPublishError,
		nil,
		nil,
	)

	rep.PublishError(
		markedContext(),
		newJob(),
		want,
	)

	if !errors.Is(
		gotErr,
		want,
	) ||
		gotEvent.AggregateID != "agg" {
		t.Fatalf(
			"got err=%v event=%+v",
			gotErr,
			gotEvent,
		)
	}

	if gotCtx.Value(ctxKey{}) != "marker" {
		t.Fatal("handler did not get the job context")
	}
}

func TestReporter_DeliveryErrorCallsHandler(
	t *testing.T,
) {
	var gotErr error
	var gotAttempt int
	want := errors.New("boom")
	onDeliveryError := func(
		_ context.Context,
		_ asynxmd.Event[string],
		err error,
		attempt int,
	) {
		gotErr, gotAttempt = err, attempt
	}
	rep := reporter.New(
		nil,
		onDeliveryError,
		nil,
	)

	rep.DeliveryError(
		markedContext(),
		newJob(),
		want,
		4,
	)

	if !errors.Is(
		gotErr,
		want,
	) ||
		gotAttempt != 4 {
		t.Fatalf(
			"got err=%v attempt=%d",
			gotErr,
			gotAttempt,
		)
	}
}

func TestReporter_PanicCallsHandler(
	t *testing.T,
) {
	var got any
	onPanic := func(
		_ context.Context,
		_ asynxmd.Event[string],
		recovered any,
	) {
		got = recovered
	}
	rep := reporter.New(
		nil,
		nil,
		onPanic,
	)

	rep.Panic(
		markedContext(),
		newJob(),
		"oops",
	)

	if got != "oops" {
		t.Fatalf(
			"got %v",
			got,
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
	var got []any
	onPublishError := func(
		context.Context,
		asynxmd.Event[string],
		error,
	) {
		panic("publish")
	}
	onDeliveryError := func(
		context.Context,
		asynxmd.Event[string],
		error,
		int,
	) {
		panic("delivery")
	}
	onPanic := func(
		_ context.Context,
		_ asynxmd.Event[string],
		recovered any,
	) {
		got = append(
			got,
			recovered,
		)
	}
	rep := reporter.New(
		onPublishError,
		onDeliveryError,
		onPanic,
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

	if len(got) != 2 ||
		got[0] != "publish" ||
		got[1] != "delivery" {
		t.Fatalf(
			"panic handler saw %v, want the publish and delivery panics",
			got,
		)
	}
}

func TestReporter_DropsAPanicOfThePanicHandler(
	t *testing.T,
) {
	onPanic := func(
		context.Context,
		asynxmd.Event[string],
		any,
	) {
		panic("panic")
	}
	rep := reporter.New[string](
		nil,
		nil,
		onPanic,
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
	onPublishError := func(
		context.Context,
		asynxmd.Event[string],
		error,
	) {
		panic("publish")
	}
	rep := reporter.New(
		onPublishError,
		nil,
		nil,
	)

	rep.PublishError(
		markedContext(),
		newJob(),
		errors.New("x"),
	)
}
