package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

func TestSubscriberCall_ATimedOutHandlerIsReportedOnceAndStillWaitedFor(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, _ := f.begin(
		t,
		acked("test.event"),
	)
	slow := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}
	call := &subscriberCall[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		job:       job,
		sub: asynxmd.DurableSub[int]{
			Name:    "slow",
			Timeout: time.Millisecond,
			Handler: slow,
		},
		attempt: 1,
	}

	err := call.run(context.Background())

	reports := f.reports.all()
	if err != nil ||
		len(reports) != 1 ||
		!errors.Is(
			reports[0].err,
			asynxmd.ErrHandlerTimeout,
		) {
		t.Fatalf(
			"run = %v, reports = %+v; want the handler's nil result and one timeout report",
			err,
			reports,
		)
	}
}

func TestSubscriberCall_APanicBecomesAnErrorAndIsReportedToThePanicHandler(
	t *testing.T,
) {
	f := newFixture(asynxmd.DispatcherConfig[int]{})
	job, _ := f.begin(
		t,
		acked("test.event"),
	)
	panicking := func(
		context.Context,
		asynxmd.Event[int],
	) error {
		panic("boom")
	}
	call := &subscriberCall[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		job:       job,
		sub:       asynxmd.DurableSub[int]{Name: "panicking", Handler: panicking},
		attempt:   1,
	}

	err := call.run(context.Background())
	if !errors.Is(
		err,
		asynxmd.ErrHandlerPanic,
	) ||
		len(f.reports.panics) != 1 ||
		f.reports.panics[0] != "boom" {
		t.Fatalf(
			"run = %v, panics = %v; want ErrHandlerPanic and the panic reported",
			err,
			f.reports.panics,
		)
	}
}
