package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/subscription"
)

func TestSubscriberCall_ATimedOutHandlerIsReportedOnceAndStillWaitedFor(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{})
	job, _ := f.begin(
		t,
		acked("test.event"),
	)
	slow := sleepingHandler{pause: 30 * time.Millisecond}
	call := &subscriberCall[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		job:       job,
		sub: subscription.Durable[int]{
			Name:    "slow",
			Timeout: time.Millisecond,
			Handler: slow.sleepThenSucceed,
		},
		attempt: 1,
	}

	err := call.run(context.Background())

	reports := f.reports.all()
	if err != nil ||
		len(reports) != 1 ||
		!errors.Is(
			reports[0].err,
			failure.ErrHandlerTimeout,
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
	f := newFixture(dispatcher.Config[int]{})
	job, _ := f.begin(
		t,
		acked("test.event"),
	)
	call := &subscriberCall[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		job:       job,
		sub:       subscription.Durable[int]{Name: "panicking", Handler: panicWithBoom},
		attempt:   1,
	}

	err := call.run(context.Background())
	if !errors.Is(
		err,
		failure.ErrHandlerPanic,
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

type sleepingHandler struct {
	pause time.Duration
}

func (h sleepingHandler) sleepThenSucceed(
	context.Context,
	event.Event[int],
) error {
	time.Sleep(h.pause)
	return nil
}

func panicWithBoom(
	context.Context,
	event.Event[int],
) error {
	panic("boom")
}
