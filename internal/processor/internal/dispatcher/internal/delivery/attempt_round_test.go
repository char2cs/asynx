package delivery

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/subscription"
)

func TestAttemptRound_KeepsOnlyTheFailedSubscribersAndJoinsTheirErrors(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{})
	job, _ := f.begin(
		t,
		acked("test.event"),
	)
	down := errors.New("down")
	round := &attemptRound[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		job:       job,
		attempt:   1,
	}

	round.run(
		context.Background(),
		[]subscription.Durable[int]{
			failing(nil),
			failing(down),
		},
	)

	if len(round.failed) != 1 ||
		!errors.Is(
			round.cause(),
			down,
		) {
		t.Fatalf(
			"failed = %d, cause = %v; want only the failing subscriber and its error",
			len(round.failed),
			round.cause(),
		)
	}

	reports := f.reports.all()
	if len(reports) != 1 ||
		reports[0].kind != "delivery" {
		t.Fatalf(
			"reports = %+v, want one delivery error",
			reports,
		)
	}
}

func TestAttemptRound_CauseIsNilWhenEverySubscriberSucceeds(
	t *testing.T,
) {
	f := newFixture(dispatcher.Config[int]{})
	job, _ := f.begin(
		t,
		acked("test.event"),
	)
	round := &attemptRound[int]{
		deliverer: f.deliverer.(*durableDeliverer[int]),
		job:       job,
		attempt:   1,
	}

	round.run(
		context.Background(),
		[]subscription.Durable[int]{failing(nil)},
	)

	if len(round.failed) != 0 ||
		round.cause() != nil {
		t.Fatalf(
			"failed = %d, cause = %v; want none",
			len(round.failed),
			round.cause(),
		)
	}
}
