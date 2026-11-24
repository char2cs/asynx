package reporter_test

import (
	"errors"
	"testing"

	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/reporter"
)

func TestNew_CallsTheHandlersItIsGiven(
	t *testing.T,
) {
	var calls recordedCalls
	r := reporter.New(
		calls.recordPublishError,
		calls.recordDeliveryError,
		calls.recordPanic,
	)
	job := newJob()
	r.PublishError(
		markedContext(),
		job,
		errors.New("down"),
	)
	r.DeliveryError(
		markedContext(),
		job,
		errors.New("down"),
		1,
	)
	r.Panic(
		markedContext(),
		job,
		"boom",
	)
	if calls.publishes != 1 ||
		calls.deliveries != 1 ||
		len(calls.panics) != 1 {
		t.Fatalf(
			"handlers ran %d, %d, %d times; want 1 each",
			calls.publishes,
			calls.deliveries,
			len(calls.panics),
		)
	}
}
