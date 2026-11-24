package reporter_test

import (
	"context"
	"errors"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/reporter"
)

func TestNew_CallsTheHandlersItIsGiven(
	t *testing.T,
) {
	var publishes, deliveries, panics int
	onPublishError := func(
		context.Context,
		asynxmd.Event[string],
		error,
	) {
		publishes++
	}
	onDeliveryError := func(
		context.Context,
		asynxmd.Event[string],
		error,
		int,
	) {
		deliveries++
	}
	onPanic := func(
		context.Context,
		asynxmd.Event[string],
		any,
	) {
		panics++
	}
	r := reporter.New(
		onPublishError,
		onDeliveryError,
		onPanic,
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
	if publishes != 1 ||
		deliveries != 1 ||
		panics != 1 {
		t.Fatalf(
			"handlers ran %d, %d, %d times; want 1 each",
			publishes,
			deliveries,
			panics,
		)
	}
}
