package reporter_test

import (
	"context"
	"errors"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/reporter"
)

func TestNew_CallsTheHandlersItIsGiven(t *testing.T) {
	var publishes, deliveries, panics int
	r := reporter.New(
		func(
			context.Context,
			asynxmd.Event[string],
			error,
		) {
			publishes++
		},
		func(
			context.Context,
			asynxmd.Event[string],
			error,
			int,
		) {
			deliveries++
		},
		func(
			context.Context,
			asynxmd.Event[string],
			any,
		) {
			panics++
		},
	)
	job := newJob()
	r.PublishError(job, errors.New("down"))
	r.DeliveryError(job, errors.New("down"), 1)
	r.Panic(job, "boom")
	if publishes != 1 || deliveries != 1 || panics != 1 {
		t.Fatalf("handlers ran %d, %d, %d times; want 1 each", publishes, deliveries, panics)
	}
}
