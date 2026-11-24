package exec_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus/internal/exec"
	"github.com/char2cs/asynx/internal/bus/models"
	asynxmd "github.com/char2cs/asynx/models"
)

func sendsLabel(
	marker chan<- string,
	label string,
) eventHandler {
	return func(
		_ context.Context,
		_ asynxmd.Event[string],
	) {
		marker <- label
	}
}

func TestMakeJob_AllFieldsMapped(
	t *testing.T,
) {
	timeout := 100 * time.Millisecond
	event := asynxmd.Event[string]{EventName: "OrderPlaced", AggregateID: "agg-1"}

	marker := make(
		chan string,
		2,
	)
	sub := &models.Subscription[string]{
		Pattern: "OrderPlaced",
		Handler: sendsLabel(
			marker,
			"handler",
		),

		FallbackHandler: sendsLabel(
			marker,
			"fallback",
		),

		Timeout: timeout,
	}

	job := exec.MakeJob(
		sub,
		event,
		nil,
		nil,
	)

	var _ *exec.HandlerJob[string] = job

	if job.Event.EventName != "OrderPlaced" {
		t.Errorf(
			"Event.EventName: got %q",
			job.Event.EventName,
		)
	}

	if job.Event.AggregateID != "agg-1" {
		t.Errorf(
			"Event.AggregateID: got %q",
			job.Event.AggregateID,
		)
	}

	if job.Timeout != timeout {
		t.Errorf(
			"Timeout: got %v, want %v",
			job.Timeout,
			timeout,
		)
	}

	job.Handler(
		context.Background(),
		event,
	)
	if got := <-marker; got != "handler" {
		t.Errorf(
			"Handler mapped incorrectly, got %q",
			got,
		)
	}

	job.FallbackHandler(
		context.Background(),
		event,
	)
	if got := <-marker; got != "fallback" {
		t.Errorf(
			"FallbackHandler mapped incorrectly, got %q",
			got,
		)
	}
}

func TestMakeJob_NilFallbackPreserved(
	t *testing.T,
) {
	sub := &models.Subscription[string]{
		Handler: func(
			_ context.Context,
			_ asynxmd.Event[string],
		) {
		},
		FallbackHandler: nil,
	}
	job := exec.MakeJob(
		sub,
		asynxmd.Event[string]{EventName: "e"},
		nil,
		nil,
	)
	if job.FallbackHandler != nil {
		t.Error("FallbackHandler should be nil")
	}
}

func TestMakeJob_CallbacksPopulated(
	t *testing.T,
) {
	sub := &models.Subscription[string]{
		Handler: func(
			_ context.Context,
			_ asynxmd.Event[string],
		) {
		},
	}
	onPanic := func(
		_ context.Context,
		_ asynxmd.Event[string],
		_ any,
	) {
	}
	onTimeout := func(
		_ context.Context,
		_ asynxmd.Event[string],
		_ time.Duration,
	) {
	}

	job := exec.MakeJob(
		sub,
		asynxmd.Event[string]{},
		onPanic,
		onTimeout,
	)
	if job.OnPanic == nil {
		t.Error("OnPanic should not be nil")
	}

	if job.OnTimeout == nil {
		t.Error("OnTimeout should not be nil")
	}
}

func TestMakeJob_NilCallbacksPreserved(
	t *testing.T,
) {
	sub := &models.Subscription[string]{
		Handler: func(
			_ context.Context,
			_ asynxmd.Event[string],
		) {
		},
	}
	job := exec.MakeJob(
		sub,
		asynxmd.Event[string]{},
		nil,
		nil,
	)
	if job.OnPanic != nil {
		t.Error("OnPanic should be nil")
	}

	if job.OnTimeout != nil {
		t.Error("OnTimeout should be nil")
	}
}
