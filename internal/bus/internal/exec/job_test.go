package exec_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/v2/internal/bus/internal/exec"
	"github.com/char2cs/asynx/v2/internal/bus/models"
	"github.com/char2cs/asynx/v2/models/event"
)

func sendsLabel(
	marker chan<- string,
	label string,
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		marker <- label
	}
}

func TestMakeJob_AllFieldsMapped(
	t *testing.T,
) {
	timeout := 100 * time.Millisecond
	evt := event.Event[string]{EventName: "OrderPlaced", AggregateID: "agg-1"}

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
		evt,
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
		evt,
	)
	if got := <-marker; got != "handler" {
		t.Errorf(
			"Handler mapped incorrectly, got %q",
			got,
		)
	}

	job.FallbackHandler(
		context.Background(),
		evt,
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
			_ event.Event[string],
		) {
		},
		FallbackHandler: nil,
	}
	job := exec.MakeJob(
		sub,
		event.Event[string]{EventName: "e"},
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
			_ event.Event[string],
		) {
		},
	}
	onPanic := func(
		_ context.Context,
		_ event.Event[string],
		_ any,
	) {
	}
	onTimeout := func(
		_ context.Context,
		_ event.Event[string],
		_ time.Duration,
	) {
	}

	job := exec.MakeJob(
		sub,
		event.Event[string]{},
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
			_ event.Event[string],
		) {
		},
	}
	job := exec.MakeJob(
		sub,
		event.Event[string]{},
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
