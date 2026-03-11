package utils_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus/exec"
	"github.com/char2cs/asynx/internal/bus/models"
	"github.com/char2cs/asynx/internal/bus/utils"
	asynxmd "github.com/char2cs/asynx/models"
)

func TestMakeJob_AllFieldsMapped(t *testing.T) {
	timeout := 100 * time.Millisecond
	event := asynxmd.Event[string]{EventName: "OrderPlaced", AggregateID: "agg-1"}

	marker := make(chan string, 2)
	sub := &models.Subscription[string]{
		Pattern:         "OrderPlaced",
		Handler:         func(_ context.Context, _ asynxmd.Event[string]) { marker <- "handler" },
		FallbackHandler: func(_ context.Context, _ asynxmd.Event[string]) { marker <- "fallback" },
		Timeout:         timeout,
	}

	job := utils.MakeJob(sub, event, context.Background(), nil, nil)

	// Return type must be *exec.HandlerJob[string].
	var _ *exec.HandlerJob[string] = job

	if job.Event.EventName != "OrderPlaced" {
		t.Errorf("Event.EventName: got %q", job.Event.EventName)
	}
	if job.Event.AggregateID != "agg-1" {
		t.Errorf("Event.AggregateID: got %q", job.Event.AggregateID)
	}
	if job.Timeout != timeout {
		t.Errorf("Timeout: got %v, want %v", job.Timeout, timeout)
	}

	// Verify Handler and FallbackHandler are not swapped.
	job.Handler(context.Background(), event)
	if got := <-marker; got != "handler" {
		t.Errorf("Handler mapped incorrectly, got %q", got)
	}
	job.FallbackHandler(context.Background(), event)
	if got := <-marker; got != "fallback" {
		t.Errorf("FallbackHandler mapped incorrectly, got %q", got)
	}
}

func TestMakeJob_NilFallbackPreserved(t *testing.T) {
	sub := &models.Subscription[string]{
		Handler:         func(_ context.Context, _ asynxmd.Event[string]) {},
		FallbackHandler: nil,
	}
	job := utils.MakeJob(sub, asynxmd.Event[string]{EventName: "e"}, context.Background(), nil, nil)

	if job.FallbackHandler != nil {
		t.Error("FallbackHandler should be nil")
	}
}

func TestMakeJob_CallbacksPopulated(t *testing.T) {
	sub := &models.Subscription[string]{
		Handler: func(_ context.Context, _ asynxmd.Event[string]) {},
	}
	ctx := context.Background()
	onPanic := func(_ context.Context, _ asynxmd.Event[string], _ any) {}
	onTimeout := func(_ context.Context, _ asynxmd.Event[string], _ time.Duration) {}

	job := utils.MakeJob(sub, asynxmd.Event[string]{}, ctx, onPanic, onTimeout)

	if job.Ctx != ctx {
		t.Error("Ctx not mapped")
	}
	if job.OnPanic == nil {
		t.Error("OnPanic should not be nil")
	}
	if job.OnTimeout == nil {
		t.Error("OnTimeout should not be nil")
	}
}

func TestMakeJob_NilCallbacksPreserved(t *testing.T) {
	sub := &models.Subscription[string]{
		Handler: func(_ context.Context, _ asynxmd.Event[string]) {},
	}
	job := utils.MakeJob(sub, asynxmd.Event[string]{}, context.Background(), nil, nil)

	if job.OnPanic != nil {
		t.Error("OnPanic should be nil")
	}
	if job.OnTimeout != nil {
		t.Error("OnTimeout should be nil")
	}
}
