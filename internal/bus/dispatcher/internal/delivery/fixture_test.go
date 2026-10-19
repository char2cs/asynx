package delivery

import (
	"context"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type fixture struct {
	registry    aggregates.Registry[jobs.Job[int]]
	reports     *recordingReporter
	interrupted chan struct{}
	deliverer   Deliverer[int]
}

func newFixture(
	cfg asynxmd.DispatcherConfig[int],
) *fixture {
	registry := aggregates.New(aggregates.Hooks[jobs.Job[int]]{})
	f := &fixture{
		registry:    registry,
		reports:     &recordingReporter{registry: registry},
		interrupted: make(chan struct{}),
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryInitial == 0 {
		cfg.RetryInitial, cfg.RetryMax = time.Millisecond, time.Millisecond
	}
	f.deliverer = New(cfg, registry, f.reports, f.interrupted)
	return f
}

func (f *fixture) pop(
	t *testing.T,
	request asynxmd.DispatchRequest[int],
) (jobs.Job[int], <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := f.registry.Enqueue(jobs.New(ctx, request, cancel)); err != nil {
		t.Fatal(err)
	}
	job, halt, ok := f.registry.Pop(request.Event.AggregateID)
	if !ok {
		t.Fatal("the job was not queued")
	}
	return job, halt
}

func acked(
	name string,
) asynxmd.DispatchRequest[int] {
	return asynxmd.DispatchRequest[int]{
		Event: asynxmd.Event[int]{AggregateID: "a", Version: 1, EventName: name},
		Ack:   func(context.Context) error { return nil },
	}
}

func tombstone(
	ack func(context.Context) error,
) asynxmd.DispatchRequest[int] {
	return asynxmd.DispatchRequest[int]{
		Event: asynxmd.Event[int]{AggregateID: "a", Version: 2, EventName: asynxmd.ForgetEventName},
		Ack:   ack,
		Kind:  asynxmd.DispatchTombstone,
	}
}

func failing(
	err error,
) asynxmd.DurableSub[int] {
	return asynxmd.DurableSub[int]{
		Name:  "failing",
		Match: func(string) bool { return true },
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return err
		},
	}
}
