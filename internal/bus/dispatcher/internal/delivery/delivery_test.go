package delivery

import (
	"errors"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

func TestNew_TakesTheConfigAsFinal(t *testing.T) {
	cfg := asynxmd.DispatcherConfig[int]{
		DurableSubs:  []asynxmd.DurableSub[int]{failing(errors.New("down"))},
		RetryInitial: time.Second,
		RetryMax:     time.Minute,
		MaxAttempts:  7,
	}
	registry := aggregates.New(aggregates.Hooks[jobs.Job[int]]{})
	interrupted := make(chan struct{})

	d := New(cfg, registry, &recordingReporter{registry: registry}, interrupted).(*durableDeliverer[int])

	retry := retrySettings{d.retryInitial, d.retryMax, d.maxAttempts}
	if retry != (retrySettings{time.Second, time.Minute, 7}) {
		t.Fatalf("retry settings = %+v; want the config's", retry)
	}
	kept := len(d.subs) == 1 && d.deadLetter == nil
	if !kept {
		t.Fatal("New did not keep the subscribers and the dead-letter handler")
	}
	if d.interrupted != interrupted {
		t.Fatal("New did not keep the interrupt channel")
	}
}

type retrySettings struct {
	initial     time.Duration
	max         time.Duration
	maxAttempts int
}
