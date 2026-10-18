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

	if d.retryInitial != time.Second || d.retryMax != time.Minute || d.maxAttempts != 7 {
		t.Fatalf("retry settings = %v, %v, %d; want the config's", d.retryInitial, d.retryMax, d.maxAttempts)
	}
	if len(d.subs) != 1 || d.deadLetter != nil || d.interrupted != interrupted {
		t.Fatal("New did not keep the subscribers, the dead-letter handler and the interrupt channel")
	}
}
