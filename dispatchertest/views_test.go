package dispatchertest

import (
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func TestViews_ReceiveFailsTheScenarioWhenNothingArrives(t *testing.T) {
	if !fails(func(
		t testing.TB,
	) {
		receive(t, time.Millisecond, make(chan int), "nothing")
	}) {
		t.Fatal("receive did not fail on a silent channel")
	}
}

func TestViews_ShapeEventsForAssertions(t *testing.T) {
	events := []models.Event[Aggregate]{event("a", 1), event("b", 1), event("a", 2), event("a", 2)}
	if got := versionsOf(events); !slices.Equal(got, []int64{1, 1, 2, 2}) || distinct(got) != 2 {
		t.Fatalf("versions = %v, distinct %d", got, distinct(got))
	}
	want := map[string][]int64{"a": {1, 2, 2}, "b": {1}}
	if got := byAggregate(events); !maps.EqualFunc(got, want, slices.Equal) || !slices.Equal(sequence(3), []int64{1, 2, 3}) {
		t.Fatalf("byAggregate = %v, sequence(3) = %v", got, sequence(3))
	}
	if got := without([]string{"a/1", "b/1", "a/2"}, "b/"); !slices.Equal(got, []string{"a/1", "a/2"}) {
		t.Fatalf("without = %v", got)
	}
}

func TestViews_SubscriberNamesSkipsErrorsWithoutASubscriber(t *testing.T) {
	joined := errors.Join(&models.SubscriberError{Subscriber: "s#1", Err: errDown}, errDown)
	if got := subscriberNames(joined); !slices.Equal(got, []string{"s#1"}) {
		t.Fatalf("names = %v, want [s#1]", got)
	}
	if got := unjoin(errDown); len(got) != 1 {
		t.Fatalf("unjoin of a plain error = %v, want it alone", got)
	}
}

func TestViews_ParksAndResume(t *testing.T) {
	h := harness{factory: defaultFactory, patience: patience}
	d := h.build(t, config(&bus{}))
	d.Park("b", errImposed)
	d.Park("a", errImposed)
	if got := parked(d); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("parked = %v, want [a b]", got)
	}
	if !resume(d, "a") || resume(d, "a") || isClosed(make(chan struct{})) {
		t.Fatal("resume must lift a park once, and an open channel is not closed")
	}
}
