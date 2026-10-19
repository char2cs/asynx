package dispatchertest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestHandlers_MatchersAndFixedOutcomes(t *testing.T) {
	ctx := context.Background()
	matches := always("x") && !never("x") && onlyTestEvents(eventName) && !onlyTestEvents("other")
	outcomesFixed := succeed(ctx, event("a", 1)) == nil && errors.Is(fail(ctx, event("a", 1)), errDown)
	if !matches || !outcomesFixed {
		t.Fatal("a matcher or a fixed handler does not do what its name says")
	}
	if sub := subscriber(succeed); sub.Name == "" || !sub.Match("anything") {
		t.Fatalf("subscriber = %+v, want a named subscriber matching everything", sub)
	}
}

func TestHandlers_ConditionalHelpersActOnlyWhenTheConditionHolds(t *testing.T) {
	var flag atomic.Bool
	var counter atomic.Int32
	ch := make(chan int, 1)
	flagIf(false, &flag)
	countIf(false, &counter)
	sendIf(false, ch, 1)
	holdIf(false, nil)
	panicIf(false, "never")
	actedOnFalse := errorIf(false, errDown) != nil || flag.Load()
	leftover := counter.Load() != 0 || len(ch) != 0
	if actedOnFalse || leftover {
		t.Fatal("a helper acted on a false condition")
	}
	flagIf(true, &flag)
	countIf(true, &counter)
	sendIf(true, ch, 1)
	sendIf(true, ch, 2)
	actedOnTrue := errors.Is(errorIf(true, errDown), errDown) && flag.Load() && counter.Load() == 1
	if !actedOnTrue || <-ch != 1 {
		t.Fatal("a helper did not act on a true condition, or a full channel blocked")
	}
}

func recovered(
	fn func(),
) (value any) {
	defer func() { value = recover() }()
	fn()
	return nil
}

func TestHandlers_PanicIfPanicsWithTheValue(t *testing.T) {
	if got := recovered(func() { panicIf(true, "boom") }); got != "boom" {
		t.Fatalf("recovered %v, want boom", got)
	}
}

func TestHandlers_HoldFirstHoldsOnlyTheFirstCall(t *testing.T) {
	var once sync.Once
	entered := make(chan struct{})
	release := make(chan struct{})
	close(release)
	holdFirst(&once, entered, release)
	holdFirst(&once, entered, release)
	if !isClosed(entered) {
		t.Fatal("the first call did not signal that it entered")
	}
}
