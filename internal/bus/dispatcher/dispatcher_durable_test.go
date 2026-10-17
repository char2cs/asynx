package dispatcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

func always(string) bool { return true }

func newDurable(t *testing.T, subs []DurableSub[int], opts ...Opt[int]) *Dispatcher[int] {
	t.Helper()
	all := append([]Opt[int]{WithDurableSubs(subs), WithRetryBackoff[int](time.Millisecond, 5*time.Millisecond)}, opts...)
	d := New[int](&recordingBus{}, all...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return d
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDurable_FailedHandlerRetriesAndHoldsBackLaterEventsOfTheAggregate(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var failuresLeft atomic.Int32
	failuresLeft.Store(3)

	var reports []int
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		mu.Lock()
		defer mu.Unlock()
		if e.AggregateID == "a" && e.Version == 1 && failuresLeft.Add(-1) >= 0 {
			order = append(order, "a1-fail")
			return errors.New("transient")
		}
		order = append(order, e.AggregateID+string(rune('0'+e.Version)))
		return nil
	}}}, WithDeliveryErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], _ error, attempt int) {
		mu.Lock()
		reports = append(reports, attempt)
		mu.Unlock()
	}))

	var acks atomic.Int32
	ack := func(context.Context) error { acks.Add(1); return nil }
	ctx := context.Background()
	d.Enqueue(ctx, makeEvent("a", 1), ack)
	d.Enqueue(ctx, makeEvent("a", 2), ack)
	doneB, _ := d.Enqueue(ctx, makeEvent("b", 1), ack)

	<-doneB // another aggregate is not held back by a's failure
	waitFor(t, "all three deliveries acked", func() bool { return acks.Load() == 3 })

	mu.Lock()
	defer mu.Unlock()
	// a2 must come after a1 finally succeeded; there were 3 failed attempts first.
	idx := map[string]int{}
	for i, name := range order {
		idx[name] = i
	}
	a1, ok1 := idx["a1"]
	a2, ok2 := idx["a2"]
	if !ok1 || !ok2 || a2 < a1 {
		t.Fatalf("a2 was delivered before a1 succeeded: %v", order)
	}
	for _, name := range order[:a1] {
		if name != "a1-fail" && name != "b1" {
			t.Fatalf("unexpected delivery before a1 succeeded: %v", order)
		}
	}
	if len(reports) != 3 || reports[0] != 1 || reports[1] != 2 || reports[2] != 3 {
		t.Fatalf("delivery error reports = %v, want attempts [1 2 3]", reports)
	}
}

func TestDurable_DoneClosesAfterTheFirstAttemptEvenWhenItFails(t *testing.T) {
	release := make(chan struct{})
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		select {
		case <-release:
			return nil
		default:
			return errors.New("not yet")
		}
	}}})

	var acked atomic.Bool
	done, err := d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the first (failed) attempt")
	}
	if acked.Load() {
		t.Fatal("acked a delivery whose handler failed")
	}

	close(release)
	waitFor(t, "ack after the retry succeeded", acked.Load)
}

func TestDurable_PanicIsAFailureThatRetriesAndReachesThePanicHandler(t *testing.T) {
	var attempts, panics atomic.Int32
	var lastErr atomic.Value
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}}},
		WithDeliveryPanicHandler[int](func(context.Context, asynxmd.Event[int], any) { panics.Add(1) }),
		WithDeliveryErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error, _ int) { lastErr.Store(err) }),
	)

	var acked atomic.Bool
	d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "ack after panic then success", acked.Load)

	if panics.Load() != 1 {
		t.Errorf("panic handler calls = %d, want 1", panics.Load())
	}
	if err, _ := lastErr.Load().(error); !errors.Is(err, asynxmd.ErrHandlerPanic) {
		t.Errorf("reported error = %v, want ErrHandlerPanic", err)
	}
}

func TestDurable_APanickingPanicHandlerCannotKillTheWorker(t *testing.T) {
	var attempts atomic.Int32
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}}}, WithDeliveryPanicHandler[int](func(context.Context, asynxmd.Event[int], any) { panic("handler of handlers") }))

	var acked atomic.Bool
	d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "recovery from a panicking panic handler", acked.Load)
}

func TestDurable_TimeoutIsReportedButTheHandlerIsStillWaitedFor(t *testing.T) {
	var running, overlapped atomic.Bool
	var timeouts atomic.Int32
	timedOut := make(chan struct{}, 8)
	d := newDurable(t, []DurableSub[int]{{
		Match:   always,
		Timeout: 10 * time.Millisecond,
		Handler: func(context.Context, asynxmd.Event[int]) error {
			if !running.CompareAndSwap(false, true) {
				overlapped.Store(true)
			}
			<-timedOut // the handler outlives its timeout
			running.Store(false)
			return nil
		},
	}}, WithDeliveryErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error, _ int) {
		if errors.Is(err, asynxmd.ErrHandlerTimeout) {
			timeouts.Add(1)
			timedOut <- struct{}{}
		}
	}))

	var acks atomic.Int32
	ack := func(context.Context) error { acks.Add(1); return nil }
	d.Enqueue(context.Background(), makeEvent("a", 1), ack)
	d.Enqueue(context.Background(), makeEvent("a", 2), ack)
	waitFor(t, "both acked", func() bool { return acks.Load() == 2 })

	if overlapped.Load() {
		t.Error("the next event's handler started while the timed-out one was still running")
	}
	if timeouts.Load() != 2 {
		t.Errorf("timeouts reported = %d, want 2", timeouts.Load())
	}
}

func TestDurable_OnlyFailedHandlersAreRetried(t *testing.T) {
	var goodRuns, flakyRuns atomic.Int32
	d := newDurable(t, []DurableSub[int]{
		{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error { goodRuns.Add(1); return nil }},
		{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
			if flakyRuns.Add(1) < 3 {
				return errors.New("flaky")
			}
			return nil
		}},
		{Match: func(string) bool { return false }, Handler: func(context.Context, asynxmd.Event[int]) error {
			t.Error("a non-matching subscriber ran")
			return nil
		}},
	})

	var acked atomic.Bool
	d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "ack", acked.Load)

	if goodRuns.Load() != 1 || flakyRuns.Load() != 3 {
		t.Fatalf("good ran %d times, flaky %d; want 1 and 3", goodRuns.Load(), flakyRuns.Load())
	}
}

func TestDurable_CloseInterruptsRetriesAndLeavesTheDeliveryUnacked(t *testing.T) {
	var attempts atomic.Int32
	d := New[int](&recordingBus{},
		WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
			attempts.Add(1)
			return errors.New("always failing")
		}}}),
		WithRetryBackoff[int](time.Hour, time.Hour),
	)

	var acked atomic.Bool
	done, _ := d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	<-done

	closed := make(chan error, 1)
	go func() { closed <- d.Close(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung on a retrying delivery")
	}
	if acked.Load() {
		t.Fatal("a failing delivery was acknowledged on close")
	}
}

func TestDurable_NonDurableJobsSkipDurableSubscribers(t *testing.T) {
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		t.Error("durable handler ran for a job without an ack")
		return nil
	}}})
	done, _ := d.Enqueue(context.Background(), makeEvent("a", 1), nil)
	<-done
}

func TestDurable_DeliveryWithNoMatchingSubscriberIsStillAcked(t *testing.T) {
	d := newDurable(t, nil)
	var acked atomic.Bool
	done, _ := d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	<-done
	if !acked.Load() {
		t.Fatal("delivery with nothing to deliver was not acknowledged")
	}
}

func TestHandle_DispatcherPanicIsReportedNotSwallowed(t *testing.T) {
	var got atomic.Value
	bus := &callbackBus{fn: func(context.Context, asynxmd.Event[int]) error { panic("bus exploded") }}
	d := New[int](bus, WithPublishErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error) { got.Store(err) }))
	defer d.Close(context.Background())

	done, _ := d.Enqueue(context.Background(), makeEvent("a", 1), nil)
	<-done
	if err, _ := got.Load().(error); err == nil {
		t.Fatal("a panic in the dispatcher was swallowed silently")
	}

	// The worker survives and keeps delivering.
	done, _ = d.Enqueue(context.Background(), makeEvent("a", 2), nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker died after a panic")
	}
}

func TestRetryBackoff_IgnoresNonPositiveValues(t *testing.T) {
	d := New[int](&recordingBus{}, WithRetryBackoff[int](0, -1))
	defer d.Close(context.Background())
	if d.retryInitial != defaultRetryInitial || d.retryMax != defaultRetryMax {
		t.Fatalf("backoff = %v..%v, want defaults", d.retryInitial, d.retryMax)
	}
}

func TestDurable_HandlerFinishingBeforeItsTimeoutReportsNothing(t *testing.T) {
	var reports atomic.Int32
	d := newDurable(t, []DurableSub[int]{{
		Match:   always,
		Timeout: time.Second,
		Handler: func(context.Context, asynxmd.Event[int]) error { return nil },
	}}, WithDeliveryErrorHandler[int](func(context.Context, asynxmd.Event[int], error, int) { reports.Add(1) }))

	var acked atomic.Bool
	d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "ack", acked.Load)
	if reports.Load() != 0 {
		t.Fatalf("reports = %d, want 0", reports.Load())
	}
}

func TestDurable_ATimedOutHandlerThatThenFailsIsRetried(t *testing.T) {
	var attempts atomic.Int32
	timedOut := make(chan struct{}, 1)
	d := newDurable(t, []DurableSub[int]{{
		Match:   always,
		Timeout: 5 * time.Millisecond,
		Handler: func(context.Context, asynxmd.Event[int]) error {
			if attempts.Add(1) == 1 {
				<-timedOut
				return errors.New("failed late")
			}
			return nil
		},
	}}, WithDeliveryErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error, _ int) {
		if errors.Is(err, asynxmd.ErrHandlerTimeout) {
			timedOut <- struct{}{}
		}
	}))

	var acked atomic.Bool
	d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "ack after the late failure was retried", acked.Load)
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}
