package delivery_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// Event a2 comes after a1 finally succeeded, which took 3 failed attempts,
// and another aggregate is not held back by a's failure.
func TestDurable_FailedHandlerRetriesAndHoldsBackLaterEventsOfTheAggregate(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var failuresLeft atomic.Int32
	failuresLeft.Store(3)

	var reports []int
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		mu.Lock()
		defer mu.Unlock()
		isFirstOfA := e.AggregateID == "a" && e.Version == 1
		if isFirstOfA && failuresLeft.Add(-1) >= 0 {
			order = append(order, "a1-fail")
			return errors.New("transient")
		}
		order = append(order, e.AggregateID+string(rune('0'+e.Version)))
		return nil
	}}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		_ error,
		attempt int,
	) {
		mu.Lock()
		reports = append(reports, attempt)
		mu.Unlock()
	}))

	var acks atomic.Int32
	ack := func(context.Context) error { acks.Add(1); return nil }
	ctx := context.Background()
	enqueueEvent(ctx, d, makeEvent("a", 1), ack)
	enqueueEvent(ctx, d, makeEvent("a", 2), ack)
	doneB, _ := enqueueEvent(ctx, d, makeEvent("b", 1), ack)

	<-doneB
	waitFor(t, "all three deliveries acked", func() bool { return acks.Load() == 3 })

	mu.Lock()
	defer mu.Unlock()
	idx := map[string]int{}
	for i, name := range order {
		idx[name] = i
	}
	a1, ok1 := idx["a1"]
	a2, ok2 := idx["a2"]
	deliveredInOrder := ok1 && ok2 && a2 >= a1
	if !deliveredInOrder {
		t.Fatalf("a2 was delivered before a1 succeeded: %v", order)
	}
	if slices.ContainsFunc(order[:a1], deliveredBeforeA1) {
		t.Fatalf("unexpected delivery before a1 succeeded: %v", order)
	}
	if !slices.Equal(reports, []int{1, 2, 3}) {
		t.Fatalf("delivery error reports = %v, want attempts [1 2 3]", reports)
	}
}

func TestDurable_DoneClosesAfterTheFirstAttemptEvenWhenItFails(t *testing.T) {
	release := make(chan struct{})
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		select {
		case <-release:
			return nil
		default:
			return errors.New("not yet")
		}
	}}})

	var acked atomic.Bool
	done, err := enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
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
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}}},
		withDeliveryPanicHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			any,
		) {
			panics.Add(1)
		}),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			lastErr.Store(err)
		}),
	)

	var acked atomic.Bool
	enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
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
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}}}, withDeliveryPanicHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		any,
	) {
		panic("handler of handlers")
	}))

	var acked atomic.Bool
	enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "recovery from a panicking panic handler", acked.Load)
}

// The timeout is reported, and the delivery still waits for a handler that
// outlives it.
func TestDurable_TimeoutIsReportedButTheHandlerIsStillWaitedFor(t *testing.T) {
	var running, overlapped atomic.Bool
	var timeouts atomic.Int32
	timedOut := make(chan struct{}, 8)
	d := newDurable(t, []asynxmd.DurableSub[int]{{
		Match:   always,
		Timeout: 10 * time.Millisecond,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if !running.CompareAndSwap(false, true) {
				overlapped.Store(true)
			}
			<-timedOut
			running.Store(false)
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
		_ int,
	) {
		if errors.Is(err, asynxmd.ErrHandlerTimeout) {
			timeouts.Add(1)
			timedOut <- struct{}{}
		}
	}))

	var acks atomic.Int32
	ack := func(context.Context) error { acks.Add(1); return nil }
	enqueueEvent(context.Background(), d, makeEvent("a", 1), ack)
	enqueueEvent(context.Background(), d, makeEvent("a", 2), ack)
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
	d := newDurable(t, []asynxmd.DurableSub[int]{
		{Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			goodRuns.Add(1)
			return nil
		}},
		{Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if flakyRuns.Add(1) < 3 {
				return errors.New("flaky")
			}
			return nil
		}},
		{Match: func(string) bool { return false }, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			t.Error("a non-matching subscriber ran")
			return nil
		}},
	})

	var acked atomic.Bool
	enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "ack", acked.Load)

	if goodRuns.Load() != 1 || flakyRuns.Load() != 3 {
		t.Fatalf("good ran %d times, flaky %d; want 1 and 3", goodRuns.Load(), flakyRuns.Load())
	}
}

func TestDurable_CloseInterruptsRetriesAndLeavesTheDeliveryUnacked(t *testing.T) {
	var attempts atomic.Int32
	d := newTestDispatcher[int](&recordingBus{},
		withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			attempts.Add(1)
			return errors.New("always failing")
		}}}),
		withRetryBackoff[int](time.Hour, time.Hour),
	)

	var acked atomic.Bool
	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	<-done

	closed := make(chan error, 1)
	go func() { closed <- d.Close(context.Background()) }()
	select {
	case err := <-closed:
		checkClosed(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung on a retrying delivery")
	}
	if acked.Load() {
		t.Fatal("a failing delivery was acknowledged on close")
	}
}

func TestDurable_DeliveryWithNoMatchingSubscriberIsStillAcked(t *testing.T) {
	d := newDurable(t, nil)
	var acked atomic.Bool
	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	<-done
	if !acked.Load() {
		t.Fatal("delivery with nothing to deliver was not acknowledged")
	}
}

func TestDurable_HandlerFinishingBeforeItsTimeoutReportsNothing(t *testing.T) {
	var reports atomic.Int32
	d := newDurable(t, []asynxmd.DurableSub[int]{{
		Match:   always,
		Timeout: time.Second,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
		int,
	) {
		reports.Add(1)
	}))

	var acked atomic.Bool
	enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "ack", acked.Load)
	if reports.Load() != 0 {
		t.Fatalf("reports = %d, want 0", reports.Load())
	}
}

func TestDurable_ATimedOutHandlerThatThenFailsIsRetried(t *testing.T) {
	var attempts atomic.Int32
	timedOut := make(chan struct{}, 1)
	d := newDurable(t, []asynxmd.DurableSub[int]{{
		Match:   always,
		Timeout: 5 * time.Millisecond,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			if attempts.Add(1) == 1 {
				<-timedOut
				return errors.New("failed late")
			}
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
		_ int,
	) {
		if errors.Is(err, asynxmd.ErrHandlerTimeout) {
			timedOut <- struct{}{}
		}
	}))

	var acked atomic.Bool
	enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	waitFor(t, "ack after the late failure was retried", acked.Load)
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne(t *testing.T) {
	var rec deliveries
	attempted := make(chan struct{}, 64)
	d := newTestDispatcher[int](&recordingBus{},
		withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			_ context.Context,
			e asynxmd.Event[int],
		) error {
			rec.add(e)
			if e.Version == 1 {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}}}),
		withRetryBackoff[int](time.Hour, time.Hour),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := enqueueTicket(ctx, d, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := enqueueTicket(ctx, d, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))
	recv(t, attempted, "first attempt of v1")

	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for v, got := range map[int]error{1: final(t, t1, "v1 final"), 2: final(t, t2, "v2 final")} {
		checkNotDelivered(t, v, got)
	}
	for _, v := range []int64{1, 2} {
		checkNotAcked(t, &acked, v)
	}
	if slices.Contains(rec.list(), "a/2") {
		t.Fatalf("v2 was delivered while v1 was still pending: %v", rec.list())
	}
}

// A later event of the parked aggregate is not queued either, while another
// aggregate keeps flowing.
func TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone(t *testing.T) {
	var attempts atomic.Int32
	var rec deliveries
	var mu sync.Mutex
	var reports []error
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		if e.AggregateID == "a" && e.Version == 1 {
			attempts.Add(1)
			return errors.New("poison")
		}
		return nil
	}}},
		withMaxAttempts[int](4),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			mu.Lock()
			reports = append(reports, err)
			mu.Unlock()
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := enqueueTicket(ctx, d, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := enqueueTicket(ctx, d, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))

	if err := final(t, t1, "v1 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := final(t, t2, "v2 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	if n := attempts.Load(); n != 4 {
		t.Fatalf("attempts = %d, want exactly the cap (4)", n)
	}
	if got := halted(d); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Halted = %v, want [a]", got)
	}
	if slices.Contains(rec.list(), "a/2") {
		t.Fatal("v2 delivered although v1 was parked")
	}

	t3, _ := enqueueTicket(ctx, d, makeEvent("a", 3), ackInto(&acked, makeEvent("a", 3)))
	if err := final(t, t3, "v3 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v3 final = %v, want ErrDeliveryParked", err)
	}

	tb, _ := enqueueTicket(ctx, d, makeEvent("b", 1), ackInto(&acked, makeEvent("b", 1)))
	if err := final(t, tb, "b final"); err != nil {
		t.Fatalf("aggregate b held back: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	parkedReports := len(slices.DeleteFunc(slices.Clone(reports), notParked))
	if parkedReports != 1 {
		t.Fatalf("parked reports = %d, want 1 (%v)", parkedReports, reports)
	}
}

func TestDeadLetter_AcceptingTheEventSkipsItAndLetsLaterEventsFlow(t *testing.T) {
	var rec deliveries
	var dead atomic.Value
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		if e.Version == 1 {
			return errors.New("poison")
		}
		return nil
	}}},
		withMaxAttempts[int](3),
		withDeadLetterHandler[int](func(
			_ context.Context,
			e asynxmd.Event[int],
			err error,
		) error {
			dead.Store(e.Version)
			return nil
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := enqueueTicket(ctx, d, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := enqueueTicket(ctx, d, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))

	if err := final(t, t1, "v1"); err != nil {
		t.Fatalf("dead-lettered event final = %v, want nil", err)
	}
	if err := final(t, t2, "v2"); err != nil {
		t.Fatalf("v2 final = %v", err)
	}
	if dead.Load() != int64(1) {
		t.Fatalf("dead letter saw %v, want v1", dead.Load())
	}
	if _, ok := acked.Load(int64(1)); !ok {
		t.Fatal("dead-lettered event stayed pending")
	}
	if len(halted(d)) != 0 {
		t.Fatalf("aggregate parked despite the dead-letter handler: %v", halted(d))
	}
}

func TestDeadLetter_RefusalParksTheAggregate(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return errors.New("poison")
	}}},
		withMaxAttempts[int](2),
		withDeadLetterHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) error {
			panic("dead-letter store down")
		}),
	)
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err := final(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

// Parking after a failed tombstone is Forget's job, not the dispatcher's, so
// the final ticket only reports the failure.
func TestTombstone_FinalReportsADurableSubscriberThatKeepsFailing(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: func(string) bool { return false },
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return errors.New("cannot forget")
		}}},
		withMaxAttempts[int](2))
	tomb := makeEvent("a", 2)
	tomb.EventName = asynxmd.ForgetEventName
	tk, _ := enqueueForget(context.Background(), d, tomb, nil)
	if err := final(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

func TestEnqueueForget_IsNeverDeadLettered(t *testing.T) {
	var dead atomic.Int32
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return errors.New("cannot forget")
	}}}, withMaxAttempts[int](2), withDeadLetterHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		dead.Add(1)
		return nil
	}))
	marker := makeEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ := enqueueForget(context.Background(), d, marker, func(context.Context) error { return nil })
	if err := final(t, tk, "marker"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked: a dead letter must not let Forget erase", err)
	}
	if dead.Load() != 0 {
		t.Fatalf("dead-letter handler called %d times for the marker", dead.Load())
	}
}

// A Forget that abandons the delivery while its last attempt runs leaves the
// aggregate unparked.
func TestGiveUp_AfterASupersedeDoesNotParkTheAggregateAgain(t *testing.T) {
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var dead atomic.Int32
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		inHandler <- struct{}{}
		<-release
		return errors.New("down")
	}}}, withMaxAttempts[int](1), withDeadLetterHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		dead.Add(1)
		return nil
	}))
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, inHandler, "last attempt running")

	d.Supersede("a")
	close(release)
	if err := final(t, tk, "superseded delivery"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v", err)
	}
	if got := halted(d); len(got) != 0 {
		t.Fatalf("halted = %v: the exhausted attempt re-parked an aggregate that was superseded", got)
	}
	if dead.Load() != 0 {
		t.Fatal("a superseded delivery was dead-lettered")
	}
}

func TestInterrupt_AfterASupersedeDoesNotParkTheAggregate(t *testing.T) {
	attempted := make(chan struct{}, 4)
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		attempted <- struct{}{}
		return errors.New("down")
	}}}, withRetryBackoff[int](time.Hour, time.Hour), withMaxAttempts[int](100))
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, attempted, "first attempt")
	d.Supersede("a")
	final(t, tk, "superseded")
	d.Interrupt()
	if got := halted(d); len(got) != 0 {
		t.Fatalf("halted = %v", got)
	}
}

func TestDeliveryErrorHandlerPanicDoesNotReleaseTheHold(t *testing.T) {
	var rec deliveries
	var failures atomic.Int32
	failures.Store(2)
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		if e.Version == 1 && failures.Add(-1) >= 0 {
			return errors.New("down")
		}
		return nil
	}}}, withDeliveryErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
		int,
	) {
		panic("boom")
	}))

	ctx := context.Background()
	ack := func(context.Context) error { return nil }
	enqueueTicket(ctx, d, makeEvent("a", 1), ack)
	last, _ := enqueueTicket(ctx, d, makeEvent("a", 2), ack)
	if err := final(t, last, "v2"); err != nil {
		t.Fatalf("v2 = %v", err)
	}
	want := []string{"a/1", "a/1", "a/1", "a/2"}
	if got := rec.list(); !slices.Equal(got, want) {
		t.Fatalf("delivered %v, want %v: a panicking handler released the hold", got, want)
	}
}

// Whoever is told that a delivery gave up may act on it at once, for instance by
// resuming; the aggregate must already be parked by then.
func TestGiveUp_TheAggregateIsParkedBeforeTheErrorHandlerIsTold(t *testing.T) {
	var d asynxmd.Dispatcher[int]
	parkedWhenTold := make(chan []string, 1)
	d = newDurable(t,
		[]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return errors.New("down")
		}}},
		withMaxAttempts[int](1),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parkedWhenTold <- halted(d)
			}
		}))

	enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if got := recv(t, parkedWhenTold, "the park to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked aggregates when the handler was told = %v, want [a]", got)
	}
}

func TestDurable_FailuresNameTheSubscriberAndDeadLetterGetsThemAll(t *testing.T) {
	billing, search := errors.New("billing down"), errors.New("search down")
	var mu sync.Mutex
	var reported []error
	deadLettered := make(chan error, 1)
	d := newDurable(t, []asynxmd.DurableSub[int]{
		{Name: "OrderUpdated#1", Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return billing
		}},
		{Name: "OrderUpdated#2", Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return search
		}},
	},
		withMaxAttempts[int](2),
		withDeliveryErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
			_ int,
		) {
			mu.Lock()
			reported = append(reported, err)
			mu.Unlock()
		}),
		withDeadLetterHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
		) error {
			deadLettered <- err
			return nil
		}),
	)

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := final(t, ticket, "dead-lettered delivery"); err != nil {
		t.Fatalf("delivery ended with %v", err)
	}

	got := recv(t, deadLettered, "the dead-letter handler")
	if !errors.Is(got, billing) || !errors.Is(got, search) {
		t.Fatalf("dead-letter handler learned of %q, want both failures", got)
	}
	var names []string
	for _, part := range unjoin(got) {
		names = appendSubscriber(names, part)
	}
	if !slices.Equal(names, []string{"OrderUpdated#1", "OrderUpdated#2"}) {
		t.Fatalf("subscribers named in the dead-letter error = %v", names)
	}

	mu.Lock()
	defer mu.Unlock()
	var sub *asynxmd.SubscriberError
	namesFirstSubscriber := len(reported) > 0 && errors.As(reported[0], &sub) && sub.Subscriber == "OrderUpdated#1"
	if !namesFirstSubscriber {
		t.Fatalf("delivery error handler got %v, want the first failure wrapped with its subscriber", reported)
	}
}

func TestDurable_ATimeoutNamesItsSubscriber(t *testing.T) {
	release := make(chan struct{})
	reported := make(chan error, 4)
	d := newDurable(t, []asynxmd.DurableSub[int]{{
		Name:    "slow#1",
		Match:   always,
		Timeout: time.Millisecond,
		Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			<-release
			return nil
		},
	}}, withDeliveryErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
		_ int,
	) {
		reported <- err
	}))

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	got := recv(t, reported, "the timeout report")
	close(release)
	<-ticket.Final

	var sub *asynxmd.SubscriberError
	namesSlow := errors.As(got, &sub) && sub.Subscriber == "slow#1"
	if !namesSlow || !errors.Is(got, asynxmd.ErrHandlerTimeout) {
		t.Fatalf("timeout reported as %q, want ErrHandlerTimeout from subscriber slow#1", got)
	}
}
