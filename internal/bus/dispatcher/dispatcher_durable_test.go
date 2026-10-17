package dispatcher

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


func TestEnqueue_DurableDeliveryStillAcksWhenBusRejectsEvent(t *testing.T) {
	var acked atomic.Bool
	var reported atomic.Int32
	d := New(
		asynxmd.Bus[int](&errBus{err: errors.New("bus closed")}),
		WithPublishErrorHandler[int](func(context.Context, asynxmd.Event[int], error) { reported.Add(1) }),
	)
	defer d.Close(context.Background())

	ticket, err := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error {
		acked.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A failing live subscriber must not skip the durable delivery: the next
	// event of the aggregate would otherwise overtake this one.
	if err := final(t, ticket, "final"); err != nil {
		t.Fatalf("delivery ended with %v", err)
	}
	if !acked.Load() {
		t.Error("ack did not run after the bus rejected the event")
	}
	if reported.Load() != 1 {
		t.Errorf("publish errors reported = %d, want 1", reported.Load())
	}
}

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

// recv waits for a value or fails the test; it is how these tests synchronise
// instead of sleeping.
func recv[V any](t *testing.T, ch <-chan V, what string) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// final waits for a ticket's delivery to end and returns how it ended.
func final(t *testing.T, tk Ticket, what string) error {
	t.Helper()
	recv(t, tk.Final, what)
	return tk.Err()
}

// failingFor fails every delivery of the named aggregate and version 1 only,
// recording the (aggregate, version) pairs it was asked to deliver.
type deliveries struct {
	mu   sync.Mutex
	seen []string
}

func (r *deliveries) add(e asynxmd.Event[int]) {
	r.mu.Lock()
	r.seen = append(r.seen, e.AggregateID+"/"+string(rune('0'+e.Version)))
	r.mu.Unlock()
}

func (r *deliveries) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

func ackInto(acked *sync.Map, e asynxmd.Event[int]) func(context.Context) error {
	return func(context.Context) error { acked.Store(e.Version, true); return nil }
}

func TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne(t *testing.T) {
	var rec deliveries
	attempted := make(chan struct{}, 64)
	d := New[int](&recordingBus{},
		WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
			rec.add(e)
			if e.Version == 1 {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}}}),
		WithRetryBackoff[int](time.Hour, time.Hour),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))
	recv(t, attempted, "first attempt of v1")

	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for v, got := range map[int]error{1: final(t, t1, "v1 final"), 2: final(t, t2, "v2 final")} {
		if got == nil {
			t.Errorf("v%d ended as delivered", v)
		}
	}
	for _, v := range []int64{1, 2} {
		if _, ok := acked.Load(v); ok {
			t.Errorf("v%d was acknowledged although v1 never succeeded", v)
		}
	}
	for _, s := range rec.list() {
		if s == "a/2" {
			t.Fatalf("v2 was delivered while v1 was still pending: %v", rec.list())
		}
	}
}

func TestInterrupt_ReleasesWaitersWithoutClosingTheDispatcher(t *testing.T) {
	attempted := make(chan struct{}, 8)
	d := New[int](&recordingBus{},
		WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
			attempted <- struct{}{}
			return errors.New("poison")
		}}}),
		WithRetryBackoff[int](time.Hour, time.Hour),
	)
	defer d.Close(context.Background())

	tk, err := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recv(t, attempted, "first attempt")

	d.Interrupt()
	if got := final(t, tk, "final"); !errors.Is(got, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("final = %v, want ErrDispatcherClosed", got)
	}
	// Still open: a non-durable event is delivered normally.
	done, err := d.Enqueue(context.Background(), makeEvent("b", 1), nil)
	if err != nil {
		t.Fatalf("Enqueue after Interrupt: %v", err)
	}
	recv(t, done, "live event after Interrupt")
}

func TestBusErrorDoesNotSkipTheDurableDelivery(t *testing.T) {
	var rec deliveries
	d := New[int](&errBus{err: errors.New("bus down")},
		WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
			rec.add(e)
			return nil
		}}}),
	)
	defer d.Close(context.Background())

	var acked sync.Map
	var last Ticket
	for v := int64(1); v <= 3; v++ {
		last, _ = d.EnqueueTicket(context.Background(), makeEvent("a", v), ackInto(&acked, makeEvent("a", v)))
	}
	if err := final(t, last, "final"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if got := rec.list(); len(got) != 3 || got[0] != "a/1" || got[2] != "a/3" {
		t.Fatalf("durable deliveries = %v, want a/1 a/2 a/3 in order", got)
	}
}

func TestExhaustedAttempts_ParkTheAggregateAndLeaveOthersAlone(t *testing.T) {
	var attempts atomic.Int32
	var rec deliveries
	var mu sync.Mutex
	var reports []error
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		rec.add(e)
		if e.AggregateID == "a" && e.Version == 1 {
			attempts.Add(1)
			return errors.New("poison")
		}
		return nil
	}}},
		WithMaxAttempts[int](4),
		WithDeliveryErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error, _ int) {
			mu.Lock()
			reports = append(reports, err)
			mu.Unlock()
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))

	if err := final(t, t1, "v1 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := final(t, t2, "v2 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 final = %v, want ErrDeliveryParked", err)
	}
	if n := attempts.Load(); n != 4 {
		t.Fatalf("attempts = %d, want exactly the cap (4)", n)
	}
	if got := d.Halted(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Halted = %v, want [a]", got)
	}
	for _, s := range rec.list() {
		if s == "a/2" {
			t.Fatal("v2 delivered although v1 was parked")
		}
	}

	// A later event of the parked aggregate is not queued either.
	t3, _ := d.EnqueueTicket(ctx, makeEvent("a", 3), ackInto(&acked, makeEvent("a", 3)))
	if err := final(t, t3, "v3 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v3 final = %v, want ErrDeliveryParked", err)
	}

	// Another aggregate flows.
	tb, _ := d.EnqueueTicket(ctx, makeEvent("b", 1), ackInto(&acked, makeEvent("b", 1)))
	if err := final(t, tb, "b final"); err != nil {
		t.Fatalf("aggregate b held back: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	parkedReports := 0
	for _, e := range reports {
		if errors.Is(e, asynxmd.ErrDeliveryParked) {
			parkedReports++
		}
	}
	if parkedReports != 1 {
		t.Fatalf("parked reports = %d, want 1 (%v)", parkedReports, reports)
	}
}

func TestResume_LiftsAParkAndDeliversAgain(t *testing.T) {
	var healthy atomic.Bool
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("down")
	}}}, WithMaxAttempts[int](2))

	var acked sync.Map
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	final(t, tk, "parked")

	healthy.Store(true)
	d.Resume("a")
	tk, err := d.EnqueueRedelivery(context.Background(), makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := final(t, tk, "redelivery"); err != nil {
		t.Fatalf("redelivery = %v", err)
	}
	if _, ok := acked.Load(int64(1)); !ok {
		t.Fatal("redelivered event was not acknowledged")
	}
	if len(d.Halted()) != 0 {
		t.Fatalf("still halted: %v", d.Halted())
	}
}

func TestDeadLetter_AcceptingTheEventSkipsItAndLetsLaterEventsFlow(t *testing.T) {
	var rec deliveries
	var dead atomic.Value
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		rec.add(e)
		if e.Version == 1 {
			return errors.New("poison")
		}
		return nil
	}}},
		WithMaxAttempts[int](3),
		WithDeadLetterHandler[int](func(_ context.Context, e asynxmd.Event[int], err error) error {
			dead.Store(e.Version)
			return nil
		}),
	)

	var acked sync.Map
	ctx := context.Background()
	t1, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	t2, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))

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
	if len(d.Halted()) != 0 {
		t.Fatalf("aggregate parked despite the dead-letter handler: %v", d.Halted())
	}
}

func TestDeadLetter_RefusalParksTheAggregate(t *testing.T) {
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		return errors.New("poison")
	}}},
		WithMaxAttempts[int](2),
		WithDeadLetterHandler[int](func(context.Context, asynxmd.Event[int], error) error {
			panic("dead-letter store down")
		}),
	)
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	if err := final(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

func TestSupersede_LetsTheTombstoneThroughAFailingDelivery(t *testing.T) {
	var seen deliveries
	attempted := make(chan struct{}, 8)
	d := newDurable(t, []DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
		Handler: func(_ context.Context, e asynxmd.Event[int]) error {
			seen.add(e)
			if e.EventName == "test.event" {
				attempted <- struct{}{}
				return errors.New("poison")
			}
			return nil
		}}}, WithRetryBackoff[int](time.Hour, time.Hour))

	var acked sync.Map
	ctx := context.Background()
	d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	d.EnqueueTicket(ctx, makeEvent("a", 2), ackInto(&acked, makeEvent("a", 2)))
	recv(t, attempted, "poison attempt")

	d.Supersede("a")
	tomb := makeEvent("a", 3)
	tomb.EventName = asynxmd.ForgetEventName
	tk, err := d.EnqueueForget(ctx, tomb, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := final(t, tk, "tombstone"); err != nil {
		t.Fatalf("tombstone final = %v", err)
	}
	if len(d.Halted()) != 0 {
		t.Fatalf("Supersede left the aggregate parked: %v", d.Halted())
	}
	// The tombstone reached a subscriber whose pattern does not match it.
	found := false
	for _, s := range seen.list() {
		if s == "a/3" {
			found = true
		}
	}
	if !found {
		t.Fatalf("durable subscriber never saw the tombstone: %v", seen.list())
	}
}

func TestTombstone_FinalReportsADurableSubscriberThatKeepsFailing(t *testing.T) {
	d := newDurable(t, []DurableSub[int]{{Match: func(string) bool { return false },
		Handler: func(context.Context, asynxmd.Event[int]) error { return errors.New("cannot forget") }}},
		WithMaxAttempts[int](2))
	tomb := makeEvent("a", 2)
	tomb.EventName = asynxmd.ForgetEventName
	tk, _ := d.EnqueueForget(context.Background(), tomb, nil)
	if err := final(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	// Parking after a failed tombstone is Forget's job, not the dispatcher's.
}

func TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents(t *testing.T) {
	d := newDurable(t, nil)
	d.Park("ghost")
	d.Park("ghost") // idempotent
	d.Resume("never-parked")

	var acked sync.Map
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := final(t, tk, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if _, ok := acked.Load(int64(1)); ok {
		t.Fatal("event of a parked aggregate was acknowledged")
	}

	d.Resume("ghost")
	tk, _ = d.EnqueueTicket(context.Background(), makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := final(t, tk, "final after resume"); err != nil {
		t.Fatalf("final after resume = %v", err)
	}
}

func TestPark_KeepsLiveOnlyJobsQueuedAndDropsDurableOnes(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		if e.Version == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}}})
	ctx := context.Background()
	d.EnqueueTicket(ctx, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, started, "v1 running")
	durable, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), func(context.Context) error { return nil })
	live, _ := d.EnqueueTicket(ctx, makeEvent("a", 3), nil)

	d.Park("a")
	if err := final(t, durable, "durable ticket"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("durable final = %v, want ErrDeliveryParked", err)
	}
	close(gate)
	if err := final(t, live, "live ticket"); err != nil {
		t.Fatalf("live-only job final = %v, want delivered", err)
	}
}

func TestOptions_NonPositiveBoundsAreIgnored(t *testing.T) {
	d := New[int](nil, WithMaxAttempts[int](0))
	if d.maxAttempts != defaultMaxAttempts {
		t.Fatalf("maxAttempts=%d, want the default", d.maxAttempts)
	}
}

func TestEnqueueRedelivery_GoesToTheDurableSubscribersOnly(t *testing.T) {
	bus := &recordingBus{}
	var seen deliveries
	d := New[int](bus, WithDurableSubs([]DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
		Handler: func(_ context.Context, e asynxmd.Event[int]) error {
			seen.add(e)
			return nil
		}}}))
	t.Cleanup(func() { d.Close(context.Background()) })
	var acked atomic.Int32
	ack := func(context.Context) error { acked.Add(1); return nil }
	ctx := context.Background()

	tk, _ := d.EnqueueRedelivery(ctx, makeEvent("a", 1), ack)
	final(t, tk, "event")
	marker := makeEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ = d.EnqueueRedelivery(ctx, marker, ack)
	final(t, tk, "marker")

	if got := len(bus.snapshot()); got != 0 {
		t.Fatalf("live subscribers got %d events from redeliveries", got)
	}
	if got := seen.list(); !slices.Equal(got, []string{"a/1", "a/2"}) {
		t.Fatalf("durable subscriber saw %v, want the event and the marker (whatever its pattern)", got)
	}
	if acked.Load() != 2 {
		t.Fatalf("acked %d, want 2", acked.Load())
	}
}

func TestEnqueueForget_IsNeverDeadLettered(t *testing.T) {
	var dead atomic.Int32
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		return errors.New("cannot forget")
	}}}, WithMaxAttempts[int](2), WithDeadLetterHandler[int](func(context.Context, asynxmd.Event[int], error) error {
		dead.Add(1)
		return nil
	}))
	marker := makeEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ := d.EnqueueForget(context.Background(), marker, func(context.Context) error { return nil })
	if err := final(t, tk, "marker"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked: a dead letter must not let Forget erase", err)
	}
	if dead.Load() != 0 {
		t.Fatalf("dead-letter handler called %d times for the marker", dead.Load())
	}
}

func TestGiveUp_AfterASupersedeDoesNotParkTheAggregateAgain(t *testing.T) {
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var dead atomic.Int32
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		inHandler <- struct{}{}
		<-release
		return errors.New("down")
	}}}, WithMaxAttempts[int](1), WithDeadLetterHandler[int](func(context.Context, asynxmd.Event[int], error) error {
		dead.Add(1)
		return nil
	}))
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, inHandler, "last attempt running")

	d.Supersede("a") // a Forget abandons the delivery while its last attempt runs
	close(release)
	if err := final(t, tk, "superseded delivery"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v", err)
	}
	if got := d.Halted(); len(got) != 0 {
		t.Fatalf("halted = %v: the exhausted attempt re-parked an aggregate that was superseded", got)
	}
	if dead.Load() != 0 {
		t.Fatal("a superseded delivery was dead-lettered")
	}
}

func TestInterrupt_AfterASupersedeDoesNotParkTheAggregate(t *testing.T) {
	attempted := make(chan struct{}, 4)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		attempted <- struct{}{}
		return errors.New("down")
	}}}, WithRetryBackoff[int](time.Hour, time.Hour), WithMaxAttempts[int](100))
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, attempted, "first attempt")
	d.Supersede("a")
	final(t, tk, "superseded")
	d.Interrupt()
	if got := d.Halted(); len(got) != 0 {
		t.Fatalf("halted = %v", got)
	}
}

func TestBusPanicDoesNotSkipTheDurableDelivery(t *testing.T) {
	var acked atomic.Bool
	var reported atomic.Int32
	d := New[int](&panicBus{}, WithDurableSubs([]DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error { return nil }}}),
		WithPublishErrorHandler[int](func(context.Context, asynxmd.Event[int], error) { reported.Add(1) }))
	t.Cleanup(func() { d.Close(context.Background()) })
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	if err := final(t, tk, "delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked=%v reported=%d, want the panic reported and the delivery completed", acked.Load(), reported.Load())
	}
}

type panicBus struct{ recordingBus }

func (*panicBus) PublishSync(context.Context, asynxmd.Event[int]) error { panic("bus exploded") }

func TestReportError_ReachesThePublishErrorHandler(t *testing.T) {
	var got atomic.Pointer[error]
	d := New[int](nil, WithPublishErrorHandler[int](func(_ context.Context, e asynxmd.Event[int], err error) { got.Store(&err) }))
	want := errors.New("bad record")
	d.ReportError(context.Background(), makeEvent("a", 1), want)
	if p := got.Load(); p == nil || !errors.Is(*p, want) {
		t.Fatalf("handler got %v", p)
	}
}

func TestClose_CalledAgainStillWaitsForTheWorkers(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error {
		close(started)
		<-release
		return nil
	}}})
	d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, started, "handler running")

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.Close(short); err == nil {
		t.Fatal("first Close succeeded with a handler running")
	}
	short2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if err := d.Close(short2); err == nil {
		t.Fatal("second Close claimed success while the handler still runs")
	}
	close(release)
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("Close after the handler finished: %v", err)
	}
}

func TestTicket_UnrunSaysWhyNoHandlerRan(t *testing.T) {
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(context.Context, asynxmd.Event[int]) error { return nil }}})
	ctx := context.Background()
	ack := func(context.Context) error { return nil }

	ran, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ack)
	final(t, ran, "ran")
	if err := ran.Unrun(); err != nil {
		t.Fatalf("Unrun = %v for a delivered event", err)
	}

	d.Park("a")
	parked, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ack)
	recv(t, parked.Settled, "settled")
	if err := parked.Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
}

func TestPark_QueuedDurableJobsReportThemselvesUnrun(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		if e.Version == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}}})
	ctx := context.Background()
	ack := func(context.Context) error { return nil }
	d.EnqueueTicket(ctx, makeEvent("a", 1), ack)
	recv(t, started, "v1 running")
	queued, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ack)

	d.Park("a")
	recv(t, queued.Settled, "settled")
	if err := queued.Unrun(); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun = %v, want ErrDeliveryParked", err)
	}
	close(gate)
}

func TestDeliveryErrorHandlerPanicDoesNotReleaseTheHold(t *testing.T) {
	var rec deliveries
	var failures atomic.Int32
	failures.Store(2)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		rec.add(e)
		if e.Version == 1 && failures.Add(-1) >= 0 {
			return errors.New("down")
		}
		return nil
	}}}, WithDeliveryErrorHandler[int](func(context.Context, asynxmd.Event[int], error, int) { panic("boom") }))

	ctx := context.Background()
	ack := func(context.Context) error { return nil }
	d.EnqueueTicket(ctx, makeEvent("a", 1), ack)
	last, _ := d.EnqueueTicket(ctx, makeEvent("a", 2), ack)
	if err := final(t, last, "v2"); err != nil {
		t.Fatalf("v2 = %v", err)
	}
	want := []string{"a/1", "a/1", "a/1", "a/2"}
	if got := rec.list(); !slices.Equal(got, want) {
		t.Fatalf("delivered %v, want %v: a panicking handler released the hold", got, want)
	}
}

func TestPublishErrorHandlerPanicDoesNotCrashTheWorker(t *testing.T) {
	d := New[int](&errBus{err: errors.New("down")}, WithPublishErrorHandler[int](func(context.Context, asynxmd.Event[int], error) { panic("boom") }))
	t.Cleanup(func() { d.Close(context.Background()) })
	done, err := d.Enqueue(context.Background(), makeEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	recv(t, done, "settled")
	done, _ = d.Enqueue(context.Background(), makeEvent("a", 2), nil)
	recv(t, done, "the worker survives")
}

func TestHandlerContext_MarksItsAggregateAndEndsWithTheDelivery(t *testing.T) {
	var got context.Context
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(ctx context.Context, e asynxmd.Event[int]) error {
		got = ctx
		if !InHandler(ctx, "a") || InHandler(ctx, "b") {
			t.Errorf("InHandler(a)=%v InHandler(b)=%v", InHandler(ctx, "a"), InHandler(ctx, "b"))
		}
		return nil
	}}})
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	final(t, tk, "final")
	if got.Err() == nil {
		t.Fatal("handler context still live after the delivery ended")
	}
	if InHandler(context.Background(), "a") {
		t.Fatal("InHandler true for a plain context")
	}
}

func TestClose_GivingUpCancelsRunningHandlers(t *testing.T) {
	started := make(chan context.Context, 1)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(ctx context.Context, _ asynxmd.Event[int]) error {
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	}}}, WithMaxAttempts[int](1))
	d.EnqueueTicket(context.Background(), makeEvent("a", 1), func(context.Context) error { return nil })
	hctx := recv(t, started, "handler running")

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.Close(short); err == nil {
		t.Fatal("Close succeeded with a handler still running")
	}
	recv(t, hctx.Done(), "handler context cancelled")
}
