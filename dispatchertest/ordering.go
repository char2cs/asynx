package dispatchertest

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func orderingScenarios() map[string]scenario {
	return map[string]scenario{
		"TestDispatch_OrderingGuarantee":                             dispatchOrderingGuarantee,
		"TestDispatch_CrossAggregateIndependence":                    dispatchCrossAggregateIndependence,
		"TestDispatch_SyncBlocking":                                  dispatchSyncBlocking,
		"TestDispatch_AsyncNonBlocking":                              dispatchAsyncNonBlocking,
		"TestDispatch_MultipleAggregatesOrdered":                     dispatchMultipleAggregatesOrdered,
		"TestDispatch_ConcurrentSameAggregate":                       dispatchConcurrentSameAggregate,
		"TestEnqueue_NeverBlocksBehindASlowHandler":                  enqueueNeverBlocksBehindASlowHandler,
		"TestDispatch_NilBusNoPanic":                                 dispatchNilBusNoPanic,
		"TestDispatch_PublishErrorHandler":                           dispatchPublishErrorHandler,
		"TestDurable_TimeoutIsReportedButTheHandlerIsStillWaitedFor": durableTimeoutIsStillWaitedFor,
	}
}

func dispatchOrderingGuarantee(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	d := h.build(t, config(b))
	for v := int64(1); v <= 10; v++ {
		mustEnqueue(t, d, live(event("agg-1", v)))
	}
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := versionsOf(b.published()); !slices.Equal(got, sequence(10)) {
		t.Fatalf("published versions = %v, want 1..10 in order", got)
	}
}

func dispatchCrossAggregateIndependence(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	b := &bus{fn: func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		holdIf(e.AggregateID == "agg-slow", release)
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, live(event("agg-slow", 1)))
	fast := mustEnqueue(t, d, live(event("agg-fast", 1)))
	h.wait(t, fast.Final, "agg-fast to be delivered while agg-slow is held")
}

func dispatchSyncBlocking(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		entered <- struct{}{}
		<-release
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	tk := mustEnqueue(t, d, live(event("agg-1", 1)))
	h.wait(t, entered, "the handler to start")
	if isClosed(tk.Settled) {
		t.Fatal("Settled closed while the handler was still running")
	}
	closeOnce(release)
	h.wait(t, tk.Settled, "Settled after the handler returned")
}

func dispatchAsyncNonBlocking(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		<-release
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	h.enqueueBounded(t, d, live(event("agg-1", 1)))
	h.enqueueBounded(t, d, live(event("agg-1", 2)))
}

func dispatchMultipleAggregatesOrdered(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	d := h.build(t, config(b))
	for v := int64(1); v <= 5; v++ {
		enqueueRound(t, d, v, "a", "b", "c")
	}
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	want := map[string][]int64{"a": sequence(5), "b": sequence(5), "c": sequence(5)}
	if got := byAggregate(b.published()); !maps.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("published per aggregate = %v, want 1..5 each in order", got)
	}
}

func dispatchConcurrentSameAggregate(
	t testing.TB,
	h harness,
) {
	const n = 100
	b := &bus{}
	d := h.build(t, config(b))
	var refused journal
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range int64(n) {
		go enqueueOne(&wg, d, live(event("agg-1", i+1)), &refused)
	}
	wg.Wait()
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	versions := versionsOf(b.published())
	if len(versions) != n || distinct(versions) != n || len(refused.list()) != 0 {
		t.Fatalf("published %d events (%d distinct), refused %v; want %d once each", len(versions), distinct(versions), refused.list(), n)
	}
}

func enqueueNeverBlocksBehindASlowHandler(
	t testing.TB,
	h harness,
) {
	const n = 200
	release := make(chan struct{})
	t.Cleanup(func() { closeOnce(release) })
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		<-release
		return nil
	}}
	d := h.build(t, config(b))
	var refused journal
	enqueued := make(chan struct{})
	go enqueueAll(d, "a", n, &refused, enqueued)

	h.wait(t, enqueued, "Enqueue to return while a handler is blocked")
	closeOnce(release)
	h.idle(t, d)
	if got := versionsOf(b.published()); !slices.Equal(got, sequence(n)) || len(refused.list()) != 0 {
		t.Fatalf("published %v (refused %v), want 1..%d in order", got, refused.list(), n)
	}
}

func dispatchNilBusNoPanic(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(nil))
	tk := mustEnqueue(t, d, live(event("agg-1", 1)))
	if err := h.final(t, tk, "the live event"); err != nil {
		t.Fatalf("final = %v", err)
	}
}

func dispatchPublishErrorHandler(
	t testing.TB,
	h harness,
) {
	publishErr := errors.New("publish failed")
	b := &bus{fn: func(
		context.Context,
		models.Event[Aggregate],
	) error {
		return publishErr
	}}
	reported := make(chan error, 4)
	events := make(chan models.Event[Aggregate], 4)
	cfg := config(b)
	cfg.OnPublishError = func(
		_ context.Context,
		e models.Event[Aggregate],
		err error,
	) {
		sendIf(true, reported, err)
		sendIf(true, events, e)
	}
	d := h.build(t, cfg)

	mustEnqueue(t, d, live(event("agg-1", 1)))
	got := receive(t, h.patience, reported, "the publish error report")
	e := receive(t, h.patience, events, "the reported event")
	if !errors.Is(got, publishErr) || e.AggregateID != "agg-1" {
		t.Fatalf("reported %v for %q, want the publish error for agg-1", got, e.AggregateID)
	}
}

func durableTimeoutIsStillWaitedFor(
	t testing.TB,
	h harness,
) {
	var running, overlapped atomic.Bool
	var timeouts, acks atomic.Int32
	timedOut := make(chan struct{}, 8)
	sub := subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		flagIf(!running.CompareAndSwap(false, true), &overlapped)
		<-timedOut
		running.Store(false)
		return nil
	})
	sub.Timeout = 10 * time.Millisecond
	cfg := config(&bus{}, sub)
	cfg.OnDeliveryError = func(
		_ context.Context,
		_ models.Event[Aggregate],
		err error,
		_ int,
	) {
		countIf(errors.Is(err, models.ErrHandlerTimeout), &timeouts)
		sendIf(errors.Is(err, models.ErrHandlerTimeout), timedOut, struct{}{})
	}
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acks.Add(1)
		return nil
	}

	mustEnqueue(t, d, durable(event("a", 1), ack))
	mustEnqueue(t, d, durable(event("a", 2), ack))
	h.waitFor(t, "both deliveries to be acked", func() bool { return acks.Load() == 2 })
	if overlapped.Load() || timeouts.Load() != 2 {
		t.Fatalf("overlapped = %v, timeouts = %d; want no overlap and 2 timeouts", overlapped.Load(), timeouts.Load())
	}
}
