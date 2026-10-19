package dispatchertest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

const shortWait = 20 * time.Millisecond

func lifecycleScenarios() map[string]scenario {
	return map[string]scenario{
		"TestClose_DrainsAllEvents":                                         closeDrainsAllEvents,
		"TestClose_Idempotent":                                              closeIdempotent,
		"TestDispatch_AfterClose":                                           enqueueAfterClose,
		"TestClose_ContextTimeout":                                          closeContextTimeout,
		"TestClose_DrainsJobsQueuedBehindASlowHandler":                      closeDrainsQueuedJobs,
		"TestClose_CalledAgainStillWaitsForTheWorkers":                      closeCalledAgainStillWaits,
		"TestClose_GivingUpCancelsRunningHandlers":                          closeGivingUpCancelsHandlers,
		"TestDurable_CloseInterruptsRetriesAndLeavesTheDeliveryUnacked":     closeInterruptsRetries,
		"TestShutdownDuringRetry_NeverDeliversALaterEventPastTheFailingOne": shutdownDuringRetry,
		"TestInterrupt_ReleasesWaitersWithoutClosingTheDispatcher":          interruptReleasesWaiters,
		"TestInterrupt_ParksTheAggregateOfTheRetryingDelivery":              interruptParks,
		"TestInterrupted_ClosesWhenTheDispatcherIsInterrupted":              interruptedOnInterrupt,
		"TestInterrupted_ClosesWhenTheDispatcherIsClosed":                   interruptedOnClose,
		"TestWaitIdle_DoesNotRefuseEnqueuesWhileItWaits":                    waitIdleRefusesNothing,
		"TestWaitIdle_ReturnsAtOnceWhenNothingIsQueued":                     waitIdleWhenNothingIsQueued,
		"TestHandlerContext_MarksItsAggregateAndEndsWithTheDelivery":        handlerContextMarksItsAggregate,
	}
}

func closeDrainsAllEvents(
	t testing.TB,
	h harness,
) {
	b := &bus{}
	d := h.build(t, config(b))
	for v := int64(1); v <= 20; v++ {
		mustEnqueue(t, d, live(event("agg-1", v)))
	}
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := versionsOf(b.published()); !slices.Equal(got, sequence(20)) {
		t.Fatalf("published after Close = %v, want 1..20 in order", got)
	}
}

func closeIdempotent(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if err := h.close(d); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := h.close(d); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func enqueueAfterClose(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, liveErr := d.Enqueue(context.Background(), live(event("agg-1", 1)))
	_, durableErr := d.Enqueue(context.Background(), durable(event("agg-1", 2), acknowledge))
	if !errors.Is(liveErr, models.ErrDispatcherClosed) || !errors.Is(durableErr, models.ErrDispatcherClosed) {
		t.Fatalf("Enqueue after Close = %v, %v; want ErrDispatcherClosed", liveErr, durableErr)
	}
}

func closeContextTimeout(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	d := h.build(t, config(gatedBus(entered, release)))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, live(event("agg-1", 1)))
	h.wait(t, entered, "the handler to start")
	ctx, cancel := context.WithTimeout(context.Background(), shortWait)
	defer cancel()
	if err := d.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want DeadlineExceeded", err)
	}
}

func closeDrainsQueuedJobs(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	running := make(chan struct{}, 3)
	var handled atomic.Int32
	b := &bus{fn: func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		sendIf(e.AggregateID == "a", running, struct{}{})
		holdIf(e.AggregateID == "a", release)
		countIf(e.AggregateID == "a", &handled)
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })
	for v := int64(1); v <= 3; v++ {
		mustEnqueue(t, d, live(event("a", v)))
	}
	h.wait(t, running, "the first job to be inside its handler")

	var handledAtClose atomic.Int32
	closed := make(chan error, 1)
	go func() {
		err := h.close(d)
		handledAtClose.Store(handled.Load())
		closed <- err
	}()
	h.waitFor(t, "Close to stop accepting requests", func() bool { return refusesProbe(d) })
	closeOnce(release)
	if err := receive(t, h.patience, closed, "Close to return"); err != nil || handledAtClose.Load() != 3 {
		t.Fatalf("Close = %v with %d jobs handled when it returned; want nil and all 3 drained first", err, handledAtClose.Load())
	}
}

func refusesProbe(
	d models.Dispatcher[Aggregate],
) bool {
	_, err := d.Enqueue(context.Background(), live(event("probe", 1)))
	return errors.Is(err, models.ErrDispatcherClosed)
}

func closeCalledAgainStillWaits(
	t testing.TB,
	h harness,
) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	d := h.build(t, config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		sendIf(true, started, struct{}{})
		<-release
		return nil
	})))
	t.Cleanup(func() { closeOnce(release) })

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, started, "the handler to run")
	if err := closeWithin(d, shortWait); err == nil {
		t.Fatal("the first Close succeeded with a handler running")
	}
	if err := closeWithin(d, shortWait); err == nil {
		t.Fatal("the second Close claimed success while the handler still runs")
	}
	closeOnce(release)
	if err := h.close(d); err != nil {
		t.Fatalf("Close after the handler finished: %v", err)
	}
}

func closeWithin(
	d models.Dispatcher[Aggregate],
	wait time.Duration,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	return d.Close(ctx)
}

func closeGivingUpCancelsHandlers(
	t testing.TB,
	h harness,
) {
	started := make(chan context.Context, 1)
	cfg := config(&bus{}, subscriber(func(
		ctx context.Context,
		_ models.Event[Aggregate],
	) error {
		sendIf(true, started, ctx)
		<-ctx.Done()
		return ctx.Err()
	}))
	cfg.MaxAttempts = 1
	d := h.build(t, cfg)

	mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	handlerCtx := receive(t, h.patience, started, "the handler to run")
	if err := closeWithin(d, shortWait); err == nil {
		t.Fatal("Close succeeded with a handler still running")
	}
	h.wait(t, handlerCtx.Done(), "the handler context to be cancelled")
}

func closeInterruptsRetries(
	t testing.TB,
	h harness,
) {
	var acked atomic.Bool
	cfg := config(&bus{}, subscriber(fail))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)
	ack := func(
		context.Context,
	) error {
		acked.Store(true)
		return nil
	}

	tk := mustEnqueue(t, d, durable(event("a", 1), ack))
	h.wait(t, tk.Settled, "the first attempt")
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if acked.Load() {
		t.Fatal("a failing delivery was acknowledged on Close")
	}
}

func shutdownDuringRetry(
	t testing.TB,
	h harness,
) {
	var rec journal
	var acked sync.Map
	attempted := make(chan struct{}, 64)
	cfg := config(&bus{}, subscriber(func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		rec.add(e)
		sendIf(e.Version == 1, attempted, struct{}{})
		return errorIf(e.Version == 1, errPoison)
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	t1 := mustEnqueue(t, d, durable(event("a", 1), ackInto(&acked, 1)))
	t2 := mustEnqueue(t, d, durable(event("a", 2), ackInto(&acked, 2)))
	h.wait(t, attempted, "the first attempt of v1")
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if h.final(t, t1, "v1") == nil || h.final(t, t2, "v2") == nil {
		t.Fatal("a delivery behind a failing one ended as delivered")
	}
	_, v1 := acked.Load(int64(1))
	_, v2 := acked.Load(int64(2))
	if v1 || v2 || slices.Contains(rec.list(), "a/2") {
		t.Fatalf("acked v1 %v, v2 %v, delivered %v; want nothing past the failing v1", v1, v2, rec.list())
	}
}

func interruptReleasesWaiters(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errPoison
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Interrupt()
	if got := h.final(t, tk, "the interrupted delivery"); !errors.Is(got, models.ErrDispatcherClosed) {
		t.Fatalf("final = %v, want ErrDispatcherClosed", got)
	}
	next, err := d.Enqueue(context.Background(), live(event("b", 1)))
	if err != nil {
		t.Fatalf("Enqueue after Interrupt: %v", err)
	}
	h.wait(t, next.Final, "a live event after Interrupt")
}

func interruptParks(
	t testing.TB,
	h harness,
) {
	attempted := make(chan struct{}, 8)
	cfg := config(&bus{}, subscriber(func(
		context.Context,
		models.Event[Aggregate],
	) error {
		sendIf(true, attempted, struct{}{})
		return errPoison
	}))
	cfg.RetryInitial, cfg.RetryMax = time.Hour, time.Hour
	d := h.build(t, cfg)

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, attempted, "the first attempt")
	d.Interrupt()
	h.wait(t, tk.Final, "the interrupted delivery")
	if !slices.Equal(parked(d), []string{"a"}) {
		t.Fatalf("parked = %v, want the interrupted aggregate parked", parked(d))
	}
	later := mustEnqueue(t, d, durable(event("a", 2), acknowledge))
	if err := h.final(t, later, "a durable event after Interrupt"); !errors.Is(err, models.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

func interruptedOnInterrupt(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if isClosed(d.Interrupted()) {
		t.Fatal("Interrupted closed before Interrupt")
	}
	d.Interrupt()
	if !isClosed(d.Interrupted()) {
		t.Fatal("Interrupted did not close after Interrupt")
	}
}

func interruptedOnClose(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	if err := h.close(d); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !isClosed(d.Interrupted()) {
		t.Fatal("Interrupted did not close after Close")
	}
}

func waitIdleRefusesNothing(
	t testing.TB,
	h harness,
) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	b := &bus{fn: func(
		_ context.Context,
		e models.Event[Aggregate],
	) error {
		sendIf(e.AggregateID == "a", entered, struct{}{})
		holdIf(e.AggregateID == "a", release)
		return nil
	}}
	d := h.build(t, config(b))
	t.Cleanup(func() { closeOnce(release) })

	first := mustEnqueue(t, d, live(event("a", 1)))
	h.wait(t, entered, "the handler to start")
	started := make(chan struct{})
	idle := make(chan struct{})
	go waitIdleAfter(d, started, idle)
	h.wait(t, started, "WaitIdle to be called")
	var refused journal
	enqueued := make(chan struct{})
	enqueueAll(d, "b", 50, &refused, enqueued)
	if got := refused.list(); len(got) != 0 || isClosed(idle) {
		t.Fatalf("refused %v, WaitIdle returned %v; want nothing refused while WaitIdle waits on a running event", got, isClosed(idle))
	}
	closeOnce(release)
	h.wait(t, idle, "WaitIdle to return")
	h.wait(t, first.Final, "the first event")
}

func waitIdleWhenNothingIsQueued(
	t testing.TB,
	h harness,
) {
	d := h.build(t, config(&bus{}))
	h.idle(t, d)
}

func handlerContextMarksItsAggregate(
	t testing.TB,
	h harness,
) {
	var d, other models.Dispatcher[Aggregate]
	var inOwn, inOther, inOtherAggregate atomic.Bool
	contexts := make(chan context.Context, 1)
	d = h.build(t, config(&bus{}, subscriber(func(
		ctx context.Context,
		_ models.Event[Aggregate],
	) error {
		inOwn.Store(d.InHandler(ctx, "a"))
		inOtherAggregate.Store(d.InHandler(ctx, "b"))
		inOther.Store(other.InHandler(ctx, "a"))
		sendIf(true, contexts, ctx)
		return nil
	})))
	other = h.build(t, config(&bus{}))

	tk := mustEnqueue(t, d, durable(event("a", 1), acknowledge))
	h.wait(t, tk.Final, "the delivery")
	handlerCtx := receive(t, h.patience, contexts, "the handler context")
	if !inOwn.Load() || inOtherAggregate.Load() || inOther.Load() {
		t.Fatalf("InHandler own=%v otherAggregate=%v otherInstance=%v; want true, false, false", inOwn.Load(), inOtherAggregate.Load(), inOther.Load())
	}
	if handlerCtx.Err() == nil || d.InHandler(context.Background(), "a") {
		t.Fatal("the handler context outlived the delivery, or a plain context counts as a handler's")
	}
}
