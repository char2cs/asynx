package workers_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

func TestDispatch_OrderingGuarantee(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)

	ctx := context.Background()
	for i := int64(1); i <= 10; i++ {
		dispatchOrFail(t, d, i)
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	calls := bus.snapshot()
	if len(calls) != 10 {
		t.Fatalf("expected 10 calls, got %d", len(calls))
	}
	for i, e := range calls {
		checkCallVersion(t, i, e)
	}
}

// agg-fast, dispatched second, finishes before agg-slow, which has a 50ms
// delay.
func TestDispatch_CrossAggregateIndependence(t *testing.T) {
	var (
		mu       sync.Mutex
		finished []string
	)

	bus := &callbackBus{fn: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		if e.AggregateID == "agg-slow" {
			time.Sleep(50 * time.Millisecond)
		}
		mu.Lock()
		finished = append(finished, e.AggregateID)
		mu.Unlock()
		return nil
	}}

	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-slow", 1), false)
	_ = dispatch(ctx, d, makeEvent("agg-fast", 1), false)

	_ = d.Close(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(finished) != 2 {
		t.Fatalf("expected 2 finished, got %d", len(finished))
	}
	if finished[0] != "agg-fast" {
		t.Errorf("expected agg-fast to finish first, got %v", finished)
	}
}

func TestDispatch_SyncBlocking(t *testing.T) {
	delay := 30 * time.Millisecond
	bus := &recordingBus{delay: delay}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	start := time.Now()
	_ = dispatch(ctx, d, makeEvent("agg-1", 1), true)
	elapsed := time.Since(start)

	if elapsed < delay {
		t.Errorf("expected blocking for at least %v, returned in %v", delay, elapsed)
	}
	_ = d.Close(ctx)
}

func TestDispatch_AsyncNonBlocking(t *testing.T) {
	bus := &recordingBus{delay: 100 * time.Millisecond}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	start := time.Now()
	_ = dispatch(ctx, d, makeEvent("agg-1", 1), false)
	elapsed := time.Since(start)

	if elapsed > 20*time.Millisecond {
		t.Errorf("expected non-blocking return, took %v", elapsed)
	}
	_ = d.Close(ctx)
}

func TestDispatch_NilBusNoPanic(t *testing.T) {
	d := newTestDispatcher[int](nil)
	ctx := context.Background()

	if err := dispatch(ctx, d, makeEvent("agg-1", 1), true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = d.Close(ctx)
}

func TestDispatch_PublishErrorHandler(t *testing.T) {
	publishErr := errors.New("publish failed")
	bus := &errBus{err: publishErr}

	var (
		mu            sync.Mutex
		gotEvent      asynxmd.Event[int]
		gotErr        error
		handlerCalled bool
	)

	handler := func(
		_ context.Context,
		e asynxmd.Event[int],
		err error,
	) {
		mu.Lock()
		gotEvent = e
		gotErr = err
		handlerCalled = true
		mu.Unlock()
	}

	d := newTestDispatcher[int](bus, withPublishErrorHandler[int](handler))
	ctx := context.Background()

	ev := makeEvent("agg-1", 1)
	_ = dispatch(ctx, d, ev, true)
	_ = d.Close(ctx)

	mu.Lock()
	defer mu.Unlock()
	if !handlerCalled {
		t.Fatal("error handler was not called")
	}
	if !errors.Is(gotErr, publishErr) {
		t.Errorf("expected %v, got %v", publishErr, gotErr)
	}
	if gotEvent.AggregateID != "agg-1" {
		t.Errorf("expected agg-1, got %s", gotEvent.AggregateID)
	}
}

// Three aggregates interleave five events each, and each aggregate sees its
// own events in order.
func TestDispatch_MultipleAggregatesOrdered(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	for v := int64(1); v <= 5; v++ {
		dispatchToEach(ctx, d, v, "a", "b", "c")
	}

	_ = d.Close(ctx)

	calls := bus.snapshot()
	if len(calls) != 15 {
		t.Fatalf("expected 15 calls, got %d", len(calls))
	}

	byAgg := make(map[string][]int64)
	for _, e := range calls {
		byAgg[e.AggregateID] = append(byAgg[e.AggregateID], e.Version)
	}

	for agg, versions := range byAgg {
		checkAggregateOrdered(t, agg, versions)
	}
}

// Events of one aggregate dispatched concurrently are each delivered once.
func TestDispatch_ConcurrentSameAggregate(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(v int64) {
			defer wg.Done()
			_ = dispatch(ctx, d, makeEvent("agg-1", v), false)
		}(int64(i))
	}
	wg.Wait()

	_ = d.Close(ctx)

	calls := bus.snapshot()
	if len(calls) != n {
		t.Fatalf("expected %d calls, got %d", n, len(calls))
	}

	seen := make(map[int64]bool)
	for _, e := range calls {
		checkNotSeen(t, seen, e.Version)
		seen[e.Version] = true
	}
}

// A panic while handling the first event does not stop the second from being
// delivered.
func TestDispatch_WorkerSurvivesPanic(t *testing.T) {
	var callCount atomic.Int64

	bus := &callbackBus{fn: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		n := callCount.Add(1)
		if n == 1 {
			panic("boom")
		}
		return nil
	}}

	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-1", 1), true)
	_ = dispatch(ctx, d, makeEvent("agg-1", 2), true)

	_ = d.Close(ctx)

	if got := callCount.Load(); got != 2 {
		t.Errorf("expected 2 PublishSync calls (panic recovered), got %d", got)
	}
}

func TestEnqueue_AckRunsAfterHandlersFinish(t *testing.T) {
	var handled, acked atomic.Bool
	gate := make(chan struct{})
	bus := &callbackBus{fn: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		<-gate
		handled.Store(true)
		return nil
	}}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())

	done, err := enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error {
		if !handled.Load() {
			t.Error("ack ran before the handlers finished")
		}
		acked.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if acked.Load() {
		t.Fatal("ack ran while the handler was still blocked")
	}
	close(gate)
	<-done
	if !acked.Load() {
		t.Fatal("ack did not run before done closed")
	}
}

func TestEnqueue_AckErrorIsReported(t *testing.T) {
	ackErr := errors.New("ack failed")
	var got atomic.Value
	d := newTestDispatcher(
		asynxmd.Bus[int](&recordingBus{}),
		withPublishErrorHandler[int](func(
			_ context.Context,
			_ asynxmd.Event[int],
			err error,
		) {
			got.Store(err)
		}),
	)
	defer d.Close(context.Background())

	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return ackErr })
	<-done

	if err, _ := got.Load().(error); !errors.Is(err, ackErr) {
		t.Fatalf("reported error = %v, want ack error", err)
	}
}

// A job enqueued while the idle worker is about to retire must still run: each
// enqueue sleeps the idle timeout first, to land right on the idle deadline.
func TestEnqueue_NeverStrandedByIdleWorkerRetirement(t *testing.T) {
	d := newTestDispatcher[int](&recordingBus{}, withIdleTimeout[int](time.Millisecond))
	defer d.Close(context.Background())

	for i := int64(1); i <= 300; i++ {
		time.Sleep(time.Millisecond)
		enqueueOnTheIdleDeadline(t, d, i)
	}
}

// A panic of the dispatcher itself is reported, and the worker survives and
// keeps delivering.
func TestHandle_DispatcherPanicIsReportedNotSwallowed(t *testing.T) {
	var got atomic.Value
	bus := &callbackBus{fn: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		panic("bus exploded")
	}}
	d := newTestDispatcher[int](bus, withPublishErrorHandler[int](func(
		_ context.Context,
		_ asynxmd.Event[int],
		err error,
	) {
		got.Store(err)
	}))
	defer d.Close(context.Background())

	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), nil)
	<-done
	if err, _ := got.Load().(error); err == nil {
		t.Fatal("a panic in the dispatcher was swallowed silently")
	}

	done, _ = enqueueEvent(context.Background(), d, makeEvent("a", 2), nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker died after a panic")
	}
}
