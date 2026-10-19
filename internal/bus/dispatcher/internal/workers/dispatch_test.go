package workers_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
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

// A failing live subscriber must not skip the durable delivery: the next
// event of the aggregate would otherwise overtake this one.
func TestEnqueue_DurableDeliveryStillAcksWhenBusRejectsEvent(t *testing.T) {
	var acked atomic.Bool
	var reported atomic.Int32
	d := newTestDispatcher(
		asynxmd.Bus[int](&errBus{err: errors.New("bus closed")}),
		withPublishErrorHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) {
			reported.Add(1)
		}),
	)
	defer d.Close(context.Background())

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error {
		acked.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

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

func TestDurable_NonDurableJobsSkipDurableSubscribers(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		t.Error("durable handler ran for a job without an ack")
		return nil
	}}})
	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), nil)
	<-done
}

func TestBusErrorDoesNotSkipTheDurableDelivery(t *testing.T) {
	var rec deliveries
	d := newTestDispatcher[int](&errBus{err: errors.New("bus down")},
		withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			_ context.Context,
			e asynxmd.Event[int],
		) error {
			rec.add(e)
			return nil
		}}}),
	)
	defer d.Close(context.Background())

	var acked sync.Map
	var last asynxmd.Ticket
	for v := int64(1); v <= 3; v++ {
		last, _ = enqueueTicket(context.Background(), d, makeEvent("a", v), ackInto(&acked, makeEvent("a", v)))
	}
	if err := final(t, last, "final"); err != nil {
		t.Fatalf("final = %v", err)
	}
	got := rec.list()
	inOrder := len(got) == 3 && got[0] == "a/1" && got[2] == "a/3"
	if !inOrder {
		t.Fatalf("durable deliveries = %v, want a/1 a/2 a/3 in order", got)
	}
}

func TestEnqueueRedelivery_GoesToTheDurableSubscribersOnly(t *testing.T) {
	bus := &recordingBus{}
	var seen deliveries
	d := newTestDispatcher[int](bus, withDurableSubs([]asynxmd.DurableSub[int]{{Match: func(name string) bool { return name == "test.event" },
		Handler: func(
			_ context.Context,
			e asynxmd.Event[int],
		) error {
			seen.add(e)
			return nil
		}}}))
	t.Cleanup(func() { d.Close(context.Background()) })
	var acked atomic.Int32
	ack := func(context.Context) error { acked.Add(1); return nil }
	ctx := context.Background()

	tk, _ := enqueueRedelivery(ctx, d, makeEvent("a", 1), ack)
	final(t, tk, "event")
	marker := makeEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ = enqueueRedelivery(ctx, d, marker, ack)
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

func TestBusPanicDoesNotSkipTheDurableDelivery(t *testing.T) {
	var acked atomic.Bool
	var reported atomic.Int32
	d := newTestDispatcher[int](&panicBus{}, withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}}),
		withPublishErrorHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) {
			reported.Add(1)
		}))
	t.Cleanup(func() { d.Close(context.Background()) })
	tk, _ := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { acked.Store(true); return nil })
	if err := final(t, tk, "delivery"); err != nil {
		t.Fatalf("final = %v", err)
	}
	if !acked.Load() || reported.Load() != 1 {
		t.Fatalf("acked=%v reported=%d, want the panic reported and the delivery completed", acked.Load(), reported.Load())
	}
}

func TestPublishErrorHandlerPanicDoesNotCrashTheWorker(t *testing.T) {
	d := newTestDispatcher[int](&errBus{err: errors.New("down")}, withPublishErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) {
		panic("boom")
	}))
	t.Cleanup(func() { d.Close(context.Background()) })
	done, err := enqueueEvent(context.Background(), d, makeEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	recv(t, done, "settled")
	done, _ = enqueueEvent(context.Background(), d, makeEvent("a", 2), nil)
	recv(t, done, "the worker survives")
}

// A job popped just before a Forget supersedes its aggregate belongs to the old
// generation: it must not run its retry loop ahead of the marker.
func TestSupersede_AfterThePopStillSkipsTheSupersededJob(t *testing.T) {
	bus := &gateBus{entered: make(chan struct{}), release: make(chan struct{})}
	var oldRuns atomic.Int32
	d := newTestDispatcher[int](bus, withDurableSubs([]asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		if e.EventName != asynxmd.ForgetEventName {
			oldRuns.Add(1)
			return errors.New("down")
		}
		return nil
	}}}), withRetryBackoff[int](50*time.Millisecond, 50*time.Millisecond), withMaxAttempts[int](5))
	t.Cleanup(func() { d.Close(context.Background()) })

	ctx := context.Background()
	enqueueTicket(ctx, d, makeEvent("a", 1), func(context.Context) error { return nil })
	recv(t, bus.entered, "v1 popped and publishing")

	d.Supersede("a")
	marker := makeEvent("a", 2)
	marker.EventName = asynxmd.ForgetEventName
	tk, _ := enqueueForget(ctx, d, marker, func(context.Context) error { return nil })
	close(bus.release)

	start := time.Now()
	if err := final(t, tk, "marker"); err != nil {
		t.Fatalf("marker = %v", err)
	}
	if time.Since(start) > 40*time.Millisecond || oldRuns.Load() != 0 {
		t.Fatalf("the superseded job ran %d attempts and held the marker back %v", oldRuns.Load(), time.Since(start))
	}
	if got := halted(d); len(got) != 0 {
		t.Fatalf("halted = %v", got)
	}
}

func TestAckFailure_TheAggregateIsParkedBeforeThePublishErrorHandlerIsTold(t *testing.T) {
	var d asynxmd.Dispatcher[int]
	parkedWhenTold := make(chan []string, 1)
	d = newDurable(t,
		[]asynxmd.DurableSub[int]{{Match: always, Handler: func(
			context.Context,
			asynxmd.Event[int],
		) error {
			return nil
		}}},
		withPublishErrorHandler[int](func(
			context.Context,
			asynxmd.Event[int],
			error,
		) {
			parkedWhenTold <- halted(d)
		}))

	enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return errors.New("store down") })
	if got := recv(t, parkedWhenTold, "the ack failure to be reported"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("parked aggregates when the handler was told = %v, want [a]", got)
	}
}

// If the ack of v1 fails, v2 must not be delivered and acknowledged ahead of it.
func TestAckFailure_ParksTheAggregateSoALaterEventDoesNotOvertake(t *testing.T) {
	var rec deliveries
	var reported atomic.Int32
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		_ context.Context,
		e asynxmd.Event[int],
	) error {
		rec.add(e)
		return nil
	}}}, withPublishErrorHandler[int](func(
		context.Context,
		asynxmd.Event[int],
		error,
	) {
		reported.Add(1)
	}))

	ctx := context.Background()
	t1, _ := enqueueTicket(ctx, d, makeEvent("a", 1), func(context.Context) error { return errors.New("store down") })
	t2, _ := enqueueTicket(ctx, d, makeEvent("a", 2), func(context.Context) error { return nil })
	if err := final(t, t1, "v1"); err == nil {
		t.Fatal("v1 ended as acknowledged")
	}
	if err := final(t, t2, "v2"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v2 = %v, want ErrDeliveryParked", err)
	}
	if got := rec.list(); !slices.Equal(got, []string{"a/1"}) {
		t.Fatalf("delivered %v, want only v1", got)
	}
	if got := halted(d); !slices.Equal(got, []string{"a"}) || reported.Load() != 1 {
		t.Fatalf("halted=%v reported=%d", got, reported.Load())
	}
}

func makeEvent(
	aggregateID string,
	version int64,
) asynxmd.Event[int] {
	return asynxmd.Event[int]{
		ID:          fmt.Sprintf("%s-v%d", aggregateID, version),
		AggregateID: aggregateID,
		EventName:   "test.event",
		Version:     version,
		OccurredAt:  time.Now(),
	}
}

func newTestDispatcher[T any](
	bus asynxmd.Bus[T],
	opts ...testOpt[T],
) asynxmd.Dispatcher[T] {
	c := testConfig[T]{cfg: asynxmd.DispatcherConfig[T]{
		Bus:          bus,
		RetryInitial: 100 * time.Millisecond,
		RetryMax:     30 * time.Second,
		MaxAttempts:  20,
	}}
	for _, opt := range opts {
		opt(&c)
	}
	return dispatcher.New(c.cfg, c.opts...)
}

func withIdleTimeout[T any](
	t time.Duration,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.opts = append(c.opts, dispatcher.WithIdleTimeout[T](t))
	}
}

func withPublishErrorHandler[T any](
	fn asynxmd.PublishErrorHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.OnPublishError = fn
	}
}

func withDurableSubs[T any](
	subs []asynxmd.DurableSub[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.DurableSubs = subs
	}
}

func withRetryBackoff[T any](
	initial time.Duration,
	max time.Duration,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.RetryInitial, c.cfg.RetryMax = initial, max
	}
}

func withMaxAttempts[T any](
	n int,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.MaxAttempts = n
	}
}

func dispatch[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	waitHandlers bool,
) error {
	t, err := d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event})
	if err != nil {
		return err
	}
	if waitHandlers {
		<-t.Settled
	}
	return nil
}

func enqueueEvent[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	t, err := enqueueTicket(ctx, d, event, ack)
	return t.Settled, err
}

func enqueueTicket[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchEvent})
}

func enqueueRedelivery[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchRedelivery})
}

func enqueueForget[T any](
	ctx context.Context,
	d asynxmd.Dispatcher[T],
	tombstone asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: tombstone, Ack: ack, Kind: asynxmd.DispatchTombstone})
}

func halted[T any](
	d asynxmd.Dispatcher[T],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func always(string) bool { return true }

func newDurable(
	t *testing.T,
	subs []asynxmd.DurableSub[int],
	opts ...testOpt[int],
) asynxmd.Dispatcher[int] {
	t.Helper()
	all := append([]testOpt[int]{withDurableSubs(subs), withRetryBackoff[int](time.Millisecond, 5*time.Millisecond)}, opts...)
	d := newTestDispatcher[int](&recordingBus{}, all...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return d
}

func recv[V any](
	t *testing.T,
	ch <-chan V,
	what string,
) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func final(
	t *testing.T,
	tk asynxmd.Ticket,
	what string,
) error {
	t.Helper()
	recv(t, tk.Final, what)
	return tk.Err()
}

func ackInto(
	acked *sync.Map,
	e asynxmd.Event[int],
) func(context.Context) error {
	return func(context.Context) error { acked.Store(e.Version, true); return nil }
}

func dispatchOrFail(
	t *testing.T,
	d asynxmd.Dispatcher[int],
	i int64,
) {
	t.Helper()
	if err := dispatch(context.Background(), d, makeEvent("agg-1", i), false); err != nil {
		t.Fatalf("dispatch v%d: %v", i, err)
	}
}

func checkCallVersion(
	t *testing.T,
	i int,
	e asynxmd.Event[int],
) {
	t.Helper()
	if e.Version != int64(i+1) {
		t.Errorf("call[%d] version=%d, want %d", i, e.Version, i+1)
	}
}

func dispatchToEach(
	ctx context.Context,
	d asynxmd.Dispatcher[int],
	v int64,
	aggs ...string,
) {
	for _, agg := range aggs {
		_ = dispatch(ctx, d, makeEvent(agg, v), false)
	}
}

func checkAggregateOrdered(
	t *testing.T,
	agg string,
	versions []int64,
) {
	t.Helper()
	if len(versions) != 5 {
		t.Errorf("aggregate %s: expected 5 events, got %d", agg, len(versions))
	}
	i := 1
	for i < len(versions) && versions[i] > versions[i-1] {
		i++
	}
	if i < len(versions) {
		t.Errorf("aggregate %s: out of order at index %d: %v", agg, i, versions)
	}
}

func checkNotSeen(
	t *testing.T,
	seen map[int64]bool,
	version int64,
) {
	t.Helper()
	if seen[version] {
		t.Errorf("duplicate version %d", version)
	}
}

func enqueueOnTheIdleDeadline(
	t *testing.T,
	d asynxmd.Dispatcher[int],
	i int64,
) {
	t.Helper()
	done, err := enqueueEvent(context.Background(), d, makeEvent("a", i), nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("event %d was stranded on a retired queue", i)
	}
}

type callbackBus struct {
	fn func(
		context.Context,
		asynxmd.Event[int],
	) error
}

func (b *callbackBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	return b.fn(ctx, e)
}

func (b *callbackBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *callbackBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *callbackBus) Unsubscribe(string) error { return nil }

func (b *callbackBus) Close(context.Context) error { return nil }

func (b *callbackBus) WaitForHandlers() {}

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

type errBus struct {
	err error
}

func (b *errBus) PublishSync(
	context.Context,
	asynxmd.Event[int],
) error {
	return b.err
}

func (b *errBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *errBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *errBus) Unsubscribe(string) error { return nil }

func (b *errBus) Close(context.Context) error { return nil }

func (b *errBus) WaitForHandlers() {}

type gateBus struct {
	recordingBus
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *gateBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return nil
}

type panicBus struct{ recordingBus }

func (*panicBus) PublishSync(
	context.Context,
	asynxmd.Event[int],
) error {
	panic("bus exploded")
}

type recordingBus struct {
	mu    sync.Mutex
	calls []asynxmd.Event[int]
	delay time.Duration
}

func (b *recordingBus) PublishSync(
	_ context.Context,
	e asynxmd.Event[int],
) error {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	b.calls = append(b.calls, e)
	b.mu.Unlock()
	return nil
}

func (b *recordingBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *recordingBus) Subscribe(
	string,
	asynxmd.ProjectionHandler[int],
	...asynxmd.SubscriptionOpt[int],
) (string, error) {
	return "", nil
}

func (b *recordingBus) Unsubscribe(string) error { return nil }

func (b *recordingBus) Close(context.Context) error { return nil }

func (b *recordingBus) WaitForHandlers() {}

func (b *recordingBus) snapshot() []asynxmd.Event[int] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]asynxmd.Event[int], len(b.calls))
	copy(out, b.calls)
	return out
}

type testConfig[T any] struct {
	cfg  asynxmd.DispatcherConfig[T]
	opts []dispatcher.Opt[T]
}

type testOpt[T any] func(
	c *testConfig[T],
)
