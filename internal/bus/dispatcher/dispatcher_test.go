package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// makeEvent creates an Event[int] with the given aggregateID and version.
func makeEvent(aggregateID string, version int64) asynxmd.Event[int] {
	return asynxmd.Event[int]{
		ID:          fmt.Sprintf("%s-v%d", aggregateID, version),
		AggregateID: aggregateID,
		EventName:   "test.event",
		Version:     version,
		OccurredAt:  time.Now(),
	}
}

// recordingBus records PublishSync calls in order.
type recordingBus struct {
	mu      sync.Mutex
	calls   []asynxmd.Event[int]
	delay   time.Duration
}

func (b *recordingBus) PublishSync(_ context.Context, e asynxmd.Event[int]) error {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	b.calls = append(b.calls, e)
	b.mu.Unlock()
	return nil
}

func (b *recordingBus) Publish(context.Context, asynxmd.Event[int]) error   { return nil }
func (b *recordingBus) Subscribe(string, asynxmd.ProjectionHandler[int], ...asynxmd.SubscriptionOpt[int]) (string, error) {
	return "", nil
}
func (b *recordingBus) Unsubscribe(string) error        { return nil }
func (b *recordingBus) Close(context.Context) error      { return nil }
func (b *recordingBus) WaitForHandlers()                 {}

func (b *recordingBus) snapshot() []asynxmd.Event[int] {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]asynxmd.Event[int], len(b.calls))
	copy(out, b.calls)
	return out
}

// errBus returns an error on every PublishSync call.
type errBus struct {
	err error
}

func (b *errBus) PublishSync(context.Context, asynxmd.Event[int]) error { return b.err }
func (b *errBus) Publish(context.Context, asynxmd.Event[int]) error     { return nil }
func (b *errBus) Subscribe(string, asynxmd.ProjectionHandler[int], ...asynxmd.SubscriptionOpt[int]) (string, error) {
	return "", nil
}
func (b *errBus) Unsubscribe(string) error   { return nil }
func (b *errBus) Close(context.Context) error { return nil }
func (b *errBus) WaitForHandlers()            {}

// callbackBus delegates PublishSync to a custom function.
type callbackBus struct {
	fn func(context.Context, asynxmd.Event[int]) error
}

func (b *callbackBus) PublishSync(ctx context.Context, e asynxmd.Event[int]) error {
	return b.fn(ctx, e)
}
func (b *callbackBus) Publish(context.Context, asynxmd.Event[int]) error { return nil }
func (b *callbackBus) Subscribe(string, asynxmd.ProjectionHandler[int], ...asynxmd.SubscriptionOpt[int]) (string, error) {
	return "", nil
}
func (b *callbackBus) Unsubscribe(string) error   { return nil }
func (b *callbackBus) Close(context.Context) error { return nil }
func (b *callbackBus) WaitForHandlers()            {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestDispatch_OrderingGuarantee(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus)

	ctx := context.Background()
	for i := int64(1); i <= 10; i++ {
		if err := d.Dispatch(ctx, makeEvent("agg-1", i), false); err != nil {
			t.Fatalf("dispatch v%d: %v", i, err)
		}
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	calls := bus.snapshot()
	if len(calls) != 10 {
		t.Fatalf("expected 10 calls, got %d", len(calls))
	}
	for i, e := range calls {
		if e.Version != int64(i+1) {
			t.Errorf("call[%d] version=%d, want %d", i, e.Version, i+1)
		}
	}
}

func TestDispatch_CrossAggregateIndependence(t *testing.T) {
	// agg-slow has a 50ms delay; agg-fast has none. agg-fast should finish
	// before agg-slow despite being dispatched second.
	var (
		mu       sync.Mutex
		finished []string
	)

	bus := &callbackBus{fn: func(_ context.Context, e asynxmd.Event[int]) error {
		if e.AggregateID == "agg-slow" {
			time.Sleep(50 * time.Millisecond)
		}
		mu.Lock()
		finished = append(finished, e.AggregateID)
		mu.Unlock()
		return nil
	}}

	d := New[int](bus)
	ctx := context.Background()

	_ = d.Dispatch(ctx, makeEvent("agg-slow", 1), false)
	_ = d.Dispatch(ctx, makeEvent("agg-fast", 1), false)

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
	d := New[int](bus)
	ctx := context.Background()

	start := time.Now()
	_ = d.Dispatch(ctx, makeEvent("agg-1", 1), true)
	elapsed := time.Since(start)

	if elapsed < delay {
		t.Errorf("expected blocking for at least %v, returned in %v", delay, elapsed)
	}
	_ = d.Close(ctx)
}

func TestDispatch_AsyncNonBlocking(t *testing.T) {
	bus := &recordingBus{delay: 100 * time.Millisecond}
	d := New[int](bus)
	ctx := context.Background()

	start := time.Now()
	_ = d.Dispatch(ctx, makeEvent("agg-1", 1), false)
	elapsed := time.Since(start)

	if elapsed > 20*time.Millisecond {
		t.Errorf("expected non-blocking return, took %v", elapsed)
	}
	_ = d.Close(ctx)
}

func TestDispatch_IdleCleanup(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus, WithIdleTimeout[int](10*time.Millisecond))
	ctx := context.Background()

	_ = d.Dispatch(ctx, makeEvent("agg-1", 1), true)

	// WaitIdle returns once the job is handled; waitWorkers waits for the
	// goroutine to exit after its idle timeout.
	d.WaitIdle()
	d.waitWorkers()

	d.mu.Lock()
	_, exists := d.queues["agg-1"]
	d.mu.Unlock()

	if exists {
		t.Error("expected queue to be cleaned up after idle timeout")
	}

	// Dispatching again should still work (new worker spins up).
	if err := d.Dispatch(ctx, makeEvent("agg-1", 2), true); err != nil {
		t.Fatalf("dispatch after idle cleanup: %v", err)
	}

	calls := bus.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(calls))
	}

	_ = d.Close(ctx)
}

func TestClose_DrainsAllEvents(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus)
	ctx := context.Background()

	for i := int64(1); i <= 20; i++ {
		_ = d.Dispatch(ctx, makeEvent("agg-1", i), false)
	}

	if err := d.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	calls := bus.snapshot()
	if len(calls) != 20 {
		t.Fatalf("expected 20 calls, got %d", len(calls))
	}
	for i, e := range calls {
		if e.Version != int64(i+1) {
			t.Errorf("call[%d] version=%d, want %d", i, e.Version, i+1)
		}
	}
}

func TestClose_Idempotent(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus)
	ctx := context.Background()

	if err := d.Close(ctx); err != nil {
		t.Fatalf("first close: %v", err)
	}
	// Second close must not panic (double close of channels).
	if err := d.Close(ctx); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestDispatch_AfterClose(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus)
	ctx := context.Background()

	_ = d.Close(ctx)

	err := d.Dispatch(ctx, makeEvent("agg-1", 1), false)
	if !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Errorf("expected ErrDispatcherClosed, got %v", err)
	}
}

func TestDispatch_NilBusNoPanic(t *testing.T) {
	d := New[int](nil)
	ctx := context.Background()

	if err := d.Dispatch(ctx, makeEvent("agg-1", 1), true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = d.Close(ctx)
}

func TestDispatch_PublishErrorHandler(t *testing.T) {
	publishErr := errors.New("publish failed")
	bus := &errBus{err: publishErr}

	var (
		mu         sync.Mutex
		gotEvent   asynxmd.Event[int]
		gotErr     error
		handlerCalled bool
	)

	handler := func(_ context.Context, e asynxmd.Event[int], err error) {
		mu.Lock()
		gotEvent = e
		gotErr = err
		handlerCalled = true
		mu.Unlock()
	}

	d := New[int](bus, WithPublishErrorHandler[int](handler))
	ctx := context.Background()

	ev := makeEvent("agg-1", 1)
	_ = d.Dispatch(ctx, ev, true)
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

func TestDispatch_MultipleAggregatesOrdered(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus)
	ctx := context.Background()

	// Interleave 3 aggregates, 5 events each.
	for v := int64(1); v <= 5; v++ {
		for _, agg := range []string{"a", "b", "c"} {
			_ = d.Dispatch(ctx, makeEvent(agg, v), false)
		}
	}

	_ = d.Close(ctx)

	calls := bus.snapshot()
	if len(calls) != 15 {
		t.Fatalf("expected 15 calls, got %d", len(calls))
	}

	// Group by aggregate and verify per-aggregate ordering.
	byAgg := make(map[string][]int64)
	for _, e := range calls {
		byAgg[e.AggregateID] = append(byAgg[e.AggregateID], e.Version)
	}

	for agg, versions := range byAgg {
		if len(versions) != 5 {
			t.Errorf("aggregate %s: expected 5 events, got %d", agg, len(versions))
		}
		for i := 1; i < len(versions); i++ {
			if versions[i] <= versions[i-1] {
				t.Errorf("aggregate %s: out of order at index %d: %v", agg, i, versions)
				break
			}
		}
	}
}

func TestClose_ContextTimeout(t *testing.T) {
	// Bus blocks forever on PublishSync, signalling when it starts.
	started := make(chan struct{})
	bus := &callbackBus{fn: func(_ context.Context, _ asynxmd.Event[int]) error {
		close(started)
		select {} // block forever
	}}

	d := New[int](bus)
	ctx := context.Background()

	_ = d.Dispatch(ctx, makeEvent("agg-1", 1), false)

	// Wait until worker is actually inside PublishSync.
	<-started

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := d.Close(closeCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestDispatch_ConcurrentSameAggregate(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus)
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(v int64) {
			defer wg.Done()
			_ = d.Dispatch(ctx, makeEvent("agg-1", v), false)
		}(int64(i))
	}
	wg.Wait()

	_ = d.Close(ctx)

	calls := bus.snapshot()
	if len(calls) != n {
		t.Fatalf("expected %d calls, got %d", n, len(calls))
	}

	// Verify no duplicates.
	seen := make(map[int64]bool)
	for _, e := range calls {
		if seen[e.Version] {
			t.Errorf("duplicate version %d", e.Version)
		}
		seen[e.Version] = true
	}
}

func TestDispatch_WithIdleTimeoutOption(t *testing.T) {
	bus := &recordingBus{}
	d := New[int](bus, WithIdleTimeout[int](10*time.Millisecond))
	ctx := context.Background()

	_ = d.Dispatch(ctx, makeEvent("agg-1", 1), true)

	// WaitIdle returns once the job is handled; waitWorkers waits for the
	// goroutine to exit after its idle timeout.
	d.WaitIdle()
	d.waitWorkers()

	d.mu.Lock()
	qLen := len(d.queues)
	d.mu.Unlock()

	if qLen != 0 {
		t.Errorf("expected 0 queues after idle timeout, got %d", qLen)
	}

	_ = d.Close(ctx)
}

func TestDispatch_WorkerSurvivesPanic(t *testing.T) {
	var callCount atomic.Int64

	bus := &callbackBus{fn: func(_ context.Context, e asynxmd.Event[int]) error {
		n := callCount.Add(1)
		if n == 1 {
			panic("boom")
		}
		return nil
	}}

	d := New[int](bus)
	ctx := context.Background()

	// First event will panic inside handle; second should still be delivered.
	_ = d.Dispatch(ctx, makeEvent("agg-1", 1), true)
	_ = d.Dispatch(ctx, makeEvent("agg-1", 2), true)

	_ = d.Close(ctx)

	if got := callCount.Load(); got != 2 {
		t.Errorf("expected 2 PublishSync calls (panic recovered), got %d", got)
	}
}

func TestEnqueue_AckRunsAfterHandlersFinish(t *testing.T) {
	var handled, acked atomic.Bool
	gate := make(chan struct{})
	bus := &callbackBus{fn: func(context.Context, asynxmd.Event[int]) error {
		<-gate
		handled.Store(true)
		return nil
	}}
	d := New[int](bus)
	defer d.Close(context.Background())

	done, err := d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error {
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
	if err := <-ticket.Final; err != nil {
		t.Fatalf("delivery ended with %v", err)
	}
	if !acked.Load() {
		t.Error("ack did not run after the bus rejected the event")
	}
	if reported.Load() != 1 {
		t.Errorf("publish errors reported = %d, want 1", reported.Load())
	}
}

func TestEnqueue_AckErrorIsReported(t *testing.T) {
	ackErr := errors.New("ack failed")
	var got atomic.Value
	d := New(
		asynxmd.Bus[int](&recordingBus{}),
		WithPublishErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error) { got.Store(err) }),
	)
	defer d.Close(context.Background())

	done, _ := d.Enqueue(context.Background(), makeEvent("a", 1), func(context.Context) error { return ackErr })
	<-done

	if err, _ := got.Load().(error); !errors.Is(err, ackErr) {
		t.Fatalf("reported error = %v, want ack error", err)
	}
}

// A job enqueued while the idle worker is about to retire must still run.
func TestEnqueue_NeverStrandedByIdleWorkerRetirement(t *testing.T) {
	d := New[int](&recordingBus{}, WithIdleTimeout[int](time.Millisecond))
	defer d.Close(context.Background())

	for i := int64(1); i <= 300; i++ {
		time.Sleep(time.Millisecond) // land right on the idle deadline
		done, err := d.Enqueue(context.Background(), makeEvent("a", i), nil)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("event %d was stranded on a retired queue", i)
		}
	}
}

// Enqueue must never block on a slow handler: callers enqueue while holding
// their per-aggregate command lock.
func TestEnqueue_NeverBlocksBehindASlowHandler(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var seen []int64
	bus := &callbackBus{fn: func(_ context.Context, e asynxmd.Event[int]) error {
		<-release
		mu.Lock()
		seen = append(seen, e.Version)
		mu.Unlock()
		return nil
	}}
	d := New[int](bus)
	defer d.Close(context.Background())

	const n = 200
	enqueued := make(chan struct{})
	go func() {
		for i := int64(1); i <= n; i++ {
			if _, err := d.Enqueue(context.Background(), makeEvent("a", i), nil); err != nil {
				t.Errorf("Enqueue: %v", err)
			}
		}
		close(enqueued)
	}()

	select {
	case <-enqueued:
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked behind a slow handler")
	}

	close(release)
	d.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("handled %d events, want %d", len(seen), n)
	}
	for i, v := range seen {
		if v != int64(i+1) {
			t.Fatalf("order broken at %d: %v", i, v)
		}
	}
}

func TestClose_DrainsJobsQueuedBehindASlowHandler(t *testing.T) {
	release := make(chan struct{})
	running := make(chan struct{}, 3)
	var handled atomic.Int32
	bus := &callbackBus{fn: func(context.Context, asynxmd.Event[int]) error {
		running <- struct{}{}
		<-release
		handled.Add(1)
		return nil
	}}
	d := New[int](bus)

	for i := int64(1); i <= 3; i++ {
		if _, err := d.Enqueue(context.Background(), makeEvent("a", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	<-running // the first job is inside its handler, two more are queued behind it

	closed := make(chan error, 1)
	go func() { closed <- d.Close(context.Background()) }()
	for {
		d.mu.Lock()
		isClosed := d.closed
		d.mu.Unlock()
		if isClosed {
			break
		}
		runtime.Gosched()
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) before the queued jobs drained", err)
	default:
	}
	close(release)

	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if handled.Load() != 3 {
		t.Fatalf("handled %d jobs, want 3 (Close must drain)", handled.Load())
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

	for v, got := range map[int]error{1: recv(t, t1.Final, "v1 final"), 2: recv(t, t2.Final, "v2 final")} {
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
	if got := recv(t, tk.Final, "final"); !errors.Is(got, asynxmd.ErrDispatcherClosed) {
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
	if err := recv(t, last.Final, "final"); err != nil {
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

	if err := recv(t, t1.Final, "v1 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked", err)
	}
	if err := recv(t, t2.Final, "v2 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
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
	if err := recv(t, t3.Final, "v3 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v3 final = %v, want ErrDeliveryParked", err)
	}

	// Another aggregate flows.
	tb, _ := d.EnqueueTicket(ctx, makeEvent("b", 1), ackInto(&acked, makeEvent("b", 1)))
	if err := recv(t, tb.Final, "b final"); err != nil {
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
	recv(t, tk.Final, "parked")

	healthy.Store(true)
	d.Resume("a")
	tk, err := d.EnqueueRecovered(context.Background(), makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := recv(t, tk.Final, "redelivery"); err != nil {
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

	if err := recv(t, t1.Final, "v1"); err != nil {
		t.Fatalf("dead-lettered event final = %v, want nil", err)
	}
	if err := recv(t, t2.Final, "v2"); err != nil {
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
	if err := recv(t, tk.Final, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
}

func TestQueueBound_ParksTheAggregateInsteadOfGrowingWithoutLimit(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	var rec deliveries
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		rec.add(e)
		if e.Version == 1 {
			select {
			case started <- struct{}{}:
			default:
			}
			<-gate
			return errors.New("poison")
		}
		return nil
	}}}, WithMaxQueued[int](3), WithMaxAttempts[int](1000))

	var acked sync.Map
	ctx := context.Background()
	first, _ := d.EnqueueTicket(ctx, makeEvent("a", 1), ackInto(&acked, makeEvent("a", 1)))
	recv(t, started, "v1 running")

	var tickets []Ticket
	for v := int64(2); v <= 6; v++ {
		tk, _ := d.EnqueueTicket(ctx, makeEvent("a", v), ackInto(&acked, makeEvent("a", v)))
		tickets = append(tickets, tk)
	}
	// v2..v4 fit behind the running v1; v5 overflows and parks everything.
	for i, tk := range tickets {
		if err := recv(t, tk.Final, "queued ticket"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
			t.Fatalf("ticket %d final = %v, want ErrDeliveryParked", i+2, err)
		}
	}
	close(gate)
	if err := recv(t, first.Final, "v1 final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("v1 final = %v, want ErrDeliveryParked (interrupted by the park)", err)
	}
	for _, s := range rec.list() {
		if s != "a/1" {
			t.Fatalf("delivered %q although the aggregate was parked", s)
		}
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
	tk, err := d.EnqueueForget(ctx, tomb)
	if err != nil {
		t.Fatal(err)
	}
	if err := recv(t, tk.Final, "tombstone"); err != nil {
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
	tk, _ := d.EnqueueForget(context.Background(), tomb)
	if err := recv(t, tk.Final, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if len(d.Halted()) != 0 {
		t.Fatal("a failed tombstone must not park the aggregate (Forget fails instead)")
	}
}

func TestPark_OfAnAggregateWithoutAQueueStillBlocksItsDurableEvents(t *testing.T) {
	d := newDurable(t, nil)
	d.Park("ghost")
	d.Park("ghost") // idempotent
	d.Resume("never-parked")

	var acked sync.Map
	tk, _ := d.EnqueueTicket(context.Background(), makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := recv(t, tk.Final, "final"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("final = %v, want ErrDeliveryParked", err)
	}
	if _, ok := acked.Load(int64(1)); ok {
		t.Fatal("event of a parked aggregate was acknowledged")
	}

	d.Resume("ghost")
	tk, _ = d.EnqueueTicket(context.Background(), makeEvent("ghost", 1), ackInto(&acked, makeEvent("ghost", 1)))
	if err := recv(t, tk.Final, "final after resume"); err != nil {
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
	if err := recv(t, durable.Final, "durable ticket"); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("durable final = %v, want ErrDeliveryParked", err)
	}
	close(gate)
	if err := recv(t, live.Final, "live ticket"); err != nil {
		t.Fatalf("live-only job final = %v, want delivered", err)
	}
}

func TestEnqueueRecovered_IsExemptFromTheQueueBound(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	d := newDurable(t, []DurableSub[int]{{Match: always, Handler: func(_ context.Context, e asynxmd.Event[int]) error {
		if e.Version == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}}}, WithMaxQueued[int](2))
	ctx := context.Background()
	ack := func(context.Context) error { return nil }
	first, _ := d.EnqueueRecovered(ctx, makeEvent("a", 1), ack)
	recv(t, started, "v1 running")
	var last Ticket
	for v := int64(2); v <= 6; v++ {
		last, _ = d.EnqueueRecovered(ctx, makeEvent("a", v), ack)
	}
	close(gate)
	if err := recv(t, last.Final, "last"); err != nil {
		t.Fatalf("recovered backlog was parked: %v", err)
	}
	recv(t, first.Final, "first")
	if len(d.Halted()) != 0 {
		t.Fatalf("halted = %v", d.Halted())
	}
}

func TestOptions_NonPositiveBoundsAreIgnored(t *testing.T) {
	d := New[int](nil, WithMaxAttempts[int](0), WithMaxQueued[int](-1))
	if d.maxAttempts != defaultMaxAttempts || d.maxQueued != defaultMaxQueued {
		t.Fatalf("maxAttempts=%d maxQueued=%d, want the defaults", d.maxAttempts, d.maxQueued)
	}
}
