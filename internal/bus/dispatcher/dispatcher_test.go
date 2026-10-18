package dispatcher

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

type testConfig[T any] struct {
	cfg  asynxmd.DispatcherConfig[T]
	opts []Opt[T]
}

type testOpt[T any] func(
	c *testConfig[T],
)

func newTestDispatcher[T any](
	bus asynxmd.Bus[T],
	opts ...testOpt[T],
) *dispatcher[T] {
	c := testConfig[T]{cfg: asynxmd.DispatcherConfig[T]{
		Bus:          bus,
		RetryInitial: 100 * time.Millisecond,
		RetryMax:     30 * time.Second,
		MaxAttempts:  20,
	}}
	for _, opt := range opts {
		opt(&c)
	}
	return newDispatcher(c.cfg, c.opts...)
}

func withIdleTimeout[T any](
	t time.Duration,
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.opts = append(c.opts, WithIdleTimeout[T](t))
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

func withDeliveryErrorHandler[T any](
	fn asynxmd.DeliveryErrorHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.OnDeliveryError = fn
	}
}

func withDeliveryPanicHandler[T any](
	fn asynxmd.PanicHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.OnPanic = fn
	}
}

func withDeadLetterHandler[T any](
	fn asynxmd.DeadLetterHandler[T],
) testOpt[T] {
	return func(
		c *testConfig[T],
	) {
		c.cfg.DeadLetter = fn
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
	d *dispatcher[T],
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
	d *dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (<-chan struct{}, error) {
	t, err := enqueueTicket(ctx, d, event, ack)
	return t.Settled, err
}

func enqueueTicket[T any](
	ctx context.Context,
	d *dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchEvent})
}

func enqueueRedelivery[T any](
	ctx context.Context,
	d *dispatcher[T],
	event asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: event, Ack: ack, Kind: asynxmd.DispatchRedelivery})
}

func enqueueForget[T any](
	ctx context.Context,
	d *dispatcher[T],
	tombstone asynxmd.Event[T],
	ack func(context.Context) error,
) (asynxmd.Ticket, error) {
	return d.Enqueue(ctx, asynxmd.DispatchRequest[T]{Event: tombstone, Ack: ack, Kind: asynxmd.DispatchTombstone})
}

func halted[T any](
	d *dispatcher[T],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func resume[T any](
	d *dispatcher[T],
	agg string,
) bool {
	generation, parked := d.Parks()[agg]
	if !parked {
		return false
	}
	lifted, _ := d.ResumeUndelivered(agg, generation)
	return lifted
}

// recordingBus records PublishSync calls in order.
type recordingBus struct {
	mu    sync.Mutex
	calls []asynxmd.Event[int]
	delay time.Duration
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

func (b *recordingBus) Publish(
	context.Context,
	asynxmd.Event[int],
) error {
	return nil
}

func (b *recordingBus) Subscribe(string, asynxmd.ProjectionHandler[int], ...asynxmd.SubscriptionOpt[int]) (string, error) {
	return "", nil
}
func (b *recordingBus) Unsubscribe(string) error    { return nil }
func (b *recordingBus) Close(context.Context) error { return nil }
func (b *recordingBus) WaitForHandlers()            {}

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
func (b *errBus) Unsubscribe(string) error    { return nil }
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
func (b *callbackBus) Unsubscribe(string) error    { return nil }
func (b *callbackBus) Close(context.Context) error { return nil }
func (b *callbackBus) WaitForHandlers()            {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestDispatch_OrderingGuarantee(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)

	ctx := context.Background()
	for i := int64(1); i <= 10; i++ {
		if err := dispatch(ctx, d, makeEvent("agg-1", i), false); err != nil {
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

func TestDispatch_IdleCleanup(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus, withIdleTimeout[int](10*time.Millisecond))
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-1", 1), true)

	// WaitIdle returns once the job is handled; workers.Wait waits for the
	// goroutine to exit after its idle timeout.
	d.WaitIdle()
	d.workers.Wait()

	exists := d.registry.Wake("agg-1") != nil

	if exists {
		t.Error("expected queue to be cleaned up after idle timeout")
	}

	// Dispatching again should still work (new worker spins up).
	if err := dispatch(ctx, d, makeEvent("agg-1", 2), true); err != nil {
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
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	for i := int64(1); i <= 20; i++ {
		_ = dispatch(ctx, d, makeEvent("agg-1", i), false)
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
	d := newTestDispatcher[int](bus)
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
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	_ = d.Close(ctx)

	err := dispatch(ctx, d, makeEvent("agg-1", 1), false)
	if !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Errorf("expected ErrDispatcherClosed, got %v", err)
	}
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

	handler := func(_ context.Context, e asynxmd.Event[int], err error) {
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

func TestDispatch_MultipleAggregatesOrdered(t *testing.T) {
	bus := &recordingBus{}
	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	// Interleave 3 aggregates, 5 events each.
	for v := int64(1); v <= 5; v++ {
		for _, agg := range []string{"a", "b", "c"} {
			_ = dispatch(ctx, d, makeEvent(agg, v), false)
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

	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-1", 1), false)

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
	d := newTestDispatcher[int](bus, withIdleTimeout[int](10*time.Millisecond))
	ctx := context.Background()

	_ = dispatch(ctx, d, makeEvent("agg-1", 1), true)

	// WaitIdle returns once the job is handled; workers.Wait waits for the
	// goroutine to exit after its idle timeout.
	d.WaitIdle()
	d.workers.Wait()

	if d.registry.Wake("agg-1") != nil {
		t.Errorf("expected 0 queues after idle timeout, got the queue of agg-1")
	}

	_ = d.Close(ctx)
}

func TestWithIdleTimeout_IgnoresNonPositiveValues(
	t *testing.T,
) {
	d := newDispatcher(asynxmd.DispatcherConfig[int]{}, WithIdleTimeout[int](0), WithIdleTimeout[int](-time.Second))
	defer d.Close(context.Background())
	if d.idleTimeout != defaultIdleTimeout {
		t.Fatalf("idleTimeout = %v, want the default", d.idleTimeout)
	}
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

	d := newTestDispatcher[int](bus)
	ctx := context.Background()

	// First event will panic inside handle; second should still be delivered.
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
	bus := &callbackBus{fn: func(context.Context, asynxmd.Event[int]) error {
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
		withPublishErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error) { got.Store(err) }),
	)
	defer d.Close(context.Background())

	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return ackErr })
	<-done

	if err, _ := got.Load().(error); !errors.Is(err, ackErr) {
		t.Fatalf("reported error = %v, want ack error", err)
	}
}

// A job enqueued while the idle worker is about to retire must still run.
func TestEnqueue_NeverStrandedByIdleWorkerRetirement(t *testing.T) {
	d := newTestDispatcher[int](&recordingBus{}, withIdleTimeout[int](time.Millisecond))
	defer d.Close(context.Background())

	for i := int64(1); i <= 300; i++ {
		time.Sleep(time.Millisecond) // land right on the idle deadline
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
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())

	const n = 200
	enqueued := make(chan struct{})
	go func() {
		for i := int64(1); i <= n; i++ {
			if _, err := enqueueEvent(context.Background(), d, makeEvent("a", i), nil); err != nil {
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
	d := newTestDispatcher[int](bus)

	for i := int64(1); i <= 3; i++ {
		if _, err := enqueueEvent(context.Background(), d, makeEvent("a", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	<-running // the first job is inside its handler, two more are queued behind it

	closed := make(chan error, 1)
	go func() { closed <- d.Close(context.Background()) }()
	<-d.Interrupted() // Close has closed the registry and is waiting for the workers
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

func TestHandle_DispatcherPanicIsReportedNotSwallowed(t *testing.T) {
	var got atomic.Value
	bus := &callbackBus{fn: func(context.Context, asynxmd.Event[int]) error { panic("bus exploded") }}
	d := newTestDispatcher[int](bus, withPublishErrorHandler[int](func(_ context.Context, _ asynxmd.Event[int], err error) { got.Store(err) }))
	defer d.Close(context.Background())

	done, _ := enqueueEvent(context.Background(), d, makeEvent("a", 1), nil)
	<-done
	if err, _ := got.Load().(error); err == nil {
		t.Fatal("a panic in the dispatcher was swallowed silently")
	}

	// The worker survives and keeps delivering.
	done, _ = enqueueEvent(context.Background(), d, makeEvent("a", 2), nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker died after a panic")
	}
}

// blockingBus holds every PublishSync until release is closed.
type blockingBus struct {
	recordingBus
	entered chan struct{}
	release chan struct{}
}

func (b *blockingBus) PublishSync(
	ctx context.Context,
	e asynxmd.Event[int],
) error {
	b.entered <- struct{}{}
	<-b.release
	return b.recordingBus.PublishSync(ctx, e)
}

func TestTicketFor_FindsAnEventWhileItsDeliveryIsUnderWay(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())

	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor found an event that was never queued")
	}

	enqueued, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	<-bus.entered

	ticket, found := d.TicketFor("a", 1)
	if !found {
		t.Fatal("TicketFor lost an event whose handler is running")
	}
	if ticket.Settled != enqueued.Settled || ticket.Final != enqueued.Final {
		t.Fatal("TicketFor returned a different ticket than the one Enqueue returned")
	}
	if _, found := d.TicketFor("a", 2); found {
		t.Fatal("TicketFor matched another version")
	}
	if _, found := d.TicketFor("b", 1); found {
		t.Fatal("TicketFor matched another aggregate")
	}

	close(bus.release)
	<-ticket.Final
	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor kept an event whose delivery is over")
	}
}

func TestTicketFor_ForgetsEventsLeftBehindByAPark(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})
	d.Park("a")

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ticket.Final
	if _, found := d.TicketFor("a", 1); found {
		t.Fatal("TicketFor kept an event that was never queued because its aggregate is parked")
	}
}

func TestUnrun_RemembersEventsLeftUndeliveredByAPark(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})
	d.Park("a")

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ticket.Final

	if err := d.Unrun("a", 1); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 1) = %v, want ErrDeliveryParked", err)
	}
	if err := d.Unrun("a", 2); err != nil {
		t.Fatalf("Unrun of an event nobody enqueued = %v, want nil", err)
	}
	if err := d.Unrun("b", 1); err != nil {
		t.Fatalf("Unrun of another aggregate = %v, want nil", err)
	}

	resume(d, "a")
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun after the park lifted = %v, want nil", err)
	}
}

func TestUnrun_RemembersQueuedEventsThatParkingTookOutOfMemory(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())
	ack := func(context.Context) error { return nil }
	if _, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), ack); err != nil {
		t.Fatal(err)
	}
	<-bus.entered
	queued, err := enqueueTicket(context.Background(), d, makeEvent("a", 2), ack)
	if err != nil {
		t.Fatal(err)
	}

	d.Park("a")
	<-queued.Final
	close(bus.release)

	if err := d.Unrun("a", 2); !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Unrun(a, 2) = %v, want ErrDeliveryParked", err)
	}
	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun of the event whose handlers ran = %v, want nil", err)
	}
}

func TestParkUndelivered_RemembersTheNamedVersionsUntilTheParkLifts(t *testing.T) {
	d := newDurable(t, nil)

	d.Park("a", 3, 5)

	if len(halted(d)) != 1 {
		t.Fatalf("halted = %v, want the aggregate parked", halted(d))
	}
	for _, v := range []int64{3, 5} {
		if err := d.Unrun("a", v); !errors.Is(err, asynxmd.ErrDeliveryParked) {
			t.Errorf("Unrun(a, %d) = %v, want ErrDeliveryParked", v, err)
		}
	}
	if err := d.Unrun("a", 4); err != nil {
		t.Errorf("Unrun(a, 4) = %v, want nil", err)
	}
	resume(d, "a")
	if err := d.Unrun("a", 3); err != nil {
		t.Errorf("Unrun after Resume = %v, want nil", err)
	}
}

// An event whose handlers ran and failed until the attempts ran out is not
// "unrun": a handler took it.
func TestUnrun_DoesNotCountAnEventWhoseHandlersRanAndFailed(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return errors.New("down")
	}}},
		withMaxAttempts[int](1))

	ticket, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ticket.Final
	waitFor(t, "the aggregate to park", func() bool { return len(halted(d)) == 1 })

	if err := d.Unrun("a", 1); err != nil {
		t.Fatalf("Unrun = %v, want nil: the handler ran", err)
	}
}

func TestTicketFor_ForgetsQueuedEventsWhenTheyAreParked(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())
	ack := func(context.Context) error { return nil }

	if _, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), ack); err != nil {
		t.Fatal(err)
	}
	<-bus.entered
	queued, err := enqueueTicket(context.Background(), d, makeEvent("a", 2), ack)
	if err != nil {
		t.Fatal(err)
	}

	d.Park("a")
	<-queued.Final
	if _, found := d.TicketFor("a", 2); found {
		t.Fatal("TicketFor kept a queued event that parking took out of memory")
	}
	close(bus.release)
}

// WaitIdle waits for what was queued when it was called and refuses nothing:
// commands keep enqueueing while it waits.
func TestWaitIdle_DoesNotRefuseEnqueuesWhileItWaits(t *testing.T) {
	bus := &blockingBus{entered: make(chan struct{}, 4), release: make(chan struct{})}
	d := newTestDispatcher[int](bus)
	defer d.Close(context.Background())

	first, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	<-bus.entered

	idle := make(chan struct{})
	go func() { d.WaitIdle(); close(idle) }()

	later, err := enqueueTicket(context.Background(), d, makeEvent("b", 1), nil)
	if err != nil {
		t.Fatalf("an enqueue during WaitIdle was refused: %v", err)
	}

	select {
	case <-idle:
		t.Fatal("WaitIdle returned while an event it should wait for was still being handled")
	default:
	}

	close(bus.release)
	<-idle
	<-first.Final
	<-later.Final
}

func TestWaitIdle_ReturnsAtOnceWhenNothingIsQueued(t *testing.T) {
	d := newTestDispatcher[int](&recordingBus{})
	defer d.Close(context.Background())
	d.WaitIdle()
}
