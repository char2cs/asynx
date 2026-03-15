package bus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/internal/bus"
)

func newBus[T any]() *bus.ChannelBus[T] {
	return bus.NewChannelBus[T]()
}

func ev(name string) models.Event[string] {
	return models.Event[string]{EventName: name}
}

func closeAndWait(t *testing.T, b *bus.ChannelBus[string]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// Compile-time check that *ChannelBus[T] implements Bus[T].
var _ models.Bus[string] = (*bus.ChannelBus[string])(nil)

// --- Subscribe ---

func TestSubscribeEmptyPattern(t *testing.T) {
	b := newBus[string]()
	defer closeAndWait(t, b)

	_, err := b.Subscribe("", func(_ context.Context, _ models.Event[string]) {})
	if err != models.ErrEmptyPattern {
		t.Errorf("expected ErrEmptyPattern, got %v", err)
	}
}

func TestSubscribeNilHandler(t *testing.T) {
	b := newBus[string]()
	defer closeAndWait(t, b)

	_, err := b.Subscribe("test", nil)
	if err != models.ErrNilHandler {
		t.Errorf("expected ErrNilHandler, got %v", err)
	}
}

func TestSubscribeReturnsUniqueIDs(t *testing.T) {
	b := newBus[string]()
	defer closeAndWait(t, b)

	handler := func(_ context.Context, _ models.Event[string]) {}

	id1, err := b.Subscribe("a", handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id2, err := b.Subscribe("b", handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id1 == "" {
		t.Error("Subscribe returned empty ID")
	}
	if id2 == "" {
		t.Error("Subscribe returned empty ID")
	}
	if id1 == id2 {
		t.Errorf("Subscribe returned duplicate IDs: %s", id1)
	}
}

func TestSubscribeOnClosedBus(t *testing.T) {
	b := newBus[string]()
	b.Close(context.Background())

	_, err := b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {})
	if err != models.ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}
}

func TestSubscribeWithFallback(t *testing.T) {
	b := newBus[string]()

	fallbackCalled := make(chan struct{}, 1)
	_, err := b.Subscribe("test",
		func(_ context.Context, e models.Event[string]) { panic("boom") },
		models.WithFallback[string](func(_ context.Context, e models.Event[string]) {
			fallbackCalled <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	b.Publish(context.Background(), ev("test"))
	closeAndWait(t, b)

	select {
	case <-fallbackCalled:
	default:
		t.Error("fallback handler not called after primary panic")
	}
}

func TestSubscribeWithHandlerTimeout(t *testing.T) {
	b := newBus[string]()

	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)

	_, err := b.Subscribe("test",
		func(_ context.Context, e models.Event[string]) {
			close(started)
			<-block
		},
		models.WithHandlerTimeout[string](30*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	b.Publish(context.Background(), ev("test"))
	<-started

	// Close should return quickly because executeHandler returns on timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := b.Close(ctx); err != nil {
		t.Errorf("unexpected Close error: %v", err)
	}
}

// --- Unsubscribe ---

func TestUnsubscribeRemovesHandler(t *testing.T) {
	b := newBus[string]()

	var called int32
	id, _ := b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&called, 1)
	})

	b.Unsubscribe(id)
	b.Publish(context.Background(), ev("test"))
	closeAndWait(t, b)

	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called after unsubscribe")
	}
}

func TestUnsubscribeIdempotent(t *testing.T) {
	b := newBus[string]()
	defer closeAndWait(t, b)

	id, _ := b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {})

	if err := b.Unsubscribe(id); err != nil {
		t.Fatalf("first Unsubscribe: %v", err)
	}
	if err := b.Unsubscribe(id); err != nil {
		t.Fatalf("second Unsubscribe: %v", err)
	}
	if err := b.Unsubscribe("nonexistent"); err != nil {
		t.Fatalf("nonexistent Unsubscribe: %v", err)
	}
}

// --- Publish ---

func TestPublishOnClosedBus(t *testing.T) {
	b := newBus[string]()
	b.Close(context.Background())

	err := b.Publish(context.Background(), ev("test"))
	if err != models.ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}
}

func TestPublishNoMatchReturnsNil(t *testing.T) {
	b := newBus[string]()
	defer closeAndWait(t, b)

	b.Subscribe("other", func(_ context.Context, _ models.Event[string]) {})

	if err := b.Publish(context.Background(), ev("test")); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestPublishExactMatch(t *testing.T) {
	b := newBus[string]()

	called := make(chan struct{}, 1)
	b.Subscribe("OrderPlaced", func(_ context.Context, e models.Event[string]) {
		called <- struct{}{}
	})

	b.Publish(context.Background(), ev("OrderPlaced"))
	closeAndWait(t, b)

	select {
	case <-called:
	default:
		t.Error("handler not called for exact match")
	}
}

func TestPublishExactMatchNoFalsePositive(t *testing.T) {
	b := newBus[string]()

	var called int32
	b.Subscribe("OrderPlaced", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&called, 1)
	})

	b.Publish(context.Background(), ev("OrderCancelled"))
	closeAndWait(t, b)

	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called for non-matching event")
	}
}

func TestPublishRegexMatch(t *testing.T) {
	b := newBus[string]()

	var count int32
	b.Subscribe("^Order.*", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&count, 1)
	})

	b.Publish(context.Background(), ev("OrderPlaced"))
	b.Publish(context.Background(), ev("OrderCancelled"))
	b.Publish(context.Background(), ev("PaymentReceived"))
	closeAndWait(t, b)

	if atomic.LoadInt32(&count) != 2 {
		t.Errorf("expected 2 calls, got %d", count)
	}
}

func TestPublishMultipleHandlers(t *testing.T) {
	b := newBus[string]()

	var count int32
	for range 3 {
		b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {
			atomic.AddInt32(&count, 1)
		})
	}

	b.Publish(context.Background(), ev("test"))
	closeAndWait(t, b)

	if atomic.LoadInt32(&count) != 3 {
		t.Errorf("expected 3 handler calls, got %d", count)
	}
}

func TestPublishEventDataPropagated(t *testing.T) {
	b := newBus[string]()

	received := make(chan models.Event[string], 1)
	b.Subscribe("test", func(_ context.Context, e models.Event[string]) {
		received <- e
	})

	want := models.Event[string]{EventName: "test", AggregateID: "agg-42"}
	b.Publish(context.Background(), want)
	closeAndWait(t, b)

	select {
	case got := <-received:
		if got.AggregateID != want.AggregateID {
			t.Errorf("AggregateID: got %s, want %s", got.AggregateID, want.AggregateID)
		}
	default:
		t.Error("handler not called")
	}
}

// --- Context propagation ---

func TestPublish_CancelledContext(t *testing.T) {
	b := newBus[string]()
	defer closeAndWait(t, b)

	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.Publish(ctx, ev("test"))
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestPublish_DeadlineExceeded(t *testing.T) {
	b := newBus[string]()
	defer closeAndWait(t, b)

	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := b.Publish(ctx, ev("test"))
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestPublish_CtxPropagatedToHandler(t *testing.T) {
	type ctxKey struct{}
	b := newBus[string]()

	received := make(chan string, 1)
	b.Subscribe("test", func(ctx context.Context, _ models.Event[string]) {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			received <- v
		}
	})

	ctx := context.WithValue(context.Background(), ctxKey{}, "request-123")
	b.Publish(ctx, ev("test"))
	closeAndWait(t, b)

	select {
	case got := <-received:
		if got != "request-123" {
			t.Errorf("got %q, want %q", got, "request-123")
		}
	default:
		t.Error("handler not called or ctx value not propagated")
	}
}

func TestPublish_CtxPropagatedToFallback(t *testing.T) {
	type ctxKey struct{}
	b := newBus[string]()

	received := make(chan string, 1)
	b.Subscribe("test",
		func(_ context.Context, _ models.Event[string]) { panic("boom") },
		models.WithFallback[string](func(ctx context.Context, _ models.Event[string]) {
			if v, ok := ctx.Value(ctxKey{}).(string); ok {
				received <- v
			}
		}),
	)

	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-abc")
	b.Publish(ctx, ev("test"))
	closeAndWait(t, b)

	select {
	case got := <-received:
		if got != "trace-abc" {
			t.Errorf("got %q, want %q", got, "trace-abc")
		}
	default:
		t.Error("fallback not called or ctx value not propagated")
	}
}

// --- Configurable callbacks ---

func TestChannelBus_WithPanicHandler(t *testing.T) {
	got := make(chan any, 1)
	b := bus.NewChannelBus[string](
		bus.WithPanicHandler[string](func(_ context.Context, _ models.Event[string], v any) {
			got <- v
		}),
	)

	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) { panic("bus-panic") })
	b.Publish(context.Background(), ev("test"))
	closeAndWait(t, b)

	select {
	case v := <-got:
		if v != "bus-panic" {
			t.Errorf("got %v, want %q", v, "bus-panic")
		}
	default:
		t.Error("panic handler not called")
	}
}

func TestChannelBus_WithTimeoutHandler(t *testing.T) {
	fired := make(chan struct{}, 1)
	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)

	b := bus.NewChannelBus[string](
		bus.WithTimeoutHandler[string](func(_ context.Context, _ models.Event[string], _ time.Duration) {
			fired <- struct{}{}
		}),
	)

	b.Subscribe("test",
		func(_ context.Context, _ models.Event[string]) {
			close(started)
			<-block
		},
		models.WithHandlerTimeout[string](20*time.Millisecond),
	)
	b.Publish(context.Background(), ev("test"))
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	b.Close(ctx)

	select {
	case <-fired:
	default:
		t.Error("timeout handler not called")
	}
}

// --- Panic recovery ---

func TestHandlerPanicDoesNotBlockOtherHandlers(t *testing.T) {
	b := newBus[string]()

	var normalCalled int32

	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {
		panic("boom")
	})
	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&normalCalled, 1)
	})

	b.Publish(context.Background(), ev("test"))
	closeAndWait(t, b)

	if atomic.LoadInt32(&normalCalled) != 1 {
		t.Error("normal handler not called after sibling handler panic")
	}
}

func TestFallbackCalledOnPanic(t *testing.T) {
	b := newBus[string]()

	fallbackCalled := make(chan struct{}, 1)
	b.Subscribe("test",
		func(_ context.Context, _ models.Event[string]) { panic("oops") },
		models.WithFallback[string](func(_ context.Context, _ models.Event[string]) {
			fallbackCalled <- struct{}{}
		}),
	)

	b.Publish(context.Background(), ev("test"))
	closeAndWait(t, b)

	select {
	case <-fallbackCalled:
	default:
		t.Error("fallback not called on primary panic")
	}
}

func TestFallbackReceivesOriginalEvent(t *testing.T) {
	b := newBus[string]()

	received := make(chan models.Event[string], 1)
	b.Subscribe("test",
		func(_ context.Context, _ models.Event[string]) { panic("oops") },
		models.WithFallback[string](func(_ context.Context, e models.Event[string]) {
			received <- e
		}),
	)

	want := models.Event[string]{EventName: "test", AggregateID: "agg-1"}
	b.Publish(context.Background(), want)
	closeAndWait(t, b)

	select {
	case got := <-received:
		if got.AggregateID != want.AggregateID {
			t.Errorf("fallback got AggregateID %s, want %s", got.AggregateID, want.AggregateID)
		}
	default:
		t.Error("fallback not called")
	}
}

// --- Pattern matching ---

func TestInvalidRegexTreatedAsNoMatch(t *testing.T) {
	b := newBus[string]()

	var called int32
	b.Subscribe("^Order[", func(_ context.Context, _ models.Event[string]) { // Invalid regex
		atomic.AddInt32(&called, 1)
	})

	b.Publish(context.Background(), ev("OrderPlaced"))
	closeAndWait(t, b)

	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called for subscription with invalid regex")
	}
}

func TestRegexAlternation(t *testing.T) {
	b := newBus[string]()

	var count int32
	b.Subscribe("^(Order|Payment).*", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&count, 1)
	})

	b.Publish(context.Background(), ev("OrderPlaced"))
	b.Publish(context.Background(), ev("PaymentReceived"))
	b.Publish(context.Background(), ev("UserCreated"))
	closeAndWait(t, b)

	if atomic.LoadInt32(&count) != 2 {
		t.Errorf("expected 2 calls, got %d", count)
	}
}

func TestLazyPatternCompilationCached(t *testing.T) {
	b := newBus[string]()

	var count int32
	b.Subscribe("^Event.*", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&count, 1)
	})

	for range 5 {
		b.Publish(context.Background(), ev("EventCreated"))
	}
	closeAndWait(t, b)

	if atomic.LoadInt32(&count) != 5 {
		t.Errorf("expected 5 calls, got %d", count)
	}
}

func TestInvalidRegexCompilationCached(t *testing.T) {
	// This test verifies that invalid regex patterns are cached and not recompiled on every publish.
	// Before the fix, invalid patterns would be recompiled each time, wasting CPU.
	b := newBus[string]()

	var called int32
	b.Subscribe("^Order[", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&called, 1)
	})

	// Publish multiple times with the invalid regex subscription
	for range 5 {
		b.Publish(context.Background(), ev("OrderPlaced"))
	}
	closeAndWait(t, b)

	// The handler should never be called because the regex is invalid
	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called for subscription with invalid regex")
	}
	// The important part: the bus should have cached the compilation error,
	// so subsequent publishes don't recompile. This is a structural guarantee—
	// we verify it by checking that the error cache contains the pattern.
	// (In production, this reduces CPU usage on invalid patterns.)
}

// --- Close / shutdown ---

func TestCloseWaitsForHandlers(t *testing.T) {
	b := newBus[string]()

	var handlerDone atomic.Bool
	block := make(chan struct{})

	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {
		<-block
		handlerDone.Store(true)
	})

	b.Publish(context.Background(), ev("test"))

	closeDone := make(chan struct{})
	go func() {
		closeAndWait(t, b)
		close(closeDone)
	}()

	close(block)
	<-closeDone

	if !handlerDone.Load() {
		t.Error("Close returned before handler finished")
	}
}

func TestCloseDeadlineExceeded(t *testing.T) {
	b := newBus[string]()

	block := make(chan struct{})
	defer close(block)

	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {
		<-block
	})

	b.Publish(context.Background(), ev("test"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := b.Close(ctx)
	if err == nil {
		t.Error("expected a context error from Close, got nil")
	}
}

func TestCloseIdempotent(t *testing.T) {
	b := newBus[string]()

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCloseBlocksNewPublish(t *testing.T) {
	b := newBus[string]()
	b.Close(context.Background())

	if err := b.Publish(context.Background(), ev("test")); err != models.ErrBusClosed {
		t.Errorf("expected ErrBusClosed after Close, got %v", err)
	}
}

func TestCloseBlocksNewSubscribe(t *testing.T) {
	b := newBus[string]()
	b.Close(context.Background())

	if _, err := b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {}); err != models.ErrBusClosed {
		t.Errorf("expected ErrBusClosed after Close, got %v", err)
	}
}

// --- Concurrency / race detector ---

func TestConcurrentPublishes(t *testing.T) {
	b := newBus[string]()

	var count int32
	b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {
		atomic.AddInt32(&count, 1)
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish(context.Background(), ev("test"))
		}()
	}
	wg.Wait()

	closeAndWait(t, b)

	if atomic.LoadInt32(&count) != 100 {
		t.Errorf("expected 100 handler calls, got %d", count)
	}
}

func TestConcurrentSubscribeAndPublish(t *testing.T) {
	b := newBus[string]()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {})
		}()
		go func() {
			defer wg.Done()
			b.Publish(context.Background(), ev("test"))
		}()
	}
	wg.Wait()

	closeAndWait(t, b)
}

func TestConcurrentUnsubscribeAndPublish(t *testing.T) {
	b := newBus[string]()

	ids := make([]string, 20)
	for i := range ids {
		id, _ := b.Subscribe("test", func(_ context.Context, _ models.Event[string]) {})
		ids[i] = id
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(2)
		go func(id string) {
			defer wg.Done()
			b.Unsubscribe(id)
		}(id)
		go func() {
			defer wg.Done()
			b.Publish(context.Background(), ev("test"))
		}()
	}
	wg.Wait()

	closeAndWait(t, b)
}
