package bus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/bus"
)

func newBus[T any]() *bus.ChannelBus[T] {
	return bus.NewChannelBus[T]()
}

func ev(name string) asynx.Event[string] {
	return asynx.Event[string]{EventName: name}
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
var _ asynx.Bus[string] = (*bus.ChannelBus[string])(nil)

// --- Subscribe ---

func TestSubscribeEmptyPattern(t *testing.T) {
	b := newBus[string]()
	defer b.Close(context.Background())

	_, err := b.Subscribe("", func(asynx.Event[string]) {})
	if err != asynx.ErrEmptyPattern {
		t.Errorf("expected ErrEmptyPattern, got %v", err)
	}
}

func TestSubscribeNilHandler(t *testing.T) {
	b := newBus[string]()
	defer b.Close(context.Background())

	_, err := b.Subscribe("test", nil)
	if err != asynx.ErrNilHandler {
		t.Errorf("expected ErrNilHandler, got %v", err)
	}
}

func TestSubscribeReturnsUniqueIDs(t *testing.T) {
	b := newBus[string]()
	defer b.Close(context.Background())

	handler := func(asynx.Event[string]) {}

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

	_, err := b.Subscribe("test", func(asynx.Event[string]) {})
	if err != asynx.ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}
}

func TestSubscribeWithFallback(t *testing.T) {
	b := newBus[string]()

	fallbackCalled := make(chan struct{}, 1)
	_, err := b.Subscribe("test",
		func(e asynx.Event[string]) { panic("boom") },
		asynx.WithFallback[string](func(e asynx.Event[string]) {
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
	_, err := b.Subscribe("test",
		func(e asynx.Event[string]) {
			close(started)
			time.Sleep(500 * time.Millisecond)
		},
		asynx.WithHandlerTimeout[string](30*time.Millisecond),
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
	id, _ := b.Subscribe("test", func(asynx.Event[string]) {
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
	defer b.Close(context.Background())

	id, _ := b.Subscribe("test", func(asynx.Event[string]) {})

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
	if err != asynx.ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}
}

func TestPublishNoMatchReturnsNil(t *testing.T) {
	b := newBus[string]()
	defer b.Close(context.Background())

	b.Subscribe("other", func(asynx.Event[string]) {})

	if err := b.Publish(context.Background(), ev("test")); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestPublishExactMatch(t *testing.T) {
	b := newBus[string]()

	called := make(chan struct{}, 1)
	b.Subscribe("OrderPlaced", func(e asynx.Event[string]) {
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
	b.Subscribe("OrderPlaced", func(asynx.Event[string]) {
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
	b.Subscribe("^Order.*", func(asynx.Event[string]) {
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
		b.Subscribe("test", func(asynx.Event[string]) {
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

	received := make(chan asynx.Event[string], 1)
	b.Subscribe("test", func(e asynx.Event[string]) {
		received <- e
	})

	want := asynx.Event[string]{EventName: "test", AggregateID: "agg-42"}
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

// --- Panic recovery ---

func TestHandlerPanicDoesNotBlockOtherHandlers(t *testing.T) {
	b := newBus[string]()

	var normalCalled int32

	b.Subscribe("test", func(asynx.Event[string]) {
		panic("boom")
	})
	b.Subscribe("test", func(asynx.Event[string]) {
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
		func(asynx.Event[string]) { panic("oops") },
		asynx.WithFallback[string](func(asynx.Event[string]) {
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

	received := make(chan asynx.Event[string], 1)
	b.Subscribe("test",
		func(asynx.Event[string]) { panic("oops") },
		asynx.WithFallback[string](func(e asynx.Event[string]) {
			received <- e
		}),
	)

	want := asynx.Event[string]{EventName: "test", AggregateID: "agg-1"}
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
	b.Subscribe("^Order[", func(asynx.Event[string]) { // Invalid regex
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
	b.Subscribe("^(Order|Payment).*", func(asynx.Event[string]) {
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
	b.Subscribe("^Event.*", func(asynx.Event[string]) {
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

// --- Close / shutdown ---

func TestCloseWaitsForHandlers(t *testing.T) {
	b := newBus[string]()

	var handlerDone bool
	b.Subscribe("test", func(asynx.Event[string]) {
		time.Sleep(50 * time.Millisecond)
		handlerDone = true
	})

	b.Publish(context.Background(), ev("test"))
	closeAndWait(t, b)

	if !handlerDone {
		t.Error("Close returned before handler finished")
	}
}

func TestCloseDeadlineExceeded(t *testing.T) {
	b := newBus[string]()

	b.Subscribe("test", func(asynx.Event[string]) {
		time.Sleep(500 * time.Millisecond)
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

	if err := b.Publish(context.Background(), ev("test")); err != asynx.ErrBusClosed {
		t.Errorf("expected ErrBusClosed after Close, got %v", err)
	}
}

func TestCloseBlocksNewSubscribe(t *testing.T) {
	b := newBus[string]()
	b.Close(context.Background())

	if _, err := b.Subscribe("test", func(asynx.Event[string]) {}); err != asynx.ErrBusClosed {
		t.Errorf("expected ErrBusClosed after Close, got %v", err)
	}
}

// --- Concurrency / race detector ---

func TestConcurrentPublishes(t *testing.T) {
	b := newBus[string]()

	var count int32
	b.Subscribe("test", func(asynx.Event[string]) {
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
			b.Subscribe("test", func(asynx.Event[string]) {})
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
		id, _ := b.Subscribe("test", func(asynx.Event[string]) {})
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
