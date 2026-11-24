package channel_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/internal/channel"
	"github.com/char2cs/asynx/models"
)

func newBus[T any]() models.Bus[T] {
	return bus.New[T]()
}

func ev(
	name string,
) models.Event[string] {
	return models.Event[string]{EventName: name}
}

func closeAndWait(
	t *testing.T,
	b models.Bus[string],
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf(
			"Close: %v",
			err,
		)
	}
}

var _ models.Bus[string] = (*channel.Bus[string])(nil)

func TestSubscribeEmptyPattern(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	_, err := b.Subscribe(
		"",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)
	if err != models.ErrEmptyPattern {
		t.Errorf(
			"expected ErrEmptyPattern, got %v",
			err,
		)
	}
}

func TestSubscribeNilHandler(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	_, err := b.Subscribe(
		"test",
		nil,
	)
	if err != models.ErrNilHandler {
		t.Errorf(
			"expected ErrNilHandler, got %v",
			err,
		)
	}
}

func TestSubscribeReturnsUniqueIDs(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
	}

	id1, err := b.Subscribe(
		"a",
		handler,
	)
	if err != nil {
		t.Fatalf(
			"unexpected error: %v",
			err,
		)
	}

	id2, err := b.Subscribe(
		"b",
		handler,
	)
	if err != nil {
		t.Fatalf(
			"unexpected error: %v",
			err,
		)
	}

	if id1 == "" {
		t.Error("Subscribe returned empty ID")
	}

	if id2 == "" {
		t.Error("Subscribe returned empty ID")
	}

	if id1 == id2 {
		t.Errorf(
			"Subscribe returned duplicate IDs: %s",
			id1,
		)
	}
}

func TestSubscribeOnClosedBus(
	t *testing.T,
) {
	b := newBus[string]()
	b.Close(context.Background())

	_, err := b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)
	if err != models.ErrBusClosed {
		t.Errorf(
			"expected ErrBusClosed, got %v",
			err,
		)
	}
}

func TestSubscribeWithFallback(
	t *testing.T,
) {
	b := newBus[string]()

	fallbackCalled := make(
		chan struct{},
		1,
	)
	_, err := b.Subscribe(
		"test",
		func(
			_ context.Context,
			e models.Event[string],
		) {
			panic("boom")
		},
		models.WithFallback[string](func(
			_ context.Context,
			e models.Event[string],
		) {
			fallbackCalled <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf(
			"Subscribe: %v",
			err,
		)
	}

	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case <-fallbackCalled:
	default:
		t.Error("fallback handler not called after primary panic")
	}
}

func TestSubscribeWithHandlerTimeout(
	t *testing.T,
) {
	b := newBus[string]()

	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)

	_, err := b.Subscribe(
		"test",
		func(
			_ context.Context,
			e models.Event[string],
		) {
			close(started)
			<-block
		},
		models.WithHandlerTimeout[string](30*time.Millisecond),
	)
	if err != nil {
		t.Fatalf(
			"Subscribe: %v",
			err,
		)
	}

	b.Publish(
		context.Background(),
		ev("test"),
	)
	<-started

	ctx, cancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer cancel()

	if err := b.Close(ctx); err != nil {
		t.Errorf(
			"unexpected Close error: %v",
			err,
		)
	}
}

func TestUnsubscribeRemovesHandler(
	t *testing.T,
) {
	b := newBus[string]()

	var called int32
	id, _ := b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&called,
				1,
			)
		},
	)

	b.Unsubscribe(id)
	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called after unsubscribe")
	}
}

func TestUnsubscribeIdempotent(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	id, _ := b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)

	if err := b.Unsubscribe(id); err != nil {
		t.Fatalf(
			"first Unsubscribe: %v",
			err,
		)
	}

	if err := b.Unsubscribe(id); err != nil {
		t.Fatalf(
			"second Unsubscribe: %v",
			err,
		)
	}

	if err := b.Unsubscribe("nonexistent"); err != nil {
		t.Fatalf(
			"nonexistent Unsubscribe: %v",
			err,
		)
	}
}

func TestPublishOnClosedBus(
	t *testing.T,
) {
	b := newBus[string]()
	b.Close(context.Background())

	err := b.Publish(
		context.Background(),
		ev("test"),
	)
	if err != models.ErrBusClosed {
		t.Errorf(
			"expected ErrBusClosed, got %v",
			err,
		)
	}
}

func TestPublishNoMatchReturnsNil(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	b.Subscribe(
		"other",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)

	if err := b.Publish(
		context.Background(),
		ev("test"),
	); err != nil {
		t.Errorf(
			"expected nil, got %v",
			err,
		)
	}
}

func TestPublishExactMatch(
	t *testing.T,
) {
	b := newBus[string]()

	called := make(
		chan struct{},
		1,
	)
	b.Subscribe(
		"OrderPlaced",
		func(
			_ context.Context,
			e models.Event[string],
		) {
			called <- struct{}{}
		},
	)

	b.Publish(
		context.Background(),
		ev("OrderPlaced"),
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case <-called:
	default:
		t.Error("handler not called for exact match")
	}
}

func TestPublishExactMatchNoFalsePositive(
	t *testing.T,
) {
	b := newBus[string]()

	var called int32
	b.Subscribe(
		"OrderPlaced",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&called,
				1,
			)
		},
	)

	b.Publish(
		context.Background(),
		ev("OrderCancelled"),
	)
	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called for non-matching event")
	}
}

func TestPublishRegexMatch(
	t *testing.T,
) {
	b := newBus[string]()

	var count int32
	b.Subscribe(
		"^Order.*",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&count,
				1,
			)
		},
	)

	b.Publish(
		context.Background(),
		ev("OrderPlaced"),
	)
	b.Publish(
		context.Background(),
		ev("OrderCancelled"),
	)
	b.Publish(
		context.Background(),
		ev("PaymentReceived"),
	)
	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&count) != 2 {
		t.Errorf(
			"expected 2 calls, got %d",
			count,
		)
	}
}

func TestPublishMultipleHandlers(
	t *testing.T,
) {
	b := newBus[string]()

	var count int32
	for range 3 {
		b.Subscribe(
			"test",
			func(
				_ context.Context,
				_ models.Event[string],
			) {
				atomic.AddInt32(
					&count,
					1,
				)
			},
		)
	}

	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&count) != 3 {
		t.Errorf(
			"expected 3 handler calls, got %d",
			count,
		)
	}
}

func TestPublishEventDataPropagated(
	t *testing.T,
) {
	b := newBus[string]()

	received := make(
		chan models.Event[string],
		1,
	)
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			e models.Event[string],
		) {
			received <- e
		},
	)

	want := models.Event[string]{EventName: "test", AggregateID: "agg-42"}
	b.Publish(
		context.Background(),
		want,
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case got := <-received:
		if got.AggregateID != want.AggregateID {
			t.Errorf(
				"AggregateID: got %s, want %s",
				got.AggregateID,
				want.AggregateID,
			)
		}
	default:
		t.Error("handler not called")
	}
}

func TestPublish_CancelledContext(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.Publish(
		ctx,
		ev("test"),
	)
	if err != context.Canceled {
		t.Errorf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

func TestPublish_DeadlineExceeded(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	err := b.Publish(
		ctx,
		ev("test"),
	)
	if err != context.DeadlineExceeded {
		t.Errorf(
			"expected context.DeadlineExceeded, got %v",
			err,
		)
	}
}

func TestPublish_CtxPropagatedToHandler(
	t *testing.T,
) {
	type ctxKey struct{}
	b := newBus[string]()

	received := make(
		chan string,
		1,
	)
	b.Subscribe(
		"test",
		func(
			ctx context.Context,
			_ models.Event[string],
		) {
			if v, ok := ctx.Value(ctxKey{}).(string); ok {
				received <- v
			}
		},
	)

	ctx := context.WithValue(
		context.Background(),
		ctxKey{},
		"request-123",
	)
	b.Publish(
		ctx,
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case got := <-received:
		if got != "request-123" {
			t.Errorf(
				"got %q, want %q",
				got,
				"request-123",
			)
		}
	default:
		t.Error("handler not called or ctx value not propagated")
	}
}

func TestPublish_CtxPropagatedToFallback(
	t *testing.T,
) {
	type ctxKey struct{}
	b := newBus[string]()

	received := make(
		chan string,
		1,
	)
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("boom")
		},
		models.WithFallback[string](func(
			ctx context.Context,
			_ models.Event[string],
		) {
			if v, ok := ctx.Value(ctxKey{}).(string); ok {
				received <- v
			}
		}),
	)

	ctx := context.WithValue(
		context.Background(),
		ctxKey{},
		"trace-abc",
	)
	b.Publish(
		ctx,
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case got := <-received:
		if got != "trace-abc" {
			t.Errorf(
				"got %q, want %q",
				got,
				"trace-abc",
			)
		}
	default:
		t.Error("fallback not called or ctx value not propagated")
	}
}

func TestChannelBus_WithPanicHandler(
	t *testing.T,
) {
	got := make(
		chan any,
		1,
	)
	b := bus.New[string](
		bus.WithPanicHandler[string](func(
			_ context.Context,
			_ models.Event[string],
			v any,
		) {
			got <- v
		}),
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("bus-panic")
		},
	)
	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case v := <-got:
		if v != "bus-panic" {
			t.Errorf(
				"got %v, want %q",
				v,
				"bus-panic",
			)
		}
	default:
		t.Error("panic handler not called")
	}
}

func TestChannelBus_WithTimeoutHandler(
	t *testing.T,
) {
	fired := make(
		chan struct{},
		1,
	)
	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)

	b := bus.New[string](
		bus.WithTimeoutHandler[string](func(
			_ context.Context,
			_ models.Event[string],
			_ time.Duration,
		) {
			fired <- struct{}{}
		}),
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			close(started)
			<-block
		},
		models.WithHandlerTimeout[string](20*time.Millisecond),
	)
	b.Publish(
		context.Background(),
		ev("test"),
	)
	<-started

	ctx, cancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer cancel()
	b.Close(ctx)

	select {
	case <-fired:
	default:
		t.Error("timeout handler not called")
	}
}

func TestHandlerPanicDoesNotBlockOtherHandlers(
	t *testing.T,
) {
	b := newBus[string]()

	var normalCalled int32

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("boom")
		},
	)
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&normalCalled,
				1,
			)
		},
	)

	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&normalCalled) != 1 {
		t.Error("normal handler not called after sibling handler panic")
	}
}

func TestFallbackCalledOnPanic(
	t *testing.T,
) {
	b := newBus[string]()

	fallbackCalled := make(
		chan struct{},
		1,
	)
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("oops")
		},
		models.WithFallback[string](func(
			_ context.Context,
			_ models.Event[string],
		) {
			fallbackCalled <- struct{}{}
		}),
	)

	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case <-fallbackCalled:
	default:
		t.Error("fallback not called on primary panic")
	}
}

func TestFallbackReceivesOriginalEvent(
	t *testing.T,
) {
	b := newBus[string]()

	received := make(
		chan models.Event[string],
		1,
	)
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("oops")
		},
		models.WithFallback[string](func(
			_ context.Context,
			e models.Event[string],
		) {
			received <- e
		}),
	)

	want := models.Event[string]{EventName: "test", AggregateID: "agg-1"}
	b.Publish(
		context.Background(),
		want,
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case got := <-received:
		if got.AggregateID != want.AggregateID {
			t.Errorf(
				"fallback got AggregateID %s, want %s",
				got.AggregateID,
				want.AggregateID,
			)
		}
	default:
		t.Error("fallback not called")
	}
}

func TestInvalidRegexTreatedAsNoMatch(
	t *testing.T,
) {
	b := newBus[string]()

	var called int32
	b.Subscribe(
		"^Order[",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&called,
				1,
			)
		},
	)

	b.Publish(
		context.Background(),
		ev("OrderPlaced"),
	)
	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called for subscription with invalid regex")
	}
}

func TestRegexAlternation(
	t *testing.T,
) {
	b := newBus[string]()

	var count int32
	b.Subscribe(
		"^(Order|Payment).*",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&count,
				1,
			)
		},
	)

	b.Publish(
		context.Background(),
		ev("OrderPlaced"),
	)
	b.Publish(
		context.Background(),
		ev("PaymentReceived"),
	)
	b.Publish(
		context.Background(),
		ev("UserCreated"),
	)
	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&count) != 2 {
		t.Errorf(
			"expected 2 calls, got %d",
			count,
		)
	}
}

func TestLazyPatternCompilationCached(
	t *testing.T,
) {
	b := newBus[string]()

	var count int32
	b.Subscribe(
		"^Event.*",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&count,
				1,
			)
		},
	)

	for range 5 {
		b.Publish(
			context.Background(),
			ev("EventCreated"),
		)
	}

	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&count) != 5 {
		t.Errorf(
			"expected 5 calls, got %d",
			count,
		)
	}
}

func TestInvalidRegexCompilationCached(
	t *testing.T,
) {
	b := newBus[string]()

	var called int32
	b.Subscribe(
		"^Order[",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&called,
				1,
			)
		},
	)

	for range 5 {
		b.Publish(
			context.Background(),
			ev("OrderPlaced"),
		)
	}

	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&called) != 0 {
		t.Error("handler called for subscription with invalid regex")
	}
}

func TestCloseWaitsForHandlers(
	t *testing.T,
) {
	b := newBus[string]()

	var handlerDone atomic.Bool
	block := make(chan struct{})

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			<-block
			handlerDone.Store(true)
		},
	)

	b.Publish(
		context.Background(),
		ev("test"),
	)

	closeDone := make(chan struct{})
	go func() {
		closeAndWait(
			t,
			b,
		)
		close(closeDone)
	}()

	close(block)
	<-closeDone

	if !handlerDone.Load() {
		t.Error("Close returned before handler finished")
	}
}

func TestCloseDeadlineExceeded(
	t *testing.T,
) {
	b := newBus[string]()

	block := make(chan struct{})
	defer close(block)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			<-block
		},
	)

	b.Publish(
		context.Background(),
		ev("test"),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Millisecond,
	)
	defer cancel()

	err := b.Close(ctx)
	if err == nil {
		t.Error("expected a context error from Close, got nil")
	}
}

func TestCloseIdempotent(
	t *testing.T,
) {
	b := newBus[string]()

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf(
			"first Close: %v",
			err,
		)
	}

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf(
			"second Close: %v",
			err,
		)
	}
}

func TestCloseBlocksNewPublish(
	t *testing.T,
) {
	b := newBus[string]()
	b.Close(context.Background())

	if err := b.Publish(
		context.Background(),
		ev("test"),
	); err != models.ErrBusClosed {
		t.Errorf(
			"expected ErrBusClosed after Close, got %v",
			err,
		)
	}
}

func TestCloseBlocksNewSubscribe(
	t *testing.T,
) {
	b := newBus[string]()
	b.Close(context.Background())

	if _, err := b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	); err != models.ErrBusClosed {
		t.Errorf(
			"expected ErrBusClosed after Close, got %v",
			err,
		)
	}
}

func TestConcurrentPublishes(
	t *testing.T,
) {
	b := newBus[string]()

	var count int32
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			atomic.AddInt32(
				&count,
				1,
			)
		},
	)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish(
				context.Background(),
				ev("test"),
			)
		}()
	}

	wg.Wait()

	closeAndWait(
		t,
		b,
	)

	if atomic.LoadInt32(&count) != 100 {
		t.Errorf(
			"expected 100 handler calls, got %d",
			count,
		)
	}
}

func TestConcurrentSubscribeAndPublish(
	t *testing.T,
) {
	b := newBus[string]()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			b.Subscribe(
				"test",
				func(
					_ context.Context,
					_ models.Event[string],
				) {
				},
			)
		}()
		go func() {
			defer wg.Done()
			b.Publish(
				context.Background(),
				ev("test"),
			)
		}()
	}

	wg.Wait()

	closeAndWait(
		t,
		b,
	)
}

func TestConcurrentUnsubscribeAndPublish(
	t *testing.T,
) {
	b := newBus[string]()

	ids := make(
		[]string,
		20,
	)
	for i := range ids {
		id, _ := b.Subscribe(
			"test",
			func(
				_ context.Context,
				_ models.Event[string],
			) {
			},
		)
		ids[i] = id
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(2)
		go func(
			id string,
		) {
			defer wg.Done()
			b.Unsubscribe(id)
		}(id)
		go func() {
			defer wg.Done()
			b.Publish(
				context.Background(),
				ev("test"),
			)
		}()
	}

	wg.Wait()

	closeAndWait(
		t,
		b,
	)
}

func TestPublishSync_NoSubscribers(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	if err := b.PublishSync(
		context.Background(),
		ev("test"),
	); err != nil {
		t.Errorf(
			"expected nil, got %v",
			err,
		)
	}
}

func TestPublishSync_BlocksUntilHandlersDone(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	var done atomic.Bool
	block := make(chan struct{})
	started := make(chan struct{})

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			close(started)
			<-block
			done.Store(true)
		},
	)

	go func() {
		<-started
		close(block)
	}()

	if err := b.PublishSync(
		context.Background(),
		ev("test"),
	); err != nil {
		t.Errorf(
			"unexpected error: %v",
			err,
		)
	}

	if !done.Load() {
		t.Error("PublishSync returned before handler finished")
	}
}

func TestPublishSync_ExactMatch(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	called := make(
		chan struct{},
		1,
	)
	b.Subscribe(
		"OrderPlaced",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			called <- struct{}{}
		},
	)

	b.PublishSync(
		context.Background(),
		ev("OrderPlaced"),
	)

	select {
	case <-called:
	default:
		t.Error("handler not called for exact match")
	}
}

func TestPublishSync_RegexMatch(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	var count atomic.Int32
	b.Subscribe(
		"^Order.*",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			count.Add(1)
		},
	)

	b.PublishSync(
		context.Background(),
		ev("OrderPlaced"),
	)
	b.PublishSync(
		context.Background(),
		ev("OrderCancelled"),
	)
	b.PublishSync(
		context.Background(),
		ev("PaymentReceived"),
	)

	if count.Load() != 2 {
		t.Errorf(
			"expected 2 calls, got %d",
			count.Load(),
		)
	}
}

func TestPublishSync_MultipleHandlers(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	var count atomic.Int32
	for range 3 {
		b.Subscribe(
			"test",
			func(
				_ context.Context,
				_ models.Event[string],
			) {
				count.Add(1)
			},
		)
	}

	b.PublishSync(
		context.Background(),
		ev("test"),
	)

	if count.Load() != 3 {
		t.Errorf(
			"expected 3 calls, got %d",
			count.Load(),
		)
	}
}

func TestPublishSync_OnClosedBus(
	t *testing.T,
) {
	b := newBus[string]()
	b.Close(context.Background())

	err := b.PublishSync(
		context.Background(),
		ev("test"),
	)
	if err != models.ErrBusClosed {
		t.Errorf(
			"expected ErrBusClosed, got %v",
			err,
		)
	}
}

func TestPublishSync_CancelledContext(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.PublishSync(
		ctx,
		ev("test"),
	)
	if err != context.Canceled {
		t.Errorf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

func TestPublishSync_DeadlineExceeded(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	err := b.PublishSync(
		ctx,
		ev("test"),
	)
	if err != context.DeadlineExceeded {
		t.Errorf(
			"expected context.DeadlineExceeded, got %v",
			err,
		)
	}
}

func TestPublishSync_PanicHandledDoesNotBlock(
	t *testing.T,
) {
	var panicCaught atomic.Bool
	b := bus.New[string](
		bus.WithPanicHandler[string](func(
			_ context.Context,
			_ models.Event[string],
			_ any,
		) {
			panicCaught.Store(true)
		}),
	)
	defer closeAndWait(
		t,
		b,
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("boom")
		},
	)

	b.PublishSync(
		context.Background(),
		ev("test"),
	)

	if !panicCaught.Load() {
		t.Error("panic handler not called")
	}
}

func TestPublishSync_EventDataPropagated(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	received := make(
		chan models.Event[string],
		1,
	)
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			e models.Event[string],
		) {
			received <- e
		},
	)

	want := models.Event[string]{EventName: "test", AggregateID: "agg-sync-42"}
	b.PublishSync(
		context.Background(),
		want,
	)

	select {
	case got := <-received:
		if got.AggregateID != want.AggregateID {
			t.Errorf(
				"AggregateID: got %s, want %s",
				got.AggregateID,
				want.AggregateID,
			)
		}
	default:
		t.Error("handler not called")
	}
}

func TestPublishSync_ConcurrentCalls(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	var count atomic.Int32
	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			count.Add(1)
		},
	)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.PublishSync(
				context.Background(),
				ev("test"),
			)
		}()
	}

	wg.Wait()

	if count.Load() != 50 {
		t.Errorf(
			"expected 50 handler calls, got %d",
			count.Load(),
		)
	}
}

func TestGetCompiledPattern_ConcurrentSamePattern(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	const pattern = "order\\..*"
	const goroutines = 100

	b.Subscribe(
		pattern,
		func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	ready := make(chan struct{})
	for i := range goroutines {
		go func(
			i int,
		) {
			defer wg.Done()
			<-ready
			b.Publish(
				context.Background(),
				ev("order.created"),
			)
		}(i)
	}

	close(ready)
	wg.Wait()
}

func TestWaitForHandlers_BlocksUntilDone(
	t *testing.T,
) {
	b := newBus[string]()
	defer closeAndWait(
		t,
		b,
	)

	var done atomic.Bool
	block := make(chan struct{})
	started := make(chan struct{})

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			close(started)
			<-block
			done.Store(true)
		},
	)

	b.Publish(
		context.Background(),
		ev("test"),
	)

	go func() {
		<-started
		close(block)
	}()

	b.WaitForHandlers()

	if !done.Load() {
		t.Error("WaitForHandlers returned before handler finished")
	}
}

func TestPublishSync_WaitsForATimedOutHandler(
	t *testing.T,
) {
	var timeouts atomic.Int32
	var finished atomic.Bool
	timedOut := make(chan struct{})
	b := bus.New[string](bus.WithTimeoutHandler[string](
		func(
			context.Context,
			models.Event[string],
			time.Duration,
		) {
			if timeouts.Add(1) == 1 {
				close(timedOut)
			}
		}))
	defer closeAndWait(
		t,
		b,
	)

	b.Subscribe(
		"slow",
		func(
			context.Context,
			models.Event[string],
		) {
			<-timedOut
			finished.Store(true)
		},
		models.WithHandlerTimeout[string](20*time.Millisecond),
	)

	if err := b.PublishSync(
		context.Background(),
		ev("slow"),
	); err != nil {
		t.Fatal(err)
	}

	if !finished.Load() {
		t.Fatal("PublishSync returned while the timed-out handler was still running")
	}

	if timeouts.Load() != 1 {
		t.Fatalf(
			"timeouts reported = %d, want 1",
			timeouts.Load(),
		)
	}
}

func TestPublish_StillAbandonsATimedOutHandler(
	t *testing.T,
) {
	var timeouts atomic.Int32
	release := make(chan struct{})
	reported := make(
		chan struct{},
		1,
	)
	b := bus.New[string](bus.WithTimeoutHandler[string](
		func(
			context.Context,
			models.Event[string],
			time.Duration,
		) {
			timeouts.Add(1)
			reported <- struct{}{}
		}))

	b.Subscribe(
		"slow",
		func(
			context.Context,
			models.Event[string],
		) {
			<-release
		},
		models.WithHandlerTimeout[string](20*time.Millisecond),
	)

	if err := b.Publish(
		context.Background(),
		ev("slow"),
	); err != nil {
		t.Fatal(err)
	}

	<-reported
	if timeouts.Load() != 1 {
		t.Fatalf(
			"timeouts reported = %d, want 1",
			timeouts.Load(),
		)
	}

	close(release)
	closeAndWait(
		t,
		b,
	)
}

func TestChannelBus_APanicHandlerThatPanicsDoesNotCrashTheProcess(
	t *testing.T,
) {
	called := make(
		chan struct{},
		1,
	)
	b := bus.New[string](
		bus.WithPanicHandler[string](func(
			_ context.Context,
			_ models.Event[string],
			_ any,
		) {
			called <- struct{}{}
			panic("observer panic")
		}),
	)

	b.Subscribe(
		"test",
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("handler panic")
		},
	)
	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	select {
	case <-called:
	default:
		t.Fatal("the panic handler was not called")
	}
}

var noop = func(
	_ context.Context,
	_ models.Event[string],
) {
}

func BenchmarkPublish_ExactMatch(
	b *testing.B,
) {
	for _, n := range []int{1, 10, 100, 1_000, 10_000} {
		b.Run(
			fmt.Sprintf(
				"%d_subs",
				n,
			),
			func(
				b *testing.B,
			) {
				cb := bus.New[string]()
				b.Cleanup(func() { cb.Close(context.Background()) })

				for i := range n {
					cb.Subscribe(
						fmt.Sprintf(
							"Event%d",
							i,
						),
						noop,
					)
				}

				event := models.Event[string]{EventName: "Event0"}
				ctx := context.Background()

				b.ResetTimer()
				b.ReportAllocs()

				for range b.N {
					cb.Publish(
						ctx,
						event,
					)
				}

				b.StopTimer()
			},
		)
	}
}

func BenchmarkPublish_NoMatch(
	b *testing.B,
) {
	for _, n := range []int{1, 100, 1_000, 10_000} {
		b.Run(
			fmt.Sprintf(
				"%d_subs",
				n,
			),
			func(
				b *testing.B,
			) {
				cb := bus.New[string]()
				b.Cleanup(func() { cb.Close(context.Background()) })

				for i := range n {
					cb.Subscribe(
						fmt.Sprintf(
							"Event%d",
							i,
						),
						noop,
					)
				}

				event := models.Event[string]{EventName: "NoMatch"}
				ctx := context.Background()

				b.ResetTimer()
				b.ReportAllocs()

				for range b.N {
					cb.Publish(
						ctx,
						event,
					)
				}

				b.StopTimer()
			},
		)
	}
}

func BenchmarkPublish_Regex(
	b *testing.B,
) {
	for _, n := range []int{1, 10, 100} {
		b.Run(
			fmt.Sprintf(
				"%d_patterns",
				n,
			),
			func(
				b *testing.B,
			) {
				cb := bus.New[string]()
				b.Cleanup(func() { cb.Close(context.Background()) })

				for i := range n {
					cb.Subscribe(
						fmt.Sprintf(
							"^Event%d.*",
							i,
						),
						noop,
					)
				}

				event := models.Event[string]{EventName: "Event0Created"}
				ctx := context.Background()

				b.ResetTimer()
				b.ReportAllocs()

				for range b.N {
					cb.Publish(
						ctx,
						event,
					)
				}

				b.StopTimer()
			},
		)
	}
}

func BenchmarkPublish_Fanout(
	b *testing.B,
) {
	for _, n := range []int{1, 10, 50, 100} {
		b.Run(
			fmt.Sprintf(
				"%d_handlers",
				n,
			),
			func(
				b *testing.B,
			) {
				cb := bus.New[string]()
				b.Cleanup(func() { cb.Close(context.Background()) })

				for range n {
					cb.Subscribe(
						"OrderPlaced",
						noop,
					)
				}

				event := models.Event[string]{EventName: "OrderPlaced"}
				ctx := context.Background()

				b.ResetTimer()
				b.ReportAllocs()

				for range b.N {
					cb.Publish(
						ctx,
						event,
					)
				}

				b.StopTimer()
			},
		)
	}
}

func BenchmarkPublish_Parallel(
	b *testing.B,
) {
	cb := bus.New[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	cb.Subscribe(
		"OrderPlaced",
		noop,
	)

	event := models.Event[string]{EventName: "OrderPlaced"}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(
		pb *testing.PB,
	) {
		for pb.Next() {
			cb.Publish(
				ctx,
				event,
			)
		}
	})
	b.StopTimer()
}

func BenchmarkPublish_MixedSubs(
	b *testing.B,
) {
	for _, exact := range []int{100, 1_000} {
		for _, regex := range []int{5, 20} {
			b.Run(
				fmt.Sprintf(
					"%d_exact_%d_regex",
					exact,
					regex,
				),
				func(
					b *testing.B,
				) {
					cb := bus.New[string]()
					b.Cleanup(func() { cb.Close(context.Background()) })

					for i := range exact {
						cb.Subscribe(
							fmt.Sprintf(
								"Exact%d",
								i,
							),
							noop,
						)
					}

					for i := range regex {
						cb.Subscribe(
							fmt.Sprintf(
								"^Regex%d.*",
								i,
							),
							noop,
						)
					}

					event := models.Event[string]{EventName: "Exact0"}
					ctx := context.Background()

					b.ResetTimer()
					b.ReportAllocs()

					for range b.N {
						cb.Publish(
							ctx,
							event,
						)
					}

					b.StopTimer()
				},
			)
		}
	}
}

func BenchmarkSubscribe(
	b *testing.B,
) {
	cb := bus.New[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		cb.Subscribe(
			"OrderPlaced",
			noop,
		)
	}
}

func BenchmarkSubscribe_Regex(
	b *testing.B,
) {
	cb := bus.New[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		cb.Subscribe(
			"^Order.*",
			noop,
		)
	}
}

func BenchmarkUnsubscribe(
	b *testing.B,
) {
	cb := bus.New[string]()
	b.Cleanup(func() { cb.Close(context.Background()) })

	ids := make(
		[]string,
		b.N,
	)
	for i := range b.N {
		ids[i], _ = cb.Subscribe(
			"OrderPlaced",
			noop,
		)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		cb.Unsubscribe(ids[i])
	}
}

func BenchmarkPublish_Timeout(
	b *testing.B,
) {
	cb := bus.New[string]()
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		cb.Close(ctx)
	})

	cb.Subscribe(
		"OrderPlaced",
		noop,
		models.WithHandlerTimeout[string](time.Second),
	)

	event := models.Event[string]{EventName: "OrderPlaced"}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		cb.Publish(
			ctx,
			event,
		)
	}

	b.StopTimer()
}
