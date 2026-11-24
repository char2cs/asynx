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

type ctxKey struct{}

func forwardCtxValue(
	ctx context.Context,
	received chan<- string,
) {
	v, ok := ctx.Value(ctxKey{}).(string)
	if !ok {
		return
	}

	received <- v
}

func unsubscribeJob(
	b models.Bus[string],
	id string,
) func() {
	return func() {
		b.Unsubscribe(id)
	}
}

func closeOnFirst(
	calls *atomic.Int32,
	ch chan struct{},
) {
	if calls.Add(1) != 1 {
		return
	}

	close(ch)
}

func receiveNow[V any](
	ch <-chan V,
) (v V, ok bool) {
	select {
	case v = <-ch:
		return v,
			true
	default:
		return v,
			false
	}
}

func spawn(
	wg *sync.WaitGroup,
	work func(),
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		work()
	}()
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
	subscribeFn := func(
		_ context.Context,
		e models.Event[string],
	) {
		panic("boom")
	}
	withFallbackFn := func(
		_ context.Context,
		e models.Event[string],
	) {
		fallbackCalled <- struct{}{}
	}
	_, err := b.Subscribe(
		"test",
		subscribeFn,
		models.WithFallback[string](withFallbackFn),
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
	subscribeFn := func(
		_ context.Context,
		e models.Event[string],
	) {
		close(started)
		<-block
	}
	_, err := b.Subscribe(
		"test",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&called,
			1,
		)
	}
	id, _ := b.Subscribe(
		"test",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		e models.Event[string],
	) {
		called <- struct{}{}
	}
	b.Subscribe(
		"OrderPlaced",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&called,
			1,
		)
	}
	b.Subscribe(
		"OrderPlaced",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&count,
			1,
		)
	}
	b.Subscribe(
		"^Order.*",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&count,
			1,
		)
	}
	for range 3 {
		b.Subscribe(
			"test",
			subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		e models.Event[string],
	) {
		received <- e
	}
	b.Subscribe(
		"test",
		subscribeFn,
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

	got, gotOK := receiveNow(received)
	if !gotOK {
		t.Error("handler not called")
	}

	if gotOK &&
		got.AggregateID != want.AggregateID {
		t.Errorf(
			"AggregateID: got %s, want %s",
			got.AggregateID,
			want.AggregateID,
		)
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
	b := newBus[string]()

	received := make(
		chan string,
		1,
	)
	subscribeFn := func(
		ctx context.Context,
		_ models.Event[string],
	) {
		forwardCtxValue(
			ctx,
			received,
		)
	}
	b.Subscribe(
		"test",
		subscribeFn,
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

	got, gotOK := receiveNow(received)
	if !gotOK {
		t.Error("handler not called or ctx value not propagated")
	}

	if gotOK &&
		got != "request-123" {
		t.Errorf(
			"got %q, want %q",
			got,
			"request-123",
		)
	}
}

func TestPublish_CtxPropagatedToFallback(
	t *testing.T,
) {
	b := newBus[string]()

	received := make(
		chan string,
		1,
	)
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("boom")
	}
	withFallbackFn := func(
		ctx context.Context,
		_ models.Event[string],
	) {
		forwardCtxValue(
			ctx,
			received,
		)
	}
	b.Subscribe(
		"test",
		subscribeFn,
		models.WithFallback[string](withFallbackFn),
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

	got, gotOK := receiveNow(received)
	if !gotOK {
		t.Error("fallback not called or ctx value not propagated")
	}

	if gotOK &&
		got != "trace-abc" {
		t.Errorf(
			"got %q, want %q",
			got,
			"trace-abc",
		)
	}
}

func TestChannelBus_WithPanicHandler(
	t *testing.T,
) {
	got := make(
		chan any,
		1,
	)
	withPanicHandlerFn := func(
		_ context.Context,
		_ models.Event[string],
		v any,
	) {
		got <- v
	}
	b := bus.New[string](
		bus.WithPanicHandler[string](withPanicHandlerFn),
	)
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("bus-panic")
	}
	b.Subscribe(
		"test",
		subscribeFn,
	)
	b.Publish(
		context.Background(),
		ev("test"),
	)
	closeAndWait(
		t,
		b,
	)

	v, gotOK := receiveNow(got)
	if !gotOK {
		t.Error("panic handler not called")
	}

	if gotOK &&
		v != "bus-panic" {
		t.Errorf(
			"got %v, want %q",
			v,
			"bus-panic",
		)
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
	withTimeoutHandlerFn := func(
		_ context.Context,
		_ models.Event[string],
		_ time.Duration,
	) {
		fired <- struct{}{}
	}
	b := bus.New[string](
		bus.WithTimeoutHandler[string](withTimeoutHandlerFn),
	)
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		close(started)
		<-block
	}
	b.Subscribe(
		"test",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("boom")
	}
	b.Subscribe(
		"test",
		subscribeFn,
	)
	subscribeFn2 := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&normalCalled,
			1,
		)
	}
	b.Subscribe(
		"test",
		subscribeFn2,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("oops")
	}
	withFallbackFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		fallbackCalled <- struct{}{}
	}
	b.Subscribe(
		"test",
		subscribeFn,
		models.WithFallback[string](withFallbackFn),
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("oops")
	}
	withFallbackFn := func(
		_ context.Context,
		e models.Event[string],
	) {
		received <- e
	}
	b.Subscribe(
		"test",
		subscribeFn,
		models.WithFallback[string](withFallbackFn),
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

	got, gotOK := receiveNow(received)
	if !gotOK {
		t.Error("fallback not called")
	}

	if gotOK &&
		got.AggregateID != want.AggregateID {
		t.Errorf(
			"fallback got AggregateID %s, want %s",
			got.AggregateID,
			want.AggregateID,
		)
	}
}

func TestInvalidRegexTreatedAsNoMatch(
	t *testing.T,
) {
	b := newBus[string]()

	var called int32
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&called,
			1,
		)
	}
	b.Subscribe(
		"^Order[",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&count,
			1,
		)
	}
	b.Subscribe(
		"^(Order|Payment).*",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&count,
			1,
		)
	}
	b.Subscribe(
		"^Event.*",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&called,
			1,
		)
	}
	b.Subscribe(
		"^Order[",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		<-block
		handlerDone.Store(true)
	}
	b.Subscribe(
		"test",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		<-block
	}
	b.Subscribe(
		"test",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		atomic.AddInt32(
			&count,
			1,
		)
	}
	b.Subscribe(
		"test",
		subscribeFn,
	)

	var wg sync.WaitGroup
	publishFn := func() {
		b.Publish(
			context.Background(),
			ev("test"),
		)
	}
	for range 100 {
		spawn(
			&wg,
			publishFn,
		)
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
	subscribeFn := func() {
		b.Subscribe(
			"test",
			func(
				_ context.Context,
				_ models.Event[string],
			) {
			},
		)
	}
	publishFn := func() {
		b.Publish(
			context.Background(),
			ev("test"),
		)
	}
	for range 50 {
		spawn(
			&wg,
			subscribeFn,
		)
		spawn(
			&wg,
			publishFn,
		)
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
	publishFn := func() {
		b.Publish(
			context.Background(),
			ev("test"),
		)
	}
	for _, id := range ids {
		spawn(
			&wg,
			unsubscribeJob(
				b,
				id,
			),
		)
		spawn(
			&wg,
			publishFn,
		)
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		close(started)
		<-block
		done.Store(true)
	}
	b.Subscribe(
		"test",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		called <- struct{}{}
	}
	b.Subscribe(
		"OrderPlaced",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		count.Add(1)
	}
	b.Subscribe(
		"^Order.*",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		count.Add(1)
	}
	for range 3 {
		b.Subscribe(
			"test",
			subscribeFn,
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
	withPanicHandlerFn := func(
		_ context.Context,
		_ models.Event[string],
		_ any,
	) {
		panicCaught.Store(true)
	}
	b := bus.New[string](
		bus.WithPanicHandler[string](withPanicHandlerFn),
	)
	defer closeAndWait(
		t,
		b,
	)
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("boom")
	}
	b.Subscribe(
		"test",
		subscribeFn,
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
	subscribeFn := func(
		_ context.Context,
		e models.Event[string],
	) {
		received <- e
	}
	b.Subscribe(
		"test",
		subscribeFn,
	)

	want := models.Event[string]{EventName: "test", AggregateID: "agg-sync-42"}
	b.PublishSync(
		context.Background(),
		want,
	)

	got, gotOK := receiveNow(received)
	if !gotOK {
		t.Error("handler not called")
	}

	if gotOK &&
		got.AggregateID != want.AggregateID {
		t.Errorf(
			"AggregateID: got %s, want %s",
			got.AggregateID,
			want.AggregateID,
		)
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		count.Add(1)
	}
	b.Subscribe(
		"test",
		subscribeFn,
	)

	var wg sync.WaitGroup
	publishSyncFn := func() {
		b.PublishSync(
			context.Background(),
			ev("test"),
		)
	}
	for range 50 {
		spawn(
			&wg,
			publishSyncFn,
		)
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
	ready := make(chan struct{})
	publishWhenReady := func() {
		<-ready
		b.Publish(
			context.Background(),
			ev("order.created"),
		)
	}
	for range goroutines {
		spawn(
			&wg,
			publishWhenReady,
		)
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
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		close(started)
		<-block
		done.Store(true)
	}
	b.Subscribe(
		"test",
		subscribeFn,
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
	withTimeoutHandlerFn := func(
		context.Context,
		models.Event[string],
		time.Duration,
	) {
		closeOnFirst(
			&timeouts,
			timedOut,
		)
	}
	b := bus.New[string](bus.WithTimeoutHandler[string](withTimeoutHandlerFn))
	defer closeAndWait(
		t,
		b,
	)
	subscribeFn := func(
		context.Context,
		models.Event[string],
	) {
		<-timedOut
		finished.Store(true)
	}
	b.Subscribe(
		"slow",
		subscribeFn,
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
	withTimeoutHandlerFn := func(
		context.Context,
		models.Event[string],
		time.Duration,
	) {
		timeouts.Add(1)
		reported <- struct{}{}
	}
	b := bus.New[string](bus.WithTimeoutHandler[string](withTimeoutHandlerFn))
	subscribeFn := func(
		context.Context,
		models.Event[string],
	) {
		<-release
	}
	b.Subscribe(
		"slow",
		subscribeFn,
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
	withPanicHandlerFn := func(
		_ context.Context,
		_ models.Event[string],
		_ any,
	) {
		called <- struct{}{}
		panic("observer panic")
	}
	b := bus.New[string](
		bus.WithPanicHandler[string](withPanicHandlerFn),
	)
	subscribeFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("handler panic")
	}
	b.Subscribe(
		"test",
		subscribeFn,
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

func newBenchBus(
	b *testing.B,
) models.Bus[string] {
	cb := bus.New[string]()
	cleanupFn := func() { cb.Close(context.Background()) }
	b.Cleanup(cleanupFn)

	return cb
}

func subscribeFormatted(
	cb models.Bus[string],
	count int,
	format string,
) {
	for i := range count {
		cb.Subscribe(
			fmt.Sprintf(
				format,
				i,
			),
			noop,
		)
	}
}

func subscribeSame(
	cb models.Bus[string],
	count int,
	pattern string,
) {
	for range count {
		cb.Subscribe(
			pattern,
			noop,
		)
	}
}

func publishLoop(
	b *testing.B,
	cb models.Bus[string],
	eventName string,
) {
	event := models.Event[string]{EventName: eventName}
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

func publishFormattedBench(
	n int,
	format string,
	eventName string,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		cb := newBenchBus(b)
		subscribeFormatted(
			cb,
			n,
			format,
		)
		publishLoop(
			b,
			cb,
			eventName,
		)
	}
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
			publishFormattedBench(
				n,
				"Event%d",
				"Event0",
			),
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
			publishFormattedBench(
				n,
				"Event%d",
				"NoMatch",
			),
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
			publishFormattedBench(
				n,
				"^Event%d.*",
				"Event0Created",
			),
		)
	}
}

func publishFanoutBench(
	n int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		cb := newBenchBus(b)
		subscribeSame(
			cb,
			n,
			"OrderPlaced",
		)
		publishLoop(
			b,
			cb,
			"OrderPlaced",
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
			publishFanoutBench(n),
		)
	}
}

func publishWhileNext(
	pb *testing.PB,
	cb models.Bus[string],
	event models.Event[string],
) {
	for pb.Next() {
		cb.Publish(
			context.Background(),
			event,
		)
	}
}

func BenchmarkPublish_Parallel(
	b *testing.B,
) {
	cb := newBenchBus(b)
	cb.Subscribe(
		"OrderPlaced",
		noop,
	)

	event := models.Event[string]{EventName: "OrderPlaced"}

	b.ResetTimer()
	b.ReportAllocs()
	runParallelFn := func(
		pb *testing.PB,
	) {
		publishWhileNext(
			pb,
			cb,
			event,
		)
	}
	b.RunParallel(runParallelFn)
	b.StopTimer()
}

func publishMixedBench(
	exact int,
	regex int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		cb := newBenchBus(b)
		subscribeFormatted(
			cb,
			exact,
			"Exact%d",
		)
		subscribeFormatted(
			cb,
			regex,
			"^Regex%d.*",
		)
		publishLoop(
			b,
			cb,
			"Exact0",
		)
	}
}

func BenchmarkPublish_MixedSubs(
	b *testing.B,
) {
	for _, exact := range []int{100, 1_000} {
		benchmarkMixedRegexes(
			b,
			exact,
		)
	}
}

func benchmarkMixedRegexes(
	b *testing.B,
	exact int,
) {
	for _, regex := range []int{5, 20} {
		b.Run(
			fmt.Sprintf(
				"%d_exact_%d_regex",
				exact,
				regex,
			),
			publishMixedBench(
				exact,
				regex,
			),
		)
	}
}

func BenchmarkSubscribe(
	b *testing.B,
) {
	cb := bus.New[string]()
	cleanupFn := func() { cb.Close(context.Background()) }
	b.Cleanup(cleanupFn)

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
	cleanupFn := func() { cb.Close(context.Background()) }
	b.Cleanup(cleanupFn)

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
	cleanupFn := func() { cb.Close(context.Background()) }
	b.Cleanup(cleanupFn)

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
	cleanupFn := func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		cb.Close(ctx)
	}
	b.Cleanup(cleanupFn)

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
