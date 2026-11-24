package asynx_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/v2"
	"github.com/char2cs/asynx/v2/internal/bus"
	"github.com/char2cs/asynx/v2/internal/mocks"
	"github.com/char2cs/asynx/v2/models/command"
	"github.com/char2cs/asynx/v2/models/dispatch"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/handler"
	"github.com/char2cs/asynx/v2/models/pubsub"
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/store"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type publishWaiter interface {
	WaitPublish()
}

func waitPublish(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
) {
	t.Helper()
	instance.(publishWaiter).WaitPublish()
}

type shutdownHook struct {
	t        *testing.T
	instance asynx.Asynx[mocks.Order]
}

func (h shutdownHook) run() {
	err := h.instance.Shutdown(context.Background())
	if err != nil &&
		!errors.Is(
			err,
			failure.ErrAlreadyShuttingDown,
		) {
		h.t.Errorf(
			"Shutdown: %v",
			err,
		)
	}
}

func newInstance(
	t *testing.T,
) asynx.Asynx[mocks.Order] {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 1000}).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(shutdownHook{t: t, instance: instance}.run)

	return instance
}

func TestSend_Success(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send failed: %v",
			err,
		)
	}
}

func TestSend_ValidationFails(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: -5.0}
	_, err := instance.Send(
		context.Background(),
		cmd,
	)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}

	if err != failure.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			err,
		)
	}
}

func TestSend_ContextCancelled(
	t *testing.T,
) {
	instance := newInstance(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(
		ctx,
		cmd,
	)
	if err == nil {
		t.Fatal("expected context cancelled error, got nil")
	}

	if err != failure.ErrContextCancelled {
		t.Fatalf(
			"expected ErrContextCancelled, got %v",
			err,
		)
	}
}

func TestSend_AfterShutdown(
	t *testing.T,
) {
	instance := newInstance(t)
	instance.Shutdown(context.Background())
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(
		context.Background(),
		cmd,
	)
	if err == nil {
		t.Fatal("expected shutdown error, got nil")
	}

	if err != failure.ErrShuttingDown {
		t.Fatalf(
			"expected ErrShuttingDown, got %v",
			err,
		)
	}
}

func TestGet_NotFound(
	t *testing.T,
) {
	instance := newInstance(t)
	_, err := instance.Get(
		context.Background(),
		"nonexistent",
	)
	if err == nil {
		t.Fatal("expected ErrNotFound, got nil")
	}

	if err != failure.ErrNotFound {
		t.Fatalf(
			"expected ErrNotFound, got %v",
			err,
		)
	}
}

func TestGet_Success(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	state, err := instance.Get(
		context.Background(),
		"order-1",
	)
	if err != nil {
		t.Fatalf(
			"Get failed: %v",
			err,
		)
	}

	if state.ID != "order-1" {
		t.Fatalf(
			"expected order-1, got %s",
			state.ID,
		)
	}

	if state.Total != 100.0 {
		t.Fatalf(
			"expected total 100.0, got %f",
			state.Total,
		)
	}
}

func TestExists_False(
	t *testing.T,
) {
	instance := newInstance(t)
	exists, err := instance.Exists(
		context.Background(),
		"nonexistent",
	)
	if err != nil {
		t.Fatalf(
			"Exists failed: %v",
			err,
		)
	}

	if exists {
		t.Fatal("expected false, got true")
	}
}

func TestExists_True(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	exists, err := instance.Exists(
		context.Background(),
		"order-1",
	)
	if err != nil {
		t.Fatalf(
			"Exists failed: %v",
			err,
		)
	}

	if !exists {
		t.Fatal("expected true, got false")
	}
}

func TestPreload_NotFoundIsNoop(
	t *testing.T,
) {
	instance := newInstance(t)
	err := instance.Preload(
		context.Background(),
		"nonexistent",
	)
	if err != nil {
		t.Fatalf(
			"Preload failed: %v",
			err,
		)
	}
}

func TestPreload_ExistingAggregate(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	err := instance.Preload(
		context.Background(),
		"order-1",
	)
	if err != nil {
		t.Fatalf(
			"Preload failed: %v",
			err,
		)
	}
}

func TestSubscribe_EmptyPattern(
	t *testing.T,
) {
	instance := newInstance(t)
	handler := func(
		ctx context.Context,
		evt event.Event[mocks.Order],
	) {
	}
	_, err := instance.Subscribe(
		"",
		handler,
	)
	if err == nil {
		t.Fatal("expected ErrEmptyPattern, got nil")
	}

	if err != failure.ErrEmptyPattern {
		t.Fatalf(
			"expected ErrEmptyPattern, got %v",
			err,
		)
	}
}

func TestSubscribe_NilHandler(
	t *testing.T,
) {
	instance := newInstance(t)
	var nilHandler handler.Projection[mocks.Order]
	_, err := instance.Subscribe(
		"OrderCreated",
		nilHandler,
	)
	if err == nil {
		t.Fatal("expected ErrNilHandler, got nil")
	}

	if err != failure.ErrNilHandler {
		t.Fatalf(
			"expected ErrNilHandler, got %v",
			err,
		)
	}
}

func TestSubscribe_Success(
	t *testing.T,
) {
	instance := newInstance(t)
	handler := func(
		ctx context.Context,
		evt event.Event[mocks.Order],
	) {
	}
	id, err := instance.Subscribe(
		"OrderCreated",
		handler,
	)
	if err != nil {
		t.Fatalf(
			"Subscribe failed: %v",
			err,
		)
	}

	if id == "" {
		t.Fatal("expected non-empty subscription ID")
	}
}

func TestUnsubscribe_Success(
	t *testing.T,
) {
	instance := newInstance(t)
	handler := func(
		ctx context.Context,
		evt event.Event[mocks.Order],
	) {
	}
	id, err := instance.Subscribe(
		"OrderCreated",
		handler,
	)
	if err != nil {
		t.Fatalf(
			"Subscribe failed: %v",
			err,
		)
	}

	err = instance.Unsubscribe(id)
	if err != nil {
		t.Fatalf(
			"Unsubscribe failed: %v",
			err,
		)
	}
}

func TestUnsubscribe_UnknownID(
	t *testing.T,
) {
	instance := newInstance(t)
	err := instance.Unsubscribe("unknown-id")
	if err != nil {
		t.Fatalf(
			"expected nil for unknown subscription, got %v",
			err,
		)
	}
}

func TestReplay_Success(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	var replayed callTally
	err := instance.Replay(
		context.Background(),
		"order-1",
		1,
		0,
		replayed.countEvent,
	)
	if err != nil {
		t.Fatalf(
			"Replay failed: %v",
			err,
		)
	}

	if replayed.total() == 0 {
		t.Fatal("expected at least one event, got 0")
	}
}

func TestReplay_EmptyRange(
	t *testing.T,
) {
	instance := newInstance(t)
	var replayed callTally
	err := instance.Replay(
		context.Background(),
		"nonexistent",
		1,
		0,
		replayed.countEvent,
	)
	if err != nil {
		t.Fatalf(
			"Replay failed: %v",
			err,
		)
	}

	if replayed.total() != 0 {
		t.Fatalf(
			"expected 0 events for nonexistent aggregate, got %d",
			replayed.total(),
		)
	}
}

func TestShutdown_Success(
	t *testing.T,
) {
	instance := newInstance(t)
	err := instance.Shutdown(context.Background())
	if err != nil {
		t.Fatalf(
			"Shutdown failed: %v",
			err,
		)
	}
}

func TestShutdown_Twice(
	t *testing.T,
) {
	instance := newInstance(t)
	instance.Shutdown(context.Background())
	err := instance.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected ErrAlreadyShuttingDown, got nil")
	}

	if err != failure.ErrAlreadyShuttingDown {
		t.Fatalf(
			"expected ErrAlreadyShuttingDown, got %v",
			err,
		)
	}
}

func TestIntegration_FullCommandCycle(
	t *testing.T,
) {
	instance := newInstance(t)

	var handled callTally

	subID, err := instance.Subscribe(
		"OrderCreated",
		handled.countEvent,
	)
	if err != nil {
		t.Fatalf(
			"Subscribe failed: %v",
			err,
		)
	}

	defer instance.Unsubscribe(subID)

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err = instance.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send failed: %v",
			err,
		)
	}

	waitPublish(
		t,
		instance,
	)

	state, err := instance.Get(
		context.Background(),
		"order-1",
	)
	if err != nil {
		t.Fatalf(
			"Get failed: %v",
			err,
		)
	}

	if state.Total != 100.0 {
		t.Fatalf(
			"expected total 100.0, got %f",
			state.Total,
		)
	}

	if handled.total() == 0 {
		t.Fatal("handler was not called")
	}
}

func sendOrderUpdate(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	orderID string,
	i int,
) {
	t.Helper()

	newTotal := float64((i + 2) * 100.0)
	cmd := mocks.UpdateOrderCmd{
		ID:       orderID,
		NewState: mocks.Order{ID: orderID, Total: newTotal, Status: "Pending"},
	}
	_, err := instance.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send update %d failed: %v",
			i,
			err,
		)
	}
}

func TestIntegration_SerialOrderingForSameAggregate(
	t *testing.T,
) {
	instance := newInstance(t)

	orderID := "order-1"
	cmd1 := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
	instance.Send(
		context.Background(),
		cmd1,
	)

	for i := 0; i < 5; i++ {
		sendOrderUpdate(
			t,
			instance,
			orderID,
			i,
		)
	}

	state, err := instance.Get(
		context.Background(),
		orderID,
	)
	if err != nil {
		t.Fatalf(
			"Get failed: %v",
			err,
		)
	}

	expected := 600.0
	if state.Total != expected {
		t.Fatalf(
			"expected final total %f, got %f",
			expected,
			state.Total,
		)
	}
}

func sendCreate(
	instance asynx.Asynx[mocks.Order],
	wg *sync.WaitGroup,
	errChan chan<- error,
	idx int,
) {
	defer wg.Done()
	orderID := fmt.Sprintf(
		"order-%d",
		idx,
	)
	cmd := mocks.CreateOrderCmd{ID: orderID, Total: float64((idx + 1) * 100.0)}
	_, err := instance.Send(
		context.Background(),
		cmd,
	)
	errChan <- err
}

func requireNoErr(
	t *testing.T,
	err error,
	what string,
) {
	t.Helper()

	if err != nil {
		t.Fatalf(
			"%s failed: %v",
			what,
			err,
		)
	}
}

func requireExists(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	orderID string,
) {
	t.Helper()

	exists, err := instance.Exists(
		context.Background(),
		orderID,
	)
	requireNoErr(
		t,
		err,
		"Exists",
	)

	if !exists {
		t.Fatalf(
			"aggregate %s not found",
			orderID,
		)
	}
}

func TestIntegration_ConcurrentCommandsDifferentAggregates(
	t *testing.T,
) {
	instance := newInstance(t)

	numGoroutines := 10
	var wg sync.WaitGroup
	errChan := make(
		chan error,
		numGoroutines,
	)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go sendCreate(
			instance,
			&wg,
			errChan,
			i,
		)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		requireNoErr(
			t,
			err,
			"Send",
		)
	}

	for i := 0; i < numGoroutines; i++ {
		requireExists(
			t,
			instance,
			fmt.Sprintf(
				"order-%d",
				i,
			),
		)
	}
}

func counterHandler(
	counter *atomic.Int32,
) func(context.Context, event.Event[mocks.Order]) {
	return func(
		context.Context,
		event.Event[mocks.Order],
	) {
		counter.Add(1)
	}
}

func subscribeCreated(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	i int,
	h func(context.Context, event.Event[mocks.Order]),
) string {
	t.Helper()

	id, err := instance.Subscribe(
		"OrderCreated",
		h,
	)
	if err != nil {
		t.Fatalf(
			"Subscribe %d failed: %v",
			i,
			err,
		)
	}

	return id
}

func unsubscribeAll(
	instance asynx.Asynx[mocks.Order],
	ids []string,
) {
	for _, id := range ids {
		instance.Unsubscribe(id)
	}
}

func requireCalled(
	t *testing.T,
	counter *atomic.Int32,
	i int,
) {
	t.Helper()

	if counter.Load() == 0 {
		t.Fatalf(
			"handler %d was not called",
			i,
		)
	}
}

func TestIntegration_MultipleProjections(
	t *testing.T,
) {
	instance := newInstance(t)

	counters := [4]atomic.Int32{}
	ids := make(
		[]string,
		4,
	)
	for i := range counters {
		ids[i] = subscribeCreated(
			t,
			instance,
			i,
			counterHandler(&counters[i]),
		)
	}

	defer unsubscribeAll(
		instance,
		ids,
	)

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send failed: %v",
			err,
		)
	}

	waitPublish(
		t,
		instance,
	)

	for i := 0; i < len(counters); i++ {
		requireCalled(
			t,
			&counters[i],
			i,
		)
	}
}

func TestIntegration_GracefulShutdown(
	t *testing.T,
) {
	instance := newInstance(t)

	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	_, err := instance.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send before shutdown failed: %v",
			err,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	err = instance.Shutdown(ctx)
	if err != nil {
		t.Fatalf(
			"Shutdown failed: %v",
			err,
		)
	}

	cmd2 := mocks.CreateOrderCmd{ID: "order-2", Total: 200.0}
	_, err = instance.Send(
		context.Background(),
		cmd2,
	)
	if err == nil {
		t.Fatal("expected error after shutdown, got nil")
	}

	if err != failure.ErrShuttingDown {
		t.Fatalf(
			"expected ErrShuttingDown, got %v",
			err,
		)
	}
}

func TestSendWait_Success(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "ew-order", Total: 55.0}

	event, err := instance.SendWait(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"SendWait failed: %v",
			err,
		)
	}

	if event.AggregateID != "ew-order" {
		t.Errorf(
			"expected AggregateID ew-order, got %q",
			event.AggregateID,
		)
	}

	if event.EventName == "" {
		t.Error("expected non-empty EventName")
	}

	if event.Version == 0 {
		t.Error("expected non-zero Version")
	}
}

func TestSendWait_ReturnsNewAndPreviousAggregate(
	t *testing.T,
) {
	instance := newInstance(t)

	cmd1 := mocks.CreateOrderCmd{ID: "ew-agg", Total: 100.0}
	_, err := instance.SendWait(
		context.Background(),
		cmd1,
	)
	if err != nil {
		t.Fatalf(
			"first SendWait failed: %v",
			err,
		)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "ew-agg",
		NewState: mocks.Order{ID: "ew-agg", Total: 200.0, Status: "Updated"},
	}
	event, err := instance.SendWait(
		context.Background(),
		cmd2,
	)
	if err != nil {
		t.Fatalf(
			"second SendWait failed: %v",
			err,
		)
	}

	if event.Aggregate.Total != 200.0 {
		t.Errorf(
			"expected new aggregate Total 200.0, got %f",
			event.Aggregate.Total,
		)
	}

	if event.PreviousAggregate.Total != 100.0 {
		t.Errorf(
			"expected previous aggregate Total 100.0, got %f",
			event.PreviousAggregate.Total,
		)
	}
}

func TestSendWait_HandlersCompleteBeforeReturn(
	t *testing.T,
) {
	instance := newInstance(t)

	var handlerDone atomic.Int32
	instance.Subscribe(
		"OrderCreated",
		counterHandler(&handlerDone),
	)

	cmd := mocks.CreateOrderCmd{ID: "ew-handler", Total: 10.0}
	_, err := instance.SendWait(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"SendWait failed: %v",
			err,
		)
	}

	if handlerDone.Load() == 0 {
		t.Error("handler not complete when SendWait returned")
	}
}

func TestSendWait_MultipleHandlersAllComplete(
	t *testing.T,
) {
	instance := newInstance(t)

	var count atomic.Int32
	for range 3 {
		instance.Subscribe(
			"OrderCreated",
			counterHandler(&count),
		)
	}

	cmd := mocks.CreateOrderCmd{ID: "ew-multi", Total: 10.0}
	_, err := instance.SendWait(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"SendWait failed: %v",
			err,
		)
	}

	if count.Load() != 3 {
		t.Errorf(
			"expected 3 handler calls, got %d",
			count.Load(),
		)
	}
}

func TestSendWait_ValidationFails(
	t *testing.T,
) {
	instance := newInstance(t)
	cmd := mocks.CreateOrderCmd{ID: "ew-bad", Total: -1.0}

	_, err := instance.SendWait(
		context.Background(),
		cmd,
	)
	if err != failure.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			err,
		)
	}
}

func TestSendWait_ContextCancelled(
	t *testing.T,
) {
	instance := newInstance(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "ew-ctx", Total: 10.0}
	_, err := instance.SendWait(
		ctx,
		cmd,
	)
	if err != failure.ErrContextCancelled {
		t.Fatalf(
			"expected ErrContextCancelled, got %v",
			err,
		)
	}
}

func TestSendWait_AfterShutdown(
	t *testing.T,
) {
	instance := newInstance(t)
	instance.Shutdown(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "ew-shutdown", Total: 10.0}
	_, err := instance.SendWait(
		context.Background(),
		cmd,
	)
	if err != failure.ErrShuttingDown {
		t.Fatalf(
			"expected ErrShuttingDown, got %v",
			err,
		)
	}
}

func TestIntegration_SendWaitThenGet(
	t *testing.T,
) {
	instance := newInstance(t)

	cmd := mocks.CreateOrderCmd{ID: "ew-get", Total: 77.0}
	event, err := instance.SendWait(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"SendWait failed: %v",
			err,
		)
	}

	state, err := instance.Get(
		context.Background(),
		"ew-get",
	)
	if err != nil {
		t.Fatalf(
			"Get failed: %v",
			err,
		)
	}

	if state.Total != 77.0 {
		t.Errorf(
			"expected Total 77.0, got %f",
			state.Total,
		)
	}

	if event.Aggregate.Total != state.Total {
		t.Errorf(
			"event aggregate (%f) does not match store (%f)",
			event.Aggregate.Total,
			state.Total,
		)
	}
}

func TestForget_AggregateDoesNotExist_ReturnsErrValidation(
	t *testing.T,
) {
	instance := newInstance(t)
	err := instance.Forget(
		context.Background(),
		"order-missing",
	)
	if !errors.Is(
		err,
		failure.ErrValidation,
	) {
		t.Fatalf(
			"expected ErrValidation, got %v",
			err,
		)
	}
}

func TestForget_ErasesAggregate(
	t *testing.T,
) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "order-1", Total: 100.0},
	); err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	if err := instance.Forget(
		ctx,
		"order-1",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	_, err := instance.Get(
		ctx,
		"order-1",
	)
	if !errors.Is(
		err,
		failure.ErrNotFound,
	) {
		t.Fatalf(
			"expected ErrNotFound after Forget, got %v",
			err,
		)
	}
}

func TestForget_CallsOnForgetHandler_WithLastState(
	t *testing.T,
) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "order-1", Total: 99.0},
	); err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	var gotEvent event.Event[mocks.Order]
	if _, err := instance.OnForget(func(
		_ context.Context,
		e event.Event[mocks.Order],
	) {
		gotEvent = e
	}); err != nil {
		t.Fatalf(
			"OnForget: %v",
			err,
		)
	}

	if err := instance.Forget(
		ctx,
		"order-1",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	if gotEvent.AggregateID != "order-1" {
		t.Errorf(
			"AggregateID = %q, want order-1",
			gotEvent.AggregateID,
		)
	}

	if gotEvent.EventName != "asynx.aggregate.forget" {
		t.Errorf(
			"EventName = %q, want asynx.aggregate.forget",
			gotEvent.EventName,
		)
	}

	if gotEvent.Aggregate.Total != 99.0 {
		t.Errorf(
			"Aggregate.Total = %v, want 99.0",
			gotEvent.Aggregate.Total,
		)
	}
}

func TestForget_AfterShutdown_ReturnsErrShuttingDown(
	t *testing.T,
) {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().WithEventStore(s).WithSnapshotStore(store.NewSnapshots()).Build()
	if err != nil {
		t.Fatal(err)
	}

	instance.Shutdown(context.Background())

	if err := instance.Forget(
		context.Background(),
		"order-1",
	); !errors.Is(
		err,
		failure.ErrShuttingDown,
	) {
		t.Fatalf(
			"expected ErrShuttingDown, got %v",
			err,
		)
	}
}

func TestForget_SerializedAfterSend(
	t *testing.T,
) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "order-1", Total: 100.0},
	); err != nil {
		t.Fatalf(
			"initial Send: %v",
			err,
		)
	}

	updated := mocks.Order{ID: "order-1", Total: 200.0, Status: "Updated"}
	instance.Send(
		ctx,
		mocks.UpdateOrderCmd{ID: "order-1", NewState: updated},
	)

	var gotTotal float64
	instance.OnForget(func(
		_ context.Context,
		e event.Event[mocks.Order],
	) {
		gotTotal = e.Aggregate.Total
	})

	if err := instance.Forget(
		ctx,
		"order-1",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	if gotTotal != 200.0 {
		t.Errorf(
			"Forget saw Total=%v, want 200.0 (Send should have completed first)",
			gotTotal,
		)
	}
}

type deleteErrStore struct {
	store.Memory
	deleteErr error
}

// Atomically runs fn against a transaction whose DeleteStream fails.
func (s *deleteErrStore) Atomically(
	ctx context.Context,
	fn func(storage.Tx) error,
) error {
	return s.Memory.Atomically(
		ctx,
		deleteErrRunner{owner: s, fn: fn}.run,
	)
}

type deleteErrRunner struct {
	owner *deleteErrStore
	fn    func(storage.Tx) error
}

func (r deleteErrRunner) run(
	tx storage.Tx,
) error {
	return r.fn(&deleteErrTx{Tx: tx, owner: r.owner})
}

type deleteErrTx struct {
	storage.Tx
	owner *deleteErrStore
}

// DeleteStream fails with the configured error.
func (t *deleteErrTx) DeleteStream(
	ctx context.Context,
	streamID string,
) error {
	if t.owner.deleteErr != nil {
		err := t.owner.deleteErr
		t.owner.deleteErr = nil
		return err
	}

	return t.Tx.DeleteStream(
		ctx,
		streamID,
	)
}

func TestForget_DeleteFails_ReturnsErrForgetFailed(
	t *testing.T,
) {
	sentinel := errors.New("store unavailable")
	backing := &deleteErrStore{Memory: store.New(), deleteErr: sentinel}

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(backing).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 1000}).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "order-1", Total: 10.0},
	); err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	err = instance.Forget(
		ctx,
		"order-1",
	)
	if !errors.Is(
		err,
		failure.ErrForgetFailed,
	) {
		t.Fatalf(
			"expected ErrForgetFailed, got %v",
			err,
		)
	}
}

func forgetInto(
	ctx context.Context,
	instance asynx.Asynx[mocks.Order],
	wg *sync.WaitGroup,
	dst *error,
) {
	defer wg.Done()
	*dst = instance.Forget(
		ctx,
		"order-1",
	)
}

func isExpectedForgetErr(
	err error,
) bool {
	return errors.Is(
		err,
		failure.ErrValidation,
	) ||
		errors.Is(
			err,
			failure.ErrPipelineFailed,
		) ||
		errors.Is(
			err,
			failure.ErrForgetInProgress,
		)
}

func checkForgetErr(
	t *testing.T,
	err error,
) {
	t.Helper()

	if err == nil ||
		isExpectedForgetErr(err) {
		return
	}

	t.Errorf(
		"unexpected error from concurrent Forget: %v",
		err,
	)
}

func TestForget_ConcurrentForget_SecondReturnsErrValidation(
	t *testing.T,
) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "order-1", Total: 100.0},
	); err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	var (
		wg   sync.WaitGroup
		errs [2]error
	)
	for i := range 2 {
		wg.Add(1)
		go forgetInto(
			ctx,
			instance,
			&wg,
			&errs[i],
		)
	}

	wg.Wait()

	for _, err := range errs {
		checkForgetErr(
			t,
			err,
		)
	}

	_, getErr := instance.Get(
		ctx,
		"order-1",
	)
	if !errors.Is(
		getErr,
		failure.ErrNotFound,
	) {
		t.Errorf(
			"expected aggregate to be erased after concurrent Forget, got Get err: %v",
			getErr,
		)
	}
}

func TestOnForget_Unsubscribe_StopsHandler(
	t *testing.T,
) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "order-1", Total: 100.0},
	); err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	var called int
	subID, err := instance.OnForget(func(
		_ context.Context,
		_ event.Event[mocks.Order],
	) {
		called++
	})
	if err != nil {
		t.Fatalf(
			"OnForget: %v",
			err,
		)
	}

	if err := instance.Unsubscribe(subID); err != nil {
		t.Fatalf(
			"Unsubscribe: %v",
			err,
		)
	}

	if err := instance.Forget(
		ctx,
		"order-1",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	if called != 0 {
		t.Errorf(
			"handler called %d times after Unsubscribe, want 0",
			called,
		)
	}
}

func TestSubscribeWait_EmptyPattern(
	t *testing.T,
) {
	instance := newInstance(t)
	_, err := instance.SubscribeWait(
		context.Background(),
		"",
	)
	if err != failure.ErrEmptyPattern {
		t.Fatalf(
			"expected ErrEmptyPattern, got %v",
			err,
		)
	}
}

func TestSubscribeWait_EventArrivesBeforeDeadline(
	t *testing.T,
) {
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	stop := feedOrders(
		t,
		instance,
		"sw",
	)
	defer stop()

	evt, err := instance.SubscribeWait(
		ctx,
		"OrderCreated",
	)
	if err != nil {
		t.Fatalf(
			"SubscribeWait: %v",
			err,
		)
	}

	if !strings.HasPrefix(
		evt.AggregateID,
		"sw-",
	) {
		t.Fatalf(
			"expected AggregateID sw-*, got %q",
			evt.AggregateID,
		)
	}

	if evt.Aggregate.Total != 1.0 {
		t.Fatalf(
			"expected Total 1.0, got %f",
			evt.Aggregate.Total,
		)
	}
}

func TestSubscribeWait_DeadlineExceeded(
	t *testing.T,
) {
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	_, err := instance.SubscribeWait(
		ctx,
		"OrderCreated",
	)
	if err != context.DeadlineExceeded {
		t.Fatalf(
			"expected context.DeadlineExceeded, got %v",
			err,
		)
	}
}

func TestSubscribeWait_ContextCancel_ReturnsCanceled(
	t *testing.T,
) {
	instance := newInstance(t)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := instance.SubscribeWait(
		ctx,
		"OrderCreated",
	)
	if err != context.Canceled {
		t.Fatalf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

func waitInto(
	ctx context.Context,
	instance asynx.Asynx[mocks.Order],
	wg *sync.WaitGroup,
	pattern string,
	result *event.Event[mocks.Order],
	errDst *error,
) {
	defer wg.Done()
	*result, *errDst = instance.SubscribeWait(
		ctx,
		pattern,
	)
}

func cancelCmd(
	id string,
) command.Command[mocks.Order] {
	return mocks.CancelOrderCmd{ID: id}
}

func requireNoErrs(
	t *testing.T,
	errs []error,
) {
	t.Helper()

	for i, err := range errs {
		requireNoErr(
			t,
			err,
			fmt.Sprintf(
				"SubscribeWait[%d]",
				i,
			),
		)
	}
}

func TestSubscribeWait_ConcurrentDifferentPatterns(
	t *testing.T,
) {
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	var wg sync.WaitGroup
	results := make(
		[]event.Event[mocks.Order],
		2,
	)
	errs := make(
		[]error,
		2,
	)

	wg.Add(2)
	go waitInto(
		ctx,
		instance,
		&wg,
		"OrderCreated",
		&results[0],
		&errs[0],
	)
	go waitInto(
		ctx,
		instance,
		&wg,
		"OrderCancelled",
		&results[1],
		&errs[1],
	)

	stop := feedOrders(
		t,
		instance,
		"conc",
		cancelCmd,
	)
	defer stop()

	wg.Wait()

	requireNoErrs(
		t,
		errs,
	)

	if results[0].EventName != "OrderCreated" {
		t.Fatalf(
			"expected OrderCreated, got %q",
			results[0].EventName,
		)
	}

	if results[1].EventName != "OrderCancelled" {
		t.Fatalf(
			"expected OrderCancelled, got %q",
			results[1].EventName,
		)
	}
}

func TestSubscribeWait_AutoUnsubscribes(
	t *testing.T,
) {
	instance := newInstance(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	stop := feedOrders(
		t,
		instance,
		"feed",
	)

	_, err := instance.SubscribeWait(
		ctx,
		"OrderCreated",
	)
	stop()
	if err != nil {
		t.Fatalf(
			"SubscribeWait: %v",
			err,
		)
	}

	if _, err := instance.SendWait(
		context.Background(),
		mocks.CreateOrderCmd{ID: "auto-2", Total: 2.0},
	); err != nil {
		t.Fatalf(
			"second SendWait: %v",
			err,
		)
	}
}

func sendRace(
	instance asynx.Asynx[mocks.Order],
	wg *sync.WaitGroup,
	iter int,
	idx int,
) {
	defer wg.Done()
	cmd := mocks.CreateOrderCmd{
		ID: fmt.Sprintf(
			"race-wp-%d-%d",
			iter,
			idx,
		),
		Total: float64(idx + 1),
	}
	instance.Send(
		context.Background(),
		cmd,
	)
}

func noRaceIteration(
	t *testing.T,
	iter int,
) {
	t.Helper()

	instance := newInstance(t)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go sendRace(
			instance,
			&wg,
			iter,
			i,
		)
	}

	wg.Wait()

	waitPublish(
		t,
		instance,
	)
}

func TestWaitPublish_NoRaceWithConcurrentDispatch(
	t *testing.T,
) {
	for iter := 0; iter < 100; iter++ {
		noRaceIteration(
			t,
			iter,
		)
	}
}

func delayedUpdate(
	ctx context.Context,
	instance asynx.Asynx[mocks.Order],
	id string,
) {
	time.Sleep(1 * time.Millisecond)

	cmd := mocks.UpdateOrderCmd{
		ID:       id,
		NewState: mocks.Order{ID: id, Total: 999.0, Status: "Updated"},
	}
	instance.Send(
		ctx,
		cmd,
	)
}

type reactingHandler struct {
	instance asynx.Asynx[mocks.Order]
	started  atomic.Int32
	finished atomic.Int32
}

func (r *reactingHandler) handle(
	ctx context.Context,
	evt event.Event[mocks.Order],
) {
	r.started.Store(1)
	defer r.finished.Store(1)

	go delayedUpdate(
		ctx,
		r.instance,
		evt.AggregateID,
	)
}

func reactionIteration(
	t *testing.T,
	iter int,
) {
	t.Helper()

	instance := newInstance(t)
	reaction := &reactingHandler{instance: instance}

	handlerID, err := instance.Subscribe(
		"OrderCreated",
		reaction.handle,
	)
	if err != nil {
		t.Fatalf(
			"Subscribe failed: %v",
			err,
		)
	}

	defer instance.Unsubscribe(handlerID)

	cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf(
		"race-reaction-%d",
		iter,
	), Total: 100.0}
	_, err = instance.Send(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Send failed: %v",
			err,
		)
	}

	waitPublish(
		t,
		instance,
	)

	if reaction.started.Load() == 0 {
		t.Logf(
			"Iteration %d: reaction never started",
			iter,
		)
	}

	if reaction.finished.Load() == 0 {
		t.Logf(
			"Iteration %d: reaction never finished",
			iter,
		)
	}
}

func TestWaitPublish_RaceWithAsyncReactionGoroutines(
	t *testing.T,
) {
	for iter := 0; iter < 100; iter++ {
		reactionIteration(
			t,
			iter,
		)
	}
}

func within[V any](
	t *testing.T,
	ch <-chan V,
	what string,
) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"timed out waiting for %s",
			what,
		)
		panic("unreachable")
	}
}

type versions struct {
	mu  sync.Mutex
	got []int64
}

func (v *versions) add(
	n int64,
) {
	v.mu.Lock()
	v.got = append(
		v.got,
		n,
	)
	v.mu.Unlock()
}

func (v *versions) list() []int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append(
		[]int64(nil),
		v.got...,
	)
}

func equalVersions(
	a,
	b []int64,
) bool {
	return slices.Equal(
		a,
		b,
	)
}

func sendUpd(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	id string,
	total float64,
) {
	t.Helper()

	_, err := inst.Send(
		context.Background(),
		upd(
			id,
			total,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func shutdownOrFatal(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	what string,
) {
	t.Helper()

	sctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	err := inst.Shutdown(sctx)
	if err != nil {
		t.Fatalf(
			"%s: %v",
			what,
			err,
		)
	}
}

func recordVersions(
	v *versions,
) func(context.Context, event.Event[mocks.Order]) error {
	return func(
		_ context.Context,
		e event.Event[mocks.Order],
	) error {
		v.add(e.Version)
		return nil
	}
}

type failFirst struct {
	seen      *versions
	attempted chan<- struct{}
}

func (f failFirst) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	f.seen.add(e.Version)
	if e.Version == 1 {
		f.attempted <- struct{}{}
		return errors.New("poison")
	}

	return nil
}

func TestFailure_ShutdownDuringRetryKeepsVersionOrderAcrossRestart(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()

	var seen versions
	attempted := make(
		chan struct{},
		64,
	)
	first, err := eight(s).
		WithDeliveryRetry(
			time.Hour,
			time.Hour,
		).
		WithSubscriber(
			"OrderUpdated",
			failFirst{seen: &seen, attempted: attempted}.handle,
		).Build()
	if err != nil {
		t.Fatal(err)
	}

	for i := range 2 {
		sendUpd(
			t,
			first,
			"agg",
			float64(i+1),
		)
	}

	within(
		t,
		attempted,
		"first attempt",
	)

	shutdownOrFatal(
		t,
		first,
		"Shutdown with a retrying delivery",
	)

	if got := seen.list(); !equalVersions(
		got,
		[]int64{1},
	) {
		t.Fatalf(
			"handler saw versions %v before restart, want only the failing v1",
			got,
		)
	}

	pending, _ := s.PendingDeliveries(
		ctx,
		10,
	)
	if len(pending) != 2 ||
		pending[0].Version != 1 ||
		pending[1].Version != 2 {
		t.Fatalf(
			"pending after shutdown = %v, want v1 and v2 both still pending",
			pending,
		)
	}

	var after versions
	second := build(
		t,
		eight(s).WithSubscriber(
			"OrderUpdated",
			recordVersions(&after),
		),
	)
	waitPublish(
		t,
		second,
	)
	if got := after.list(); !equalVersions(
		got,
		[]int64{1, 2},
	) {
		t.Fatalf(
			"after restart delivered %v, want [1 2]",
			got,
		)
	}
}

type poisonBad struct {
	attempted chan<- struct{}
}

func (p poisonBad) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	if strings.HasPrefix(
		e.AggregateID,
		"bad",
	) &&
		e.EventName != event.ForgetEventName {
		p.attempted <- struct{}{}
		return errors.New("poison")
	}

	return nil
}

func sendWaitIntoChannel(
	ctx context.Context,
	inst asynx.Asynx[mocks.Order],
	dst chan<- error,
	cmd command.Command[mocks.Order],
) {
	_, err := inst.SendWait(
		ctx,
		cmd,
	)
	dst <- err
}

type commitWatch struct {
	t      *testing.T
	waited <-chan error
	s      store.Memory
}

func (w commitWatch) committed() bool {
	select {
	case err := <-w.waited:
		w.t.Fatalf(
			"SendWait returned %v before committing",
			err,
		)
	default:
	}

	blobs, _ := w.s.ReadFrom(
		context.Background(),
		"events:bad",
		1,
	)

	return len(blobs) == 2
}

func TestFailure_PoisonEventDoesNotBlockTheShardOrShutdown(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()

	attempted := make(
		chan struct{},
		64,
	)
	inst, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, WorkersPerShard: 1, QueueDepth: 100}).
		WithDeliveryRetry(
			time.Hour,
			time.Hour,
		).
		WithSubscriber(
			"OrderUpdated",
			poisonBad{attempted: attempted}.handle,
		).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	sendUpd(
		t,
		inst,
		"bad",
		1,
	)

	within(
		t,
		attempted,
		"poison attempt",
	)

	wctx, cancelWait := context.WithCancel(ctx)
	waited := make(
		chan error,
		1,
	)
	go sendWaitIntoChannel(
		wctx,
		inst,
		waited,
		upd(
			"bad",
			2,
		),
	)
	eventually(
		t,
		"the second command to commit",
		commitWatch{t: t, waited: waited, s: s}.committed,
	)
	cancelWait()
	if err := within(
		t,
		waited,
		"SendWait to give up",
	); !errors.Is(
		err,
		failure.ErrContextCancelled,
	) {
		t.Fatalf(
			"SendWait err = %v, want ErrContextCancelled",
			err,
		)
	}

	if _, err := inst.SendWait(
		ctx,
		upd(
			"good",
			1,
		),
	); err != nil {
		t.Fatalf(
			"unrelated aggregate on the same shard: %v",
			err,
		)
	}

	fctx, cancelForget := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancelForget()
	if err := inst.Forget(
		fctx,
		"bad",
	); err != nil {
		t.Fatalf(
			"Forget of a poisoned aggregate: %v",
			err,
		)
	}

	if ok, _ := inst.Exists(
		ctx,
		"bad",
	); ok {
		t.Fatal("poisoned aggregate survived Forget")
	}

	sendUpd(
		t,
		inst,
		"bad2",
		1,
	)

	within(
		t,
		attempted,
		"second poison attempt",
	)
	shutdownOrFatal(
		t,
		inst,
		"Shutdown with a poison delivery",
	)
}

type switchable struct {
	mu      sync.Mutex
	healthy bool
	seen    versions
}

func (h *switchable) setHealthy() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy = true
}

func (h *switchable) isDown(
	id string,
) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return !h.healthy && id == "bad"
}

func (h *switchable) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	if h.isDown(e.AggregateID) {
		return errors.New("read model down")
	}

	if e.AggregateID == "bad" {
		h.seen.add(e.Version)
	}

	return nil
}

type parkReporter struct {
	parked chan<- error
}

func (r parkReporter) handle(
	_ context.Context,
	_ event.Event[mocks.Order],
	err error,
	_ int,
) {
	if errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		r.parked <- err
	}
}

func notAggregate(
	id string,
) func(event.Delivery) bool {
	return func(
		p event.Delivery,
	) bool {
		return p.AggregateID != id
	}
}

func countPending(
	pending []event.Delivery,
	id string,
) int {
	return len(slices.DeleteFunc(
		slices.Clone(pending),
		notAggregate(id),
	))
}

func TestFailure_RetriesStopAndTheAggregateIsParkedUntilResumed(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()

	handler := &switchable{}
	parked := make(
		chan error,
		8,
	)
	inst := build(
		t,
		eight(s).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithDeliveryMaxAttempts(3).
			WithDeliveryErrorHandler(parkReporter{parked: parked}.handle).
			WithSubscriber(
				"OrderUpdated",
				handler.handle,
			),
	)

	for i := range 2 {
		sendUpd(
			t,
			inst,
			"bad",
			float64(i+1),
		)
	}

	within(
		t,
		parked,
		"park report",
	)

	if _, err := inst.SendWait(
		ctx,
		upd(
			"other",
			1,
		),
	); err != nil {
		t.Fatalf(
			"another aggregate: %v",
			err,
		)
	}

	if evt, err := inst.SendWait(
		ctx,
		upd(
			"bad",
			3,
		),
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) ||
		evt.Version != 3 {
		t.Fatalf(
			"SendWait to a parked aggregate = v%d, %v; want the committed v3 with ErrDeliveryParked",
			evt.Version,
			err,
		)
	}

	if got := inst.PausedAggregates(); !slices.Equal(
		got,
		[]string{"bad"},
	) {
		t.Fatalf(
			"PausedAggregates = %v, want [bad]",
			got,
		)
	}

	if got := handler.seen.list(); len(got) != 0 {
		t.Fatalf(
			"parked aggregate delivered %v",
			got,
		)
	}

	pending, _ := s.PendingDeliveries(
		ctx,
		10,
	)
	nBad := countPending(
		pending,
		"bad",
	)
	if nBad != 3 {
		t.Fatalf(
			"pending deliveries of the parked aggregate = %d, want 3 (%v)",
			nBad,
			pending,
		)
	}

	handler.setHealthy()
	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatalf(
			"ResumeDeliveries: %v",
			err,
		)
	}

	waitPublish(
		t,
		inst,
	)
	if got := handler.seen.list(); !equalVersions(
		got,
		[]int64{1, 2, 3},
	) {
		t.Fatalf(
			"after resume delivered %v, want [1 2 3] in order",
			got,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending after resume = %v",
			pending,
		)
	}
}

type deadSink struct {
	dead chan<- int64
}

func (d deadSink) handle(
	_ context.Context,
	e event.Event[mocks.Order],
	_ error,
) error {
	d.dead <- e.Version
	return nil
}

type poisonFirst struct {
	seen *versions
}

func (p poisonFirst) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	if e.Version == 1 {
		return errors.New("poison")
	}

	p.seen.add(e.Version)
	return nil
}

func TestFailure_DeadLetterSkipsThePoisonEvent(
	t *testing.T,
) {
	var seen versions
	dead := make(
		chan int64,
		4,
	)
	inst := build(
		t,
		eight(store.New()).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithDeliveryMaxAttempts(2).
			WithDeadLetterHandler(deadSink{dead: dead}.handle).
			WithSubscriber(
				"OrderUpdated",
				poisonFirst{seen: &seen}.handle,
			),
	)
	for i := range 3 {
		sendUpd(
			t,
			inst,
			"agg",
			float64(i+1),
		)
	}

	if v := within(
		t,
		dead,
		"dead letter",
	); v != 1 {
		t.Fatalf(
			"dead-lettered v%d, want v1",
			v,
		)
	}

	waitPublish(
		t,
		inst,
	)
	if got := seen.list(); !equalVersions(
		got,
		[]int64{2, 3},
	) {
		t.Fatalf(
			"delivered %v after skipping v1, want [2 3]",
			got,
		)
	}
}

func TestFailure_CommandsAreRejectedWhileForgetRuns(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	inst := build(
		t,
		eight(s),
	)

	entered := make(chan struct{})
	release := make(chan struct{})
	var reaction error
	if _, err := inst.OnForget(func(
		c context.Context,
		e event.Event[mocks.Order],
	) {
		close(entered)
		_, reaction = inst.Send(
			c,
			upd(
				"agg",
				99,
			),
		)
		<-release
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := inst.Send(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	forgot := make(
		chan error,
		1,
	)
	go func() {
		forgot <- inst.Forget(
			ctx,
			"agg",
		)
	}()
	within(
		t,
		entered,
		"forget handler",
	)

	if _, err := inst.Send(
		ctx,
		upd(
			"agg",
			2,
		),
	); !errors.Is(
		err,
		failure.ErrForgetInProgress,
	) {
		t.Fatalf(
			"Send during Forget err = %v, want ErrForgetInProgress",
			err,
		)
	}

	if _, err := inst.Send(
		ctx,
		upd(
			"other",
			1,
		),
	); err != nil {
		t.Fatalf(
			"unrelated aggregate during Forget: %v",
			err,
		)
	}

	close(release)
	if err := within(
		t,
		forgot,
		"Forget",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	if !errors.Is(
		reaction,
		failure.ErrForgetInProgress,
	) {
		t.Fatalf(
			"reaction err = %v, want ErrForgetInProgress",
			reaction,
		)
	}

	if blobs, _ := s.ReadFrom(
		ctx,
		"events:agg",
		1,
	); len(blobs) != 0 {
		t.Fatalf(
			"stream has %d events after Forget: a command slipped in",
			len(blobs),
		)
	}

	evt, err := inst.Send(
		ctx,
		upd(
			"agg",
			3,
		),
	)
	if err != nil {
		t.Fatalf(
			"Send after Forget: %v",
			err,
		)
	}

	if evt.Version != 1 {
		t.Fatalf(
			"re-created aggregate version = %d, want 1",
			evt.Version,
		)
	}
}

type versionKey struct {
	id string
	v  int64
}

func keyIs(
	id string,
) func(versionKey, bool) bool {
	return func(
		k versionKey,
		_ bool,
	) bool {
		return k.id == id
	}
}

type markerTracker struct {
	mu      sync.Mutex
	seen    map[versionKey]bool
	applied []int64
	markers int
}

func (m *markerTracker) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.EventName == event.ForgetEventName {
		m.markers++
		maps.DeleteFunc(
			m.seen,
			keyIs(e.AggregateID),
		)

		return nil
	}

	k := versionKey{e.AggregateID, e.Version}
	if m.seen[k] {
		return nil
	}

	m.seen[k] = true
	m.applied = append(
		m.applied,
		e.Version,
	)
	return nil
}

func sendWaitUpd(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	id string,
	total float64,
) {
	t.Helper()

	_, err := inst.SendWait(
		context.Background(),
		upd(
			id,
			total,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFailure_DurableSubscribersReceiveTheDeletionMarker(
	t *testing.T,
) {
	ctx := context.Background()
	tracker := &markerTracker{seen: map[versionKey]bool{}}

	inst := build(
		t,
		eight(store.New()).
			WithSubscriber(
				"OrderUpdated",
				tracker.handle,
			),
	)

	sendWaitUpd(
		t,
		inst,
		"agg",
		1,
	)

	if err := inst.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	sendWaitUpd(
		t,
		inst,
		"agg",
		2,
	)

	waitPublish(
		t,
		inst,
	)

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.markers != 1 {
		t.Fatalf(
			"deletion markers seen = %d, want 1 (pattern OrderUpdated does not match it, it is delivered anyway)",
			tracker.markers,
		)
	}

	if !equalVersions(
		tracker.applied,
		[]int64{1, 1},
	) {
		t.Fatalf(
			"applied versions = %v, want the re-created aggregate's v1 applied again",
			tracker.applied,
		)
	}
}

type markerGate struct {
	mu      sync.Mutex
	healthy bool
}

func (g *markerGate) setHealthy() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.healthy = true
}

func (g *markerGate) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e.EventName == event.ForgetEventName &&
		!g.healthy {
		return errors.New("cannot drop state")
	}

	return nil
}

func TestFailure_ForgetFailsWhenADurableSubscriberCannotTakeTheMarker(
	t *testing.T,
) {
	ctx := context.Background()
	gate := &markerGate{}
	inst := build(
		t,
		eight(store.New()).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithDeliveryMaxAttempts(2).
			WithSubscriber(
				"OrderUpdated",
				gate.handle,
			),
	)
	sendWaitUpd(
		t,
		inst,
		"agg",
		1,
	)

	if err := inst.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		failure.ErrForgetFailed,
	) {
		t.Fatalf(
			"Forget err = %v, want ErrForgetFailed",
			err,
		)
	}

	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			2,
		),
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait after a failed Forget = %v, want ErrDeliveryParked",
			err,
		)
	}

	gate.setHealthy()
	if err := inst.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatalf(
			"retried Forget: %v",
			err,
		)
	}

	if ok, _ := inst.Exists(
		ctx,
		"agg",
	); ok {
		t.Fatal("aggregate survived the retried Forget")
	}
}

type failingLiveBus struct{ pubsub.Bus[mocks.Order] }

// PublishSync fails with the configured error.
func (failingLiveBus) PublishSync(
	context.Context,
	event.Event[mocks.Order],
) error {
	return errors.New("live bus down")
}

type errSink struct {
	reported chan<- error
}

func (s errSink) handle(
	_ context.Context,
	_ event.Event[mocks.Order],
	err error,
) {
	s.reported <- err
}

func TestFailure_LiveBusErrorDoesNotSkipDurableDelivery(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	var seen versions
	reported := make(
		chan error,
		8,
	)
	inst := build(
		t,
		eight(s).
			WithBus(failingLiveBus{bus.New[mocks.Order]()}).
			WithPublishErrorHandler(errSink{reported: reported}.handle).
			WithSubscriber(
				"OrderUpdated",
				recordVersions(&seen),
			),
	)
	for i := range 3 {
		sendWaitUpd(
			t,
			inst,
			"agg",
			float64(i+1),
		)
	}

	waitPublish(
		t,
		inst,
	)
	if got := seen.list(); !equalVersions(
		got,
		[]int64{1, 2, 3},
	) {
		t.Fatalf(
			"durable deliveries = %v, want [1 2 3]",
			got,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending = %v, want none",
			pending,
		)
	}

	within(
		t,
		reported,
		"bus error report",
	)
}

type deliveryLog struct {
	mu        sync.Mutex
	healthy   bool
	delivered []string
}

func (l *deliveryLog) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.healthy {
		return errors.New("down")
	}

	l.delivered = append(
		l.delivered,
		fmt.Sprintf(
			"%s/%d",
			e.EventName,
			e.Version,
		),
	)
	return nil
}

func (l *deliveryLog) setHealthy() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.healthy = true
}

func (l *deliveryLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.delivered)
}

func TestFailure_FailedForgetDoesNotLetLaterEventsOvertakeTheMarker(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	log := &deliveryLog{}
	inst := build(
		t,
		eight(s).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithDeliveryMaxAttempts(100000).
			WithSubscriber(
				"OrderUpdated",
				log.handle,
			),
	)

	sendUpd(
		t,
		inst,
		"agg",
		1,
	)

	short, cancel := context.WithTimeout(
		ctx,
		100*time.Millisecond,
	)
	defer cancel()
	if err := inst.Forget(
		short,
		"agg",
	); !errors.Is(
		err,
		failure.ErrForgetFailed,
	) {
		t.Fatalf(
			"Forget = %v, want ErrForgetFailed",
			err,
		)
	}

	if got := inst.PausedAggregates(); !slices.Equal(
		got,
		[]string{"agg"},
	) {
		t.Fatalf(
			"PausedAggregates = %v, want [agg]",
			got,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 ||
		pending[0].Version != 2 {
		t.Fatalf(
			"pending after the tombstone = %v, want only the marker v2",
			pending,
		)
	}

	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			3,
		),
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait = %v, want ErrDeliveryParked",
			err,
		)
	}

	log.setHealthy()
	if got := log.list(); len(got) != 0 {
		t.Fatalf(
			"delivered %v while the marker was undelivered",
			got,
		)
	}

	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatal(err)
	}

	waitPublish(
		t,
		inst,
	)
	delivered := log.list()
	if want := []string{event.ForgetEventName + "/2", "OrderUpdated/3"}; !slices.Equal(
		delivered,
		want,
	) {
		t.Fatalf(
			"after resume delivered %v, want the marker first, then the event after it: %v",
			delivered,
			want,
		)
	}

	if len(inst.PausedAggregates()) != 0 {
		t.Fatalf(
			"still paused: %v",
			inst.PausedAggregates(),
		)
	}
}

type healthGate struct {
	mu      sync.Mutex
	healthy bool
}

func (g *healthGate) setHealthy() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.healthy = true
}

func (g *healthGate) handle(
	context.Context,
	event.Event[mocks.Order],
) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.healthy {
		return errors.New("down")
	}

	return nil
}

func recordLive(
	v *versions,
) func(context.Context, event.Event[mocks.Order]) {
	return func(
		_ context.Context,
		e event.Event[mocks.Order],
	) {
		v.add(e.Version)
	}
}

type pausedCount struct {
	inst asynx.Asynx[mocks.Order]
	want int
}

func (p pausedCount) reached() bool {
	return len(p.inst.PausedAggregates()) == p.want
}

func subscribeOrFatal(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	pattern string,
	h func(context.Context, event.Event[mocks.Order]),
) {
	t.Helper()

	_, err := inst.Subscribe(
		pattern,
		h,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFailure_ResumeDoesNotReplayLiveSubscribers(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	gate := &healthGate{}
	var live, cancelled versions
	inst := build(
		t,
		eight(s).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithDeliveryMaxAttempts(2).
			WithSubscriber(
				"OrderUpdated",
				gate.handle,
			),
	)
	subscribeOrFatal(
		t,
		inst,
		"OrderUpdated",
		recordLive(&live),
	)
	subscribeOrFatal(
		t,
		inst,
		"OrderCancelled",
		recordLive(&cancelled),
	)

	sendUpd(
		t,
		inst,
		"agg",
		1,
	)

	eventually(
		t,
		"aggregate paused",
		pausedCount{inst: inst, want: 1}.reached,
	)
	if _, err := inst.SendWait(
		ctx,
		mocks.CancelOrderCmd{ID: "agg"},
	); err != nil {
		t.Fatalf(
			"live-only event on a paused aggregate: %v",
			err,
		)
	}

	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			3,
		),
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait = %v, want ErrDeliveryParked",
			err,
		)
	}

	gate.setHealthy()
	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatal(err)
	}

	waitPublish(
		t,
		inst,
	)
	if got := live.list(); !equalVersions(
		got,
		[]int64{1},
	) {
		t.Fatalf(
			"live OrderUpdated versions = %v, want [1]: nothing replayed",
			got,
		)
	}

	if got := cancelled.list(); !equalVersions(
		got,
		[]int64{2},
	) {
		t.Fatalf(
			"live OrderCancelled versions = %v, want [2] once",
			got,
		)
	}
}

type selfForget struct {
	inst asynx.Asynx[mocks.Order]
	got  chan<- error
}

func (h *selfForget) handle(
	ctx context.Context,
	e event.Event[mocks.Order],
) error {
	h.got <- h.inst.Forget(
		ctx,
		e.AggregateID,
	)
	return nil
}

func TestFailure_HandlerForgettingItsOwnAggregateFailsFast(
	t *testing.T,
) {
	got := make(
		chan error,
		1,
	)
	handler := &selfForget{got: got}
	inst := build(
		t,
		eight(store.New()).
			WithSubscriber(
				"OrderUpdated",
				handler.handle,
			),
	)
	handler.inst = inst
	sendWaitUpd(
		t,
		inst,
		"agg",
		1,
	)

	if err := within(
		t,
		got,
		"Forget from the handler",
	); !errors.Is(
		err,
		failure.ErrForgetFromHandler,
	) {
		t.Fatalf(
			"Forget from a handler = %v, want ErrForgetFromHandler",
			err,
		)
	}

	if _, err := inst.SendWait(
		context.Background(),
		upd(
			"agg",
			2,
		),
	); err != nil {
		t.Fatalf(
			"the aggregate is frozen after the refused Forget: %v",
			err,
		)
	}

	shutdownOrFatal(
		t,
		inst,
		"Shutdown",
	)
}

func alwaysDown(
	context.Context,
	event.Event[mocks.Order],
) error {
	return errors.New("down")
}

func TestFailure_RestartAfterAFailedForgetDeliversTheMarkerFirst(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	down := build(
		t,
		eight(s).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithDeliveryMaxAttempts(2).
			WithSubscriber(
				"OrderUpdated",
				alwaysDown,
			),
	)
	sendUpd(
		t,
		down,
		"agg",
		1,
	)

	if err := down.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		failure.ErrForgetFailed,
	) {
		t.Fatalf(
			"Forget = %v, want ErrForgetFailed",
			err,
		)
	}

	if _, err := down.SendWait(
		ctx,
		upd(
			"agg",
			3,
		),
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait = %v, want ErrDeliveryParked",
			err,
		)
	}

	shutdownOrFatal(
		t,
		down,
		"Shutdown",
	)

	log := &deliveryLog{healthy: true}
	up := build(
		t,
		eight(s).
			WithSubscriber(
				"OrderUpdated",
				log.handle,
			),
	)
	waitPublish(
		t,
		up,
	)
	delivered := log.list()
	if want := []string{event.ForgetEventName + "/2", "OrderUpdated/3"}; !slices.Equal(
		delivered,
		want,
	) {
		t.Fatalf(
			"after the restart delivered %v, want %v",
			delivered,
			want,
		)
	}
}

type orderFeeder struct {
	t       *testing.T
	quit    chan struct{}
	stopped chan error
	once    sync.Once
}

func (f *orderFeeder) stop() {
	f.once.Do(f.shutDown)
}

func (f *orderFeeder) shutDown() {
	close(f.quit)
	reportFeederStop(
		f.t,
		<-f.stopped,
	)
}

func feedOrders(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	prefix string,
	followUps ...func(id string) command.Command[mocks.Order],
) (stop func()) {
	feeder := &orderFeeder{
		t:    t,
		quit: make(chan struct{}),
		stopped: make(
			chan error,
			1,
		),
	}
	go feeder.feed(
		instance,
		prefix,
		followUps,
	)

	return feeder.stop
}

func (f *orderFeeder) feed(
	instance asynx.Asynx[mocks.Order],
	prefix string,
	followUps []func(id string) command.Command[mocks.Order],
) {
	f.stopped <- feedUntil(
		f.quit,
		instance,
		prefix,
		followUps,
	)
}

func reportFeederStop(
	t *testing.T,
	err error,
) {
	if err != nil {
		t.Logf(
			"order feeder stopped early: %v",
			err,
		)
	}
}

func feedOne(
	instance asynx.Asynx[mocks.Order],
	prefix string,
	i int,
	followUps []func(id string) command.Command[mocks.Order],
) error {
	id := fmt.Sprintf(
		"%s-%d",
		prefix,
		i,
	)
	_, err := instance.SendWait(
		context.Background(),
		mocks.CreateOrderCmd{ID: id, Total: 1},
	)
	if err != nil {
		return fmt.Errorf(
			"feed %s: %w",
			id,
			err,
		)
	}

	for _, followUp := range followUps {
		instance.SendWait(
			context.Background(),
			followUp(id),
		)
	}

	return nil
}

func feedUntil(
	quit <-chan struct{},
	instance asynx.Asynx[mocks.Order],
	prefix string,
	followUps []func(id string) command.Command[mocks.Order],
) (err error) {
	for i := 0; !isClosed(quit) && err == nil; i++ {
		err = feedOne(
			instance,
			prefix,
			i,
			followUps,
		)
	}

	return err
}

func isClosed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

type blockingSnapshots struct {
	storage.SnapshotStore
	aggregate string
	armed     atomic.Bool
	entered   chan struct{}
	release   chan struct{}
}

// Put blocks until released, then stores the snapshot.
func (s *blockingSnapshots) Put(
	ctx context.Context,
	aggregateID string,
	version int64,
	data []byte,
) error {
	if aggregateID == s.aggregate &&
		s.armed.CompareAndSwap(
			true,
			false,
		) {
		close(s.entered)
		<-s.release
	}

	return s.SnapshotStore.Put(
		ctx,
		aggregateID,
		version,
		data,
	)
}

func identityUpcaster(
	_ context.Context,
	_ string,
	p []byte,
) ([]byte, error) {
	return p,
		nil
}

func getInto(
	ctx context.Context,
	inst asynx.Asynx[mocks.Order],
	id string,
	dst chan<- error,
) {
	_, err := inst.Get(
		ctx,
		id,
	)
	dst <- err
}

func forgetTo(
	ctx context.Context,
	inst asynx.Asynx[mocks.Order],
	id string,
	dst chan<- error,
) {
	dst <- inst.Forget(
		ctx,
		id,
	)
}

func sendWaitCmd(
	t *testing.T,
	inst asynx.Asynx[mocks.Order],
	cmd command.Command[mocks.Order],
) {
	t.Helper()

	_, err := inst.SendWait(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGet_AutoSnapshotCannotResurrectAForgottenAggregate(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	old := build(
		t,
		eight(s),
	)
	for _, cmd := range []command.Command[mocks.Order]{mocks.CreateOrderCmd{ID: "ws", Total: 10}, upd(
		"ws",
		20,
	)} {
		sendWaitCmd(
			t,
			old,
			cmd,
		)
	}

	if err := old.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	snaps := &blockingSnapshots{SnapshotStore: store.NewSnapshots(), aggregate: "ws", entered: make(chan struct{}), release: make(chan struct{})}
	inst := build(
		t,
		asynx.New[mocks.Order]().WithEventStore(s).WithSnapshotStore(snaps).
			WithSchemaVersion(2).
			WithUpcaster(
				1,
				identityUpcaster,
			),
	)
	snaps.armed.Store(true)
	got := make(
		chan error,
		1,
	)
	go getInto(
		ctx,
		inst,
		"ws",
		got,
	)
	within(
		t,
		snaps.entered,
		"the auto-snapshot write",
	)

	forgot := make(
		chan error,
		1,
	)
	go forgetTo(
		ctx,
		inst,
		"ws",
		forgot,
	)
	select {
	case err := <-forgot:
		t.Fatalf(
			"Forget finished (%v) while an auto-snapshot of the aggregate was being written",
			err,
		)
	case <-time.After(50 * time.Millisecond):
	}

	close(snaps.release)
	if err := within(
		t,
		forgot,
		"Forget",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	within(
		t,
		got,
		"Get",
	)

	if exists, _ := inst.Exists(
		ctx,
		"ws",
	); exists {
		t.Fatal("the forgotten aggregate exists again")
	}

	if _, err := inst.Get(
		ctx,
		"ws",
	); !errors.Is(
		err,
		failure.ErrNotFound,
	) {
		t.Fatalf(
			"Get after Forget = %v, want ErrNotFound",
			err,
		)
	}

	if evt, err := inst.SendWait(
		ctx,
		mocks.CreateOrderCmd{ID: "ws", Total: 1},
	); err != nil ||
		evt.Version != 1 {
		t.Fatalf(
			"re-create = v%d, %v; want v1",
			evt.Version,
			err,
		)
	}
}

type blockingHandler struct {
	entered  chan struct{}
	release  chan struct{}
	finished atomic.Bool
}

func (b *blockingHandler) handle(
	context.Context,
	event.Event[mocks.Order],
) {
	b.entered <- struct{}{}
	<-b.release
	b.finished.Store(true)
}

func TestSendWait_RetryOfAnIdempotentCommandWaitsForTheOriginalHandlers(
	t *testing.T,
) {
	ctx := context.Background()
	inst := build(
		t,
		eight(store.New()),
	)
	blocker := &blockingHandler{
		entered: make(
			chan struct{},
			1,
		),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(blocker.release) }) })
	if _, err := inst.Subscribe(
		"OrderCreated",
		blocker.handle,
	); err != nil {
		t.Fatal(err)
	}

	cmd := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: 1}, Cmd: "cmd-1"}

	short, cancel := context.WithTimeout(
		ctx,
		50*time.Millisecond,
	)
	defer cancel()
	if _, err := inst.SendWait(
		short,
		cmd,
	); !errors.Is(
		err,
		failure.ErrContextCancelled,
	) {
		t.Fatalf(
			"first SendWait = %v, want ErrContextCancelled (committed, handler still running)",
			err,
		)
	}

	within(
		t,
		blocker.entered,
		"the handler",
	)

	retried := make(
		chan error,
		1,
	)
	go sendWaitIntoChannel(
		ctx,
		inst,
		retried,
		cmd,
	)
	select {
	case err := <-retried:
		t.Fatalf(
			"the retry returned (%v) before the original handler finished",
			err,
		)
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(blocker.release) })
	if err := within(
		t,
		retried,
		"the retry",
	); err != nil {
		t.Fatalf(
			"retry: %v",
			err,
		)
	}

	if !blocker.finished.Load() {
		t.Fatal("the retry returned before the handler finished")
	}
}

type bgForget struct {
	inst asynx.Asynx[mocks.Order]
	out  chan<- error
}

func (h *bgForget) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	h.out <- h.inst.Forget(
		context.Background(),
		e.AggregateID,
	)
	return nil
}

func boundedShutdown(
	inst asynx.Asynx[mocks.Order],
) {
	bounded, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()
	inst.Shutdown(bounded)
}

type fenceProbe struct {
	inst asynx.Asynx[mocks.Order]
}

func (p fenceProbe) raised() bool {
	_, err := p.inst.Send(
		context.Background(),
		upd(
			"agg",
			2,
		),
	)
	return errors.Is(
		err,
		failure.ErrForgetInProgress,
	)
}

func TestShutdown_ReleasesAHandlerWaitingOnItsOwnAggregatesForget(
	t *testing.T,
) {
	ctx := context.Background()
	forgetResult := make(
		chan error,
		1,
	)
	handler := &bgForget{out: forgetResult}
	inst, err := eight(store.New()).
		WithSubscriber(
			"OrderCreated",
			handler.handle,
		).Build()
	if err != nil {
		t.Fatal(err)
	}

	handler.inst = inst
	t.Cleanup(func() { boundedShutdown(inst) })
	if _, err := inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "agg", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	eventually(
		t,
		"the Forget to raise its fence",
		fenceProbe{inst: inst}.raised,
	)

	shutdownOrFatal(
		t,
		inst,
		"Shutdown",
	)

	err = within(
		t,
		forgetResult,
		"the Forget",
	)
	if !errors.Is(
		err,
		failure.ErrForgetFailed,
	) ||
		!errors.Is(
			err,
			failure.ErrShuttingDown,
		) {
		t.Fatalf(
			"Forget = %v, want ErrForgetFailed wrapping ErrShuttingDown",
			err,
		)
	}
}

type errDeadSink struct {
	got chan<- error
}

func (d errDeadSink) handle(
	_ context.Context,
	_ event.Event[mocks.Order],
	err error,
) error {
	d.got <- err
	return nil
}

func failWith(
	err error,
) func(context.Context, event.Event[mocks.Order]) error {
	return func(
		context.Context,
		event.Event[mocks.Order],
	) error {
		return err
	}
}

func TestFailure_DeadLetterLearnsEverySubscriberThatFailed(
	t *testing.T,
) {
	got := make(
		chan error,
		1,
	)
	billing, search := errors.New("billing down"), errors.New("search down")
	inst := build(
		t,
		eight(store.New()).
			WithDeliveryMaxAttempts(2).
			WithDeliveryRetry(
				time.Millisecond,
				time.Millisecond,
			).
			WithDeadLetterHandler(errDeadSink{got: got}.handle).
			WithSubscriber(
				"OrderUpdated",
				failWith(billing),
			).
			WithSubscriber(
				"OrderUpdated",
				failWith(search),
			),
	)
	sendUpd(
		t,
		inst,
		"agg",
		1,
	)

	err := within(
		t,
		got,
		"the dead letter",
	)
	if !errors.Is(
		err,
		billing,
	) ||
		!errors.Is(
			err,
			search,
		) {
		t.Fatalf(
			"dead-letter handler received %q, want both failures",
			err,
		)
	}

	if !strings.Contains(
		err.Error(),
		"OrderUpdated#1",
	) ||
		!strings.Contains(
			err.Error(),
			"OrderUpdated#2",
		) {
		t.Fatalf(
			"dead-letter handler received %q, want both subscribers named",
			err,
		)
	}

	var sub *failure.SubscriberError
	if !errors.As(
		err,
		&sub,
	) {
		t.Fatal("errors.As does not find a SubscriberError")
	}
}

type downFromV2 struct {
	down atomic.Bool
}

func (d *downFromV2) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	if e.Version >= 2 &&
		d.down.Load() {
		return errors.New("downstream down")
	}

	return nil
}

func TestSendWait_RetryOnAParkedAggregateRepeatsWhatTheFirstAttemptWasTold(
	t *testing.T,
) {
	ctx := context.Background()
	handler := &downFromV2{}
	handler.down.Store(true)
	inst := build(
		t,
		eight(store.New()).
			WithDeliveryMaxAttempts(1).
			WithSubscriber(
				"OrderUpdated",
				handler.handle,
			),
	)
	delivered := idemCmd{UpdateOrderCmd: upd(
		"agg",
		1,
	), Cmd: "c-1"}
	causedThePark := idemCmd{UpdateOrderCmd: upd(
		"agg",
		2,
	), Cmd: "c-2"}
	leftBehind := idemCmd{UpdateOrderCmd: upd(
		"agg",
		3,
	), Cmd: "c-3"}
	if _, err := inst.SendWait(
		ctx,
		delivered,
	); err != nil {
		t.Fatalf(
			"delivered: %v",
			err,
		)
	}

	if _, err := inst.Send(
		ctx,
		causedThePark,
	); err != nil {
		t.Fatal(err)
	}

	eventually(
		t,
		"the aggregate to park",
		pausedCount{inst: inst, want: 1}.reached,
	)
	if _, err := inst.SendWait(
		ctx,
		leftBehind,
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"first SendWait on a parked aggregate = %v, want ErrDeliveryParked",
			err,
		)
	}

	if _, err := inst.SendWait(
		ctx,
		delivered,
	); err != nil {
		t.Errorf(
			"retry of a command delivered before the park = %v, want nil",
			err,
		)
	}

	if _, err := inst.SendWait(
		ctx,
		causedThePark,
	); err != nil {
		t.Errorf(
			"retry of the command whose own handler failed = %v, want nil: the handler ran",
			err,
		)
	}

	if _, err := inst.SendWait(
		ctx,
		leftBehind,
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Errorf(
			"retry of a command left undelivered = %v, want ErrDeliveryParked",
			err,
		)
	}

	handler.down.Store(false)
	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatal(err)
	}

	inst.WaitPublish()
	if _, err := inst.SendWait(
		ctx,
		leftBehind,
	); err != nil {
		t.Errorf(
			"retry after the park lifted and the handler ran = %v, want nil",
			err,
		)
	}
}

func newBenchInstance(
	b *testing.B,
) asynx.Asynx[mocks.Order] {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().
		WithEventStore(s).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 8, QueueDepth: 10000}).
		Build()
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { instance.Shutdown(context.Background()) })
	return instance
}

func BenchmarkSend_SingleSerial(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf(
			"order-%d",
			i,
		)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(
			context.Background(),
			cmd,
		)
	}
}

type benchDifferent struct {
	instance asynx.Asynx[mocks.Order]
	n        int
}

func (d benchDifferent) run(
	pb *testing.PB,
) {
	i := 0
	for pb.Next() {
		orderID := fmt.Sprintf(
			"order-%d-%d",
			i,
			d.n,
		)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		d.instance.Send(
			context.Background(),
			cmd,
		)
		i++
	}
}

func BenchmarkSend_Parallel_DifferentAggregates(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	b.RunParallel(benchDifferent{instance: instance, n: b.N}.run)
}

type benchSame struct {
	instance asynx.Asynx[mocks.Order]
}

func (s benchSame) run(
	pb *testing.PB,
) {
	for pb.Next() {
		cmd := mocks.UpdateOrderCmd{
			ID:       "order-1",
			NewState: mocks.Order{ID: "order-1", Total: 100.0},
		}
		s.instance.Send(
			context.Background(),
			cmd,
		)
	}
}

func BenchmarkSend_Parallel_SameAggregate(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	b.RunParallel(benchSame{instance: instance}.run)
}

func BenchmarkGet_WarmPath(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(
			context.Background(),
			"order-1",
		)
	}
}

func BenchmarkGet_ColdPath(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	orderID := "order-1"
	cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	for i := 0; i < 50; i++ {
		upCmd := mocks.UpdateOrderCmd{
			ID:       orderID,
			NewState: mocks.Order{ID: orderID, Total: float64((i + 2) * 100.0)},
		}
		instance.Send(
			context.Background(),
			upCmd,
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(
			context.Background(),
			orderID,
		)
	}
}

func BenchmarkGet_WithSnapshot(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	orderID := "order-1"
	cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(
			context.Background(),
			orderID,
		)
	}
}

func BenchmarkPublish_SingleHandler(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	handler := func(
		ctx context.Context,
		evt event.Event[mocks.Order],
	) {
	}
	subID, _ := instance.Subscribe(
		"OrderCreated",
		handler,
	)
	defer instance.Unsubscribe(subID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf(
			"order-%d",
			i,
		)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(
			context.Background(),
			cmd,
		)
	}
}

func BenchmarkPublish_FiveHandlers(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	handler := func(
		ctx context.Context,
		evt event.Event[mocks.Order],
	) {
	}
	ids := make(
		[]string,
		5,
	)
	for i := 0; i < 5; i++ {
		id, _ := instance.Subscribe(
			"OrderCreated",
			handler,
		)
		ids[i] = id
	}

	defer unsubscribeAll(
		instance,
		ids,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf(
			"order-%d",
			i,
		)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(
			context.Background(),
			cmd,
		)
	}
}

func BenchmarkPublish_HandlerWithTimeout(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	handler := func(
		ctx context.Context,
		evt event.Event[mocks.Order],
	) {
	}
	subID, _ := instance.Subscribe(
		"OrderCreated",
		handler,
	)
	defer instance.Unsubscribe(subID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf(
			"order-%d",
			i,
		)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(
			context.Background(),
			cmd,
		)
	}
}

func BenchmarkShutdown_NoInFlight(
	b *testing.B,
) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		instance := newBenchInstance(b)
		cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
		instance.Send(
			context.Background(),
			cmd,
		)
		b.StartTimer()

		instance.Shutdown(context.Background())
	}
}

func spawnSends(
	instance asynx.Asynx[mocks.Order],
	n int,
) {
	for j := 0; j < n; j++ {
		orderID := fmt.Sprintf(
			"order-%d",
			j,
		)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		go instance.Send(
			context.Background(),
			cmd,
		)
	}
}

func BenchmarkShutdown_WithInFlight(
	b *testing.B,
) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		instance := newBenchInstance(b)
		spawnSends(
			instance,
			100,
		)

		b.StartTimer()

		instance.Shutdown(context.Background())
	}
}

func BenchmarkAllocations_Send(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		orderID := fmt.Sprintf(
			"order-%d",
			i,
		)
		cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
		instance.Send(
			context.Background(),
			cmd,
		)
	}
}

func BenchmarkAllocations_Subscribe(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	handler := func(
		ctx context.Context,
		evt event.Event[mocks.Order],
	) {
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id, _ := instance.Subscribe(
			"OrderCreated",
			handler,
		)
		instance.Unsubscribe(id)
	}
}

func BenchmarkAllocations_Get(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	cmd := mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}
	instance.Send(
		context.Background(),
		cmd,
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instance.Get(
			context.Background(),
			"order-1",
		)
	}
}

type cancelCounter struct {
	mu        sync.Mutex
	successes int
	rejected  []error
}

func (c *cancelCounter) attempt(
	ctx context.Context,
	instance asynx.Asynx[mocks.Order],
	start <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	<-start
	_, err := instance.Send(
		ctx,
		mocks.CancelOrderCmd{ID: "X"},
	)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.rejected = append(
			c.rejected,
			err,
		)
		return
	}

	c.successes++
}

func (c *cancelCounter) expectValidationRejections(
	t *testing.T,
) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	unexpected := slices.DeleteFunc(
		slices.Clone(c.rejected),
		isValidationError,
	)
	if len(unexpected) != 0 {
		t.Errorf(
			"cancels were rejected with %v, want only ErrValidation",
			errors.Join(unexpected...),
		)
	}
}

func isValidationError(
	err error,
) bool {
	return errors.Is(
		err,
		failure.ErrValidation,
	)
}

func countConcurrentCancelSuccesses(
	t *testing.T,
	workersPerShard int,
	attempts int,
) int {
	t.Helper()

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, QueueDepth: 4096, WorkersPerShard: workersPerShard}).
		Build()
	if err != nil {
		t.Fatalf(
			"build: %v",
			err,
		)
	}

	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, err := instance.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "X", Total: 100},
	); err != nil {
		t.Fatalf(
			"seed create: %v",
			err,
		)
	}

	var (
		wg      sync.WaitGroup
		counter = &cancelCounter{}
		start   = make(chan struct{})
	)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go counter.attempt(
			ctx,
			instance,
			start,
			&wg,
		)
	}

	close(start)
	wg.Wait()
	counter.expectValidationRejections(t)

	counter.mu.Lock()
	defer counter.mu.Unlock()

	return counter.successes
}

func requireOneCancel(
	t *testing.T,
	workers int,
) {
	t.Helper()

	got := countConcurrentCancelSuccesses(
		t,
		workers,
		64,
	)
	if got != 1 {
		t.Errorf(
			"WorkersPerShard=%d: %d cancels accepted, want exactly 1",
			workers,
			got,
		)
	}
}

func TestConcurrentCancel_AcceptsExactlyOne(
	t *testing.T,
) {
	for _, workers := range []int{1, 8} {
		requireOneCancel(
			t,
			workers,
		)
	}
}

type failBad struct {
	err error
}

func (f failBad) handle(
	_ context.Context,
	e event.Event[mocks.Order],
) error {
	if e.AggregateID == "bad" {
		return f.err
	}

	return nil
}

func TestAsynx_InspectDelivery_ShowsWhyAnAggregateIsParked(
	t *testing.T,
) {
	ctx := context.Background()
	readModelDown := errors.New("read model down")
	parked := make(
		chan error,
		8,
	)
	inst := build(
		t,
		eight(store.New()).
			WithDeliveryRetry(
				time.Millisecond,
				2*time.Millisecond,
			).
			WithDeliveryMaxAttempts(3).
			WithDeliveryErrorHandler(parkReporter{parked: parked}.handle).
			WithSubscriber(
				"OrderUpdated",
				failBad{err: readModelDown}.handle,
			),
	)

	sendUpd(
		t,
		inst,
		"bad",
		1,
	)

	within(
		t,
		parked,
		"park report",
	)
	if _, err := inst.SendWait(
		ctx,
		upd(
			"bad",
			2,
		),
	); !errors.Is(
		err,
		failure.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait behind the park = %v, want ErrDeliveryParked",
			err,
		)
	}

	got := inst.InspectDelivery("bad")
	if !got.Parked ||
		got.Generation == 0 {
		t.Fatalf(
			"InspectDelivery = %+v, want parked with a generation",
			got,
		)
	}

	if !errors.Is(
		got.Cause,
		failure.ErrDeliveryParked,
	) ||
		!errors.Is(
			got.Cause,
			readModelDown,
		) {
		t.Fatalf(
			"Cause = %v, want ErrDeliveryParked wrapping %v",
			got.Cause,
			readModelDown,
		)
	}

	if !slices.Contains(
		got.Undelivered,
		2,
	) {
		t.Fatalf(
			"Undelivered = %v, want v2, which no handler took",
			got.Undelivered,
		)
	}

	if other := inst.InspectDelivery("other"); !reflect.DeepEqual(
		other,
		dispatch.Status{},
	) {
		t.Fatalf(
			"InspectDelivery of an unknown aggregate = %+v, want the zero status",
			other,
		)
	}
}
