package asynx_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
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

	t.Cleanup(func() { instance.Shutdown(context.Background()) })
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

	if err != models.ErrValidation {
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

	if err != models.ErrContextCancelled {
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

	if err != models.ErrShuttingDown {
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

	if err != models.ErrNotFound {
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
		evt models.Event[mocks.Order],
	) {
	}
	_, err := instance.Subscribe(
		"",
		handler,
	)
	if err == nil {
		t.Fatal("expected ErrEmptyPattern, got nil")
	}

	if err != models.ErrEmptyPattern {
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
	var nilHandler models.ProjectionHandler[mocks.Order]
	_, err := instance.Subscribe(
		"OrderCreated",
		nilHandler,
	)
	if err == nil {
		t.Fatal("expected ErrNilHandler, got nil")
	}

	if err != models.ErrNilHandler {
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
		evt models.Event[mocks.Order],
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
		evt models.Event[mocks.Order],
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

	count := 0
	fn := func(
		ctx context.Context,
		evt models.Event[mocks.Order],
	) {
		count++
	}
	err := instance.Replay(
		context.Background(),
		"order-1",
		1,
		0,
		fn,
	)
	if err != nil {
		t.Fatalf(
			"Replay failed: %v",
			err,
		)
	}

	if count == 0 {
		t.Fatal("expected at least one event, got 0")
	}
}

func TestReplay_EmptyRange(
	t *testing.T,
) {
	instance := newInstance(t)
	count := 0
	fn := func(
		ctx context.Context,
		evt models.Event[mocks.Order],
	) {
		count++
	}
	err := instance.Replay(
		context.Background(),
		"nonexistent",
		1,
		0,
		fn,
	)
	if err != nil {
		t.Fatalf(
			"Replay failed: %v",
			err,
		)
	}

	if count != 0 {
		t.Fatalf(
			"expected 0 events for nonexistent aggregate, got %d",
			count,
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

	if err != models.ErrAlreadyShuttingDown {
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

	handlerCalled := atomic.Int32{}
	handler := func(
		ctx context.Context,
		evt models.Event[mocks.Order],
	) {
		handlerCalled.Add(1)
	}

	subID, err := instance.Subscribe(
		"OrderCreated",
		handler,
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

	if handlerCalled.Load() == 0 {
		t.Fatal("handler was not called")
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
		go func(
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
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			t.Fatalf(
				"Send failed: %v",
				err,
			)
		}
	}

	for i := 0; i < numGoroutines; i++ {
		orderID := fmt.Sprintf(
			"order-%d",
			i,
		)
		exists, err := instance.Exists(
			context.Background(),
			orderID,
		)
		if err != nil {
			t.Fatalf(
				"Exists failed: %v",
				err,
			)
		}

		if !exists {
			t.Fatalf(
				"aggregate %s not found",
				orderID,
			)
		}
	}
}

func TestIntegration_MultipleProjections(
	t *testing.T,
) {
	instance := newInstance(t)

	counters := [4]atomic.Int32{}
	handlers := []func(context.Context, models.Event[mocks.Order]){
		func(
			ctx context.Context,
			evt models.Event[mocks.Order],
		) {
			counters[0].Add(1)
		},
		func(
			ctx context.Context,
			evt models.Event[mocks.Order],
		) {
			counters[1].Add(1)
		},
		func(
			ctx context.Context,
			evt models.Event[mocks.Order],
		) {
			counters[2].Add(1)
		},
		func(
			ctx context.Context,
			evt models.Event[mocks.Order],
		) {
			counters[3].Add(1)
		},
	}

	ids := make(
		[]string,
		4,
	)
	for i, h := range handlers {
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

		ids[i] = id
	}

	defer func() {
		for _, id := range ids {
			instance.Unsubscribe(id)
		}
	}()

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
		if counters[i].Load() == 0 {
			t.Fatalf(
				"handler %d was not called",
				i,
			)
		}
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

	if err != models.ErrShuttingDown {
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
		func(
			_ context.Context,
			_ models.Event[mocks.Order],
		) {
			handlerDone.Add(1)
		},
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
			func(
				_ context.Context,
				_ models.Event[mocks.Order],
			) {
				count.Add(1)
			},
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
	if err != models.ErrValidation {
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
	if err != models.ErrContextCancelled {
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
	if err != models.ErrShuttingDown {
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
		models.ErrValidation,
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
		models.ErrNotFound,
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

	var gotEvent models.Event[mocks.Order]
	if _, err := instance.OnForget(func(
		_ context.Context,
		e models.Event[mocks.Order],
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
		models.ErrShuttingDown,
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
		e models.Event[mocks.Order],
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

func (s *deleteErrStore) Atomically(
	ctx context.Context,
	fn func(models.Tx) error,
) error {
	return s.Memory.Atomically(
		ctx,
		func(
			tx models.Tx,
		) error {
			return fn(&deleteErrTx{Tx: tx, owner: s})
		},
	)
}

type deleteErrTx struct {
	models.Tx
	owner *deleteErrStore
}

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
		models.ErrForgetFailed,
	) {
		t.Fatalf(
			"expected ErrForgetFailed, got %v",
			err,
		)
	}
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
		go func(
			idx int,
		) {
			defer wg.Done()
			errs[idx] = instance.Forget(
				ctx,
				"order-1",
			)
		}(i)
	}

	wg.Wait()

	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		} else if !errors.Is(
			err,
			models.ErrValidation,
		) &&
			!errors.Is(
				err,
				models.ErrPipelineFailed,
			) &&
			!errors.Is(
				err,
				models.ErrForgetInProgress,
			) {
			t.Errorf(
				"unexpected error from concurrent Forget: %v",
				err,
			)
		}
	}

	_, getErr := instance.Get(
		ctx,
		"order-1",
	)
	if !errors.Is(
		getErr,
		models.ErrNotFound,
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
		_ models.Event[mocks.Order],
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
	if err != models.ErrEmptyPattern {
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
		[]models.Event[mocks.Order],
		2,
	)
	errs := make(
		[]error,
		2,
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0], errs[0] = instance.SubscribeWait(
			ctx,
			"OrderCreated",
		)
	}()
	go func() {
		defer wg.Done()
		results[1], errs[1] = instance.SubscribeWait(
			ctx,
			"OrderCancelled",
		)
	}()

	stop := feedOrders(
		instance,
		"conc",
		func(
			id string,
		) models.Command[mocks.Order] {
			return mocks.CancelOrderCmd{ID: id}
		},
	)
	defer stop()

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf(
				"SubscribeWait[%d]: %v",
				i,
				err,
			)
		}
	}

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

func TestWaitPublish_NoRaceWithConcurrentDispatch(
	t *testing.T,
) {
	for iter := 0; iter < 100; iter++ {
		func() {
			instance := newInstance(t)

			const n = 50
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(
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
				}(i)
			}

			wg.Wait()

			waitPublish(
				t,
				instance,
			)
		}()
	}
}

func TestWaitPublish_RaceWithAsyncReactionGoroutines(
	t *testing.T,
) {
	for iter := 0; iter < 100; iter++ {
		func() {
			instance := newInstance(t)

			var reactionStarted atomic.Int32
			var reactionFinished atomic.Int32

			handlerID, err := instance.Subscribe(
				"OrderCreated",
				func(
					ctx context.Context,
					evt models.Event[mocks.Order],
				) {
					reactionStarted.Store(1)
					defer func() { reactionFinished.Store(1) }()

					go func() {
						time.Sleep(1 * time.Millisecond)

						cmd := mocks.UpdateOrderCmd{
							ID:       evt.AggregateID,
							NewState: mocks.Order{ID: evt.AggregateID, Total: 999.0, Status: "Updated"},
						}
						instance.Send(
							ctx,
							cmd,
						)
					}()
				},
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

			if reactionStarted.Load() == 0 {
				t.Logf(
					"Iteration %d: reaction never started",
					iter,
				)
			}

			if reactionFinished.Load() == 0 {
				t.Logf(
					"Iteration %d: reaction never finished",
					iter,
				)
			}
		}()
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
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
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
			func(
				_ context.Context,
				e models.Event[mocks.Order],
			) error {
				seen.add(e.Version)
				if e.Version == 1 {
					attempted <- struct{}{}
					return errors.New("poison")
				}

				return nil
			},
		).Build()
	if err != nil {
		t.Fatal(err)
	}

	for i := range 2 {
		if _, err := first.Send(
			ctx,
			upd(
				"agg",
				float64(i+1),
			),
		); err != nil {
			t.Fatal(err)
		}
	}

	within(
		t,
		attempted,
		"first attempt",
	)

	sctx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()
	if err := first.Shutdown(sctx); err != nil {
		t.Fatalf(
			"Shutdown with a retrying delivery: %v",
			err,
		)
	}

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
			func(
				_ context.Context,
				e models.Event[mocks.Order],
			) error {
				after.add(e.Version)
				return nil
			},
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
			func(
				_ context.Context,
				e models.Event[mocks.Order],
			) error {
				if strings.HasPrefix(
					e.AggregateID,
					"bad",
				) &&
					e.EventName != models.ForgetEventName {
					attempted <- struct{}{}
					return errors.New("poison")
				}

				return nil
			},
		).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := inst.Send(
		ctx,
		upd(
			"bad",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

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
	go func() {
		_, err := inst.SendWait(
			wctx,
			upd(
				"bad",
				2,
			),
		)
		waited <- err
	}()
	eventually(
		t,
		"the second command to commit",
		func() bool {
			select {
			case err := <-waited:
				t.Fatalf(
					"SendWait returned %v before committing",
					err,
				)
			default:
			}

			blobs, _ := s.ReadFrom(
				ctx,
				"events:bad",
				1,
			)
			return len(blobs) == 2
		},
	)
	cancelWait()
	if err := within(
		t,
		waited,
		"SendWait to give up",
	); !errors.Is(
		err,
		models.ErrContextCancelled,
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

	if _, err := inst.Send(
		ctx,
		upd(
			"bad2",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	within(
		t,
		attempted,
		"second poison attempt",
	)
	sctx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()
	if err := inst.Shutdown(sctx); err != nil {
		t.Fatalf(
			"Shutdown with a poison delivery: %v",
			err,
		)
	}
}

func TestFailure_RetriesStopAndTheAggregateIsParkedUntilResumed(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()

	var healthy bool
	var mu sync.Mutex
	var seen versions
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
			WithDeliveryErrorHandler(func(
				_ context.Context,
				_ models.Event[mocks.Order],
				err error,
				_ int,
			) {
				if errors.Is(
					err,
					models.ErrDeliveryParked,
				) {
					parked <- err
				}
			}).
			WithSubscriber(
				"OrderUpdated",
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					mu.Lock()
					ok := healthy || e.AggregateID != "bad"
					mu.Unlock()
					if !ok {
						return errors.New("read model down")
					}

					if e.AggregateID == "bad" {
						seen.add(e.Version)
					}

					return nil
				},
			),
	)

	for i := range 2 {
		if _, err := inst.Send(
			ctx,
			upd(
				"bad",
				float64(i+1),
			),
		); err != nil {
			t.Fatal(err)
		}
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
		models.ErrDeliveryParked,
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

	if got := seen.list(); len(got) != 0 {
		t.Fatalf(
			"parked aggregate delivered %v",
			got,
		)
	}

	pending, _ := s.PendingDeliveries(
		ctx,
		10,
	)
	nBad := 0
	for _, p := range pending {
		if p.AggregateID == "bad" {
			nBad++
		}
	}

	if nBad != 3 {
		t.Fatalf(
			"pending deliveries of the parked aggregate = %d, want 3 (%v)",
			nBad,
			pending,
		)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
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
	if got := seen.list(); !equalVersions(
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

func TestFailure_DeadLetterSkipsThePoisonEvent(
	t *testing.T,
) {
	ctx := context.Background()
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
			WithDeadLetterHandler(func(
				_ context.Context,
				e models.Event[mocks.Order],
				_ error,
			) error {
				dead <- e.Version
				return nil
			}).
			WithSubscriber(
				"OrderUpdated",
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					if e.Version == 1 {
						return errors.New("poison")
					}

					seen.add(e.Version)
					return nil
				},
			),
	)
	for i := range 3 {
		if _, err := inst.Send(
			ctx,
			upd(
				"agg",
				float64(i+1),
			),
		); err != nil {
			t.Fatal(err)
		}
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
		e models.Event[mocks.Order],
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
		models.ErrForgetInProgress,
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
		models.ErrForgetInProgress,
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

func TestFailure_DurableSubscribersReceiveTheDeletionMarker(
	t *testing.T,
) {
	ctx := context.Background()
	type key struct {
		id string
		v  int64
	}
	var mu sync.Mutex
	seen := map[key]bool{}
	var applied []int64
	var markers int

	inst := build(
		t,
		eight(store.New()).
			WithSubscriber(
				"OrderUpdated",
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					mu.Lock()
					defer mu.Unlock()
					if e.EventName == models.ForgetEventName {
						markers++
						for k := range seen {
							if k.id == e.AggregateID {
								delete(
									seen,
									k,
								)
							}
						}

						return nil
					}

					k := key{e.AggregateID, e.Version}
					if seen[k] {
						return nil
					}

					seen[k] = true
					applied = append(
						applied,
						e.Version,
					)
					return nil
				},
			),
	)

	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := inst.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			2,
		),
	); err != nil {
		t.Fatal(err)
	}

	waitPublish(
		t,
		inst,
	)

	mu.Lock()
	defer mu.Unlock()
	if markers != 1 {
		t.Fatalf(
			"deletion markers seen = %d, want 1 (pattern OrderUpdated does not match it, it is delivered anyway)",
			markers,
		)
	}

	if !equalVersions(
		applied,
		[]int64{1, 1},
	) {
		t.Fatalf(
			"applied versions = %v, want the re-created aggregate's v1 applied again",
			applied,
		)
	}
}

func TestFailure_ForgetFailsWhenADurableSubscriberCannotTakeTheMarker(
	t *testing.T,
) {
	ctx := context.Background()
	var healthy bool
	var mu sync.Mutex
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
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					mu.Lock()
					defer mu.Unlock()
					if e.EventName == models.ForgetEventName &&
						!healthy {
						return errors.New("cannot drop state")
					}

					return nil
				},
			),
	)
	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := inst.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		models.ErrForgetFailed,
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
		models.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait after a failed Forget = %v, want ErrDeliveryParked",
			err,
		)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
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

type failingLiveBus struct{ models.Bus[mocks.Order] }

func (failingLiveBus) PublishSync(
	context.Context,
	models.Event[mocks.Order],
) error {
	return errors.New("live bus down")
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
			WithPublishErrorHandler(func(
				_ context.Context,
				_ models.Event[mocks.Order],
				err error,
			) {
				reported <- err
			}).
			WithSubscriber(
				"OrderUpdated",
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					seen.add(e.Version)
					return nil
				},
			),
	)
	for i := range 3 {
		if _, err := inst.SendWait(
			ctx,
			upd(
				"agg",
				float64(i+1),
			),
		); err != nil {
			t.Fatal(err)
		}
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

func TestFailure_FailedForgetDoesNotLetLaterEventsOvertakeTheMarker(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	var mu sync.Mutex
	healthy := false
	var delivered []string
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
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					mu.Lock()
					defer mu.Unlock()
					if !healthy {
						return errors.New("down")
					}

					delivered = append(
						delivered,
						fmt.Sprintf(
							"%s/%d",
							e.EventName,
							e.Version,
						),
					)
					return nil
				},
			),
	)

	if _, err := inst.Send(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

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
		models.ErrForgetFailed,
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
		models.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait = %v, want ErrDeliveryParked",
			err,
		)
	}

	mu.Lock()
	healthy = true
	if len(delivered) != 0 {
		t.Fatalf(
			"delivered %v while the marker was undelivered",
			delivered,
		)
	}

	mu.Unlock()

	if err := inst.ResumeDeliveries(ctx); err != nil {
		t.Fatal(err)
	}

	waitPublish(
		t,
		inst,
	)
	mu.Lock()
	defer mu.Unlock()
	if want := []string{models.ForgetEventName + "/2", "OrderUpdated/3"}; !slices.Equal(
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

func TestFailure_ResumeDoesNotReplayLiveSubscribers(
	t *testing.T,
) {
	s := store.New()
	ctx := context.Background()
	var mu sync.Mutex
	healthy := false
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
				func(
					context.Context,
					models.Event[mocks.Order],
				) error {
					mu.Lock()
					defer mu.Unlock()
					if !healthy {
						return errors.New("down")
					}

					return nil
				},
			),
	)
	if _, err := inst.Subscribe(
		"OrderUpdated",
		func(
			_ context.Context,
			e models.Event[mocks.Order],
		) {
			live.add(e.Version)
		},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := inst.Subscribe(
		"OrderCancelled",
		func(
			_ context.Context,
			e models.Event[mocks.Order],
		) {
			cancelled.add(e.Version)
		},
	); err != nil {
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

	eventually(
		t,
		"aggregate paused",
		func() bool { return len(inst.PausedAggregates()) == 1 },
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
		models.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait = %v, want ErrDeliveryParked",
			err,
		)
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
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

func TestFailure_HandlerForgettingItsOwnAggregateFailsFast(
	t *testing.T,
) {
	ctx := context.Background()
	var inst asynx.Asynx[mocks.Order]
	got := make(
		chan error,
		1,
	)
	inst = build(
		t,
		eight(store.New()).
			WithSubscriber(
				"OrderUpdated",
				func(
					ctx context.Context,
					e models.Event[mocks.Order],
				) error {
					got <- inst.Forget(
						ctx,
						e.AggregateID,
					)
					return nil
				},
			),
	)
	if _, err := inst.SendWait(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := within(
		t,
		got,
		"Forget from the handler",
	); !errors.Is(
		err,
		models.ErrForgetFromHandler,
	) {
		t.Fatalf(
			"Forget from a handler = %v, want ErrForgetFromHandler",
			err,
		)
	}

	if _, err := inst.SendWait(
		ctx,
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

	sctx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()
	if err := inst.Shutdown(sctx); err != nil {
		t.Fatalf(
			"Shutdown: %v",
			err,
		)
	}
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
				func(
					context.Context,
					models.Event[mocks.Order],
				) error {
					return errors.New("down")
				},
			),
	)
	if _, err := down.Send(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := down.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		models.ErrForgetFailed,
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
		models.ErrDeliveryParked,
	) {
		t.Fatalf(
			"SendWait = %v, want ErrDeliveryParked",
			err,
		)
	}

	sctx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()
	if err := down.Shutdown(sctx); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var delivered []string
	up := build(
		t,
		eight(s).
			WithSubscriber(
				"OrderUpdated",
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					mu.Lock()
					defer mu.Unlock()
					delivered = append(
						delivered,
						fmt.Sprintf(
							"%s/%d",
							e.EventName,
							e.Version,
						),
					)
					return nil
				},
			),
	)
	waitPublish(
		t,
		up,
	)
	mu.Lock()
	defer mu.Unlock()
	if want := []string{models.ForgetEventName + "/2", "OrderUpdated/3"}; !slices.Equal(
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

func feedOrders(
	instance asynx.Asynx[mocks.Order],
	prefix string,
	followUps ...func(id string) models.Command[mocks.Order],
) (stop func()) {
	quit := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		feedUntil(
			quit,
			instance,
			prefix,
			followUps,
		)
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(quit); <-finished })
	}
}

func feedUntil(
	quit <-chan struct{},
	instance asynx.Asynx[mocks.Order],
	prefix string,
	followUps []func(id string) models.Command[mocks.Order],
) {
	for i := 0; !isClosed(quit); i++ {
		id := fmt.Sprintf(
			"%s-%d",
			prefix,
			i,
		)
		if _, err := instance.SendWait(
			context.Background(),
			mocks.CreateOrderCmd{ID: id, Total: 1},
		); err != nil {
			return
		}

		for _, followUp := range followUps {
			instance.SendWait(
				context.Background(),
				followUp(id),
			)
		}
	}
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
	models.SnapshotStore
	aggregate string
	armed     atomic.Bool
	entered   chan struct{}
	release   chan struct{}
}

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

func TestGet_AutoSnapshotCannotResurrectAForgottenAggregate(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	old := build(
		t,
		eight(s),
	)
	for _, cmd := range []models.Command[mocks.Order]{mocks.CreateOrderCmd{ID: "ws", Total: 10}, upd(
		"ws",
		20,
	)} {
		if _, err := old.SendWait(
			ctx,
			cmd,
		); err != nil {
			t.Fatal(err)
		}
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
				func(
					_ context.Context,
					_ string,
					p []byte,
				) ([]byte, error) {
					return p,
						nil
				},
			),
	)
	snaps.armed.Store(true)
	got := make(
		chan error,
		1,
	)
	go func() {
		_, err := inst.Get(
			ctx,
			"ws",
		)
		got <- err
	}()
	within(
		t,
		snaps.entered,
		"the auto-snapshot write",
	)

	forgot := make(
		chan error,
		1,
	)
	go func() {
		forgot <- inst.Forget(
			ctx,
			"ws",
		)
	}()
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
		models.ErrNotFound,
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

func TestSendWait_RetryOfAnIdempotentCommandWaitsForTheOriginalHandlers(
	t *testing.T,
) {
	ctx := context.Background()
	inst := build(
		t,
		eight(store.New()),
	)
	entered := make(
		chan struct{},
		1,
	)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var finished atomic.Bool
	if _, err := inst.Subscribe(
		"OrderCreated",
		func(
			context.Context,
			models.Event[mocks.Order],
		) {
			entered <- struct{}{}
			<-release
			finished.Store(true)
		},
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
		models.ErrContextCancelled,
	) {
		t.Fatalf(
			"first SendWait = %v, want ErrContextCancelled (committed, handler still running)",
			err,
		)
	}

	within(
		t,
		entered,
		"the handler",
	)

	retried := make(
		chan error,
		1,
	)
	go func() {
		_, err := inst.SendWait(
			ctx,
			cmd,
		)
		retried <- err
	}()
	select {
	case err := <-retried:
		t.Fatalf(
			"the retry returned (%v) before the original handler finished",
			err,
		)
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
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

	if !finished.Load() {
		t.Fatal("the retry returned before the handler finished")
	}
}

func TestShutdown_ReleasesAHandlerWaitingOnItsOwnAggregatesForget(
	t *testing.T,
) {
	ctx := context.Background()
	forgetResult := make(
		chan error,
		1,
	)
	var inst asynx.Asynx[mocks.Order]
	inst, err := eight(store.New()).
		WithSubscriber(
			"OrderCreated",
			func(
				_ context.Context,
				e models.Event[mocks.Order],
			) error {
				forgetResult <- inst.Forget(
					context.Background(),
					e.AggregateID,
				)
				return nil
			},
		).Build()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		bounded, cancel := context.WithTimeout(
			ctx,
			2*time.Second,
		)
		defer cancel()
		inst.Shutdown(bounded)
	})
	if _, err := inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "agg", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	eventually(
		t,
		"the Forget to raise its fence",
		func() bool {
			_, err := inst.Send(
				ctx,
				upd(
					"agg",
					2,
				),
			)
			return errors.Is(
				err,
				models.ErrForgetInProgress,
			)
		},
	)

	shutdown, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()
	if err := inst.Shutdown(shutdown); err != nil {
		t.Fatalf(
			"Shutdown: %v",
			err,
		)
	}

	err = within(
		t,
		forgetResult,
		"the Forget",
	)
	if !errors.Is(
		err,
		models.ErrForgetFailed,
	) ||
		!errors.Is(
			err,
			models.ErrShuttingDown,
		) {
		t.Fatalf(
			"Forget = %v, want ErrForgetFailed wrapping ErrShuttingDown",
			err,
		)
	}
}

func TestFailure_DeadLetterLearnsEverySubscriberThatFailed(
	t *testing.T,
) {
	ctx := context.Background()
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
			WithDeadLetterHandler(func(
				_ context.Context,
				_ models.Event[mocks.Order],
				err error,
			) error {
				got <- err
				return nil
			}).
			WithSubscriber(
				"OrderUpdated",
				func(
					context.Context,
					models.Event[mocks.Order],
				) error {
					return billing
				},
			).
			WithSubscriber(
				"OrderUpdated",
				func(
					context.Context,
					models.Event[mocks.Order],
				) error {
					return search
				},
			),
	)
	if _, err := inst.Send(
		ctx,
		upd(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

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

	var sub *models.SubscriberError
	if !errors.As(
		err,
		&sub,
	) {
		t.Fatal("errors.As does not find a SubscriberError")
	}
}

func TestSendWait_RetryOnAParkedAggregateRepeatsWhatTheFirstAttemptWasTold(
	t *testing.T,
) {
	ctx := context.Background()
	var down atomic.Bool
	down.Store(true)
	inst := build(
		t,
		eight(store.New()).
			WithDeliveryMaxAttempts(1).
			WithSubscriber(
				"OrderUpdated",
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					if e.Version >= 2 &&
						down.Load() {
						return errors.New("downstream down")
					}

					return nil
				},
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
		func() bool { return len(inst.PausedAggregates()) == 1 },
	)
	if _, err := inst.SendWait(
		ctx,
		leftBehind,
	); !errors.Is(
		err,
		models.ErrDeliveryParked,
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
		models.ErrDeliveryParked,
	) {
		t.Errorf(
			"retry of a command left undelivered = %v, want ErrDeliveryParked",
			err,
		)
	}

	down.Store(false)
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

func BenchmarkSend_Parallel_DifferentAggregates(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	b.RunParallel(func(
		pb *testing.PB,
	) {
		i := 0
		for pb.Next() {
			orderID := fmt.Sprintf(
				"order-%d-%d",
				i,
				b.N,
			)
			cmd := mocks.CreateOrderCmd{ID: orderID, Total: 100.0}
			instance.Send(
				context.Background(),
				cmd,
			)
			i++
		}
	})
}

func BenchmarkSend_Parallel_SameAggregate(
	b *testing.B,
) {
	instance := newBenchInstance(b)
	b.ResetTimer()

	b.RunParallel(func(
		pb *testing.PB,
	) {
		for pb.Next() {
			cmd := mocks.UpdateOrderCmd{
				ID:       "order-1",
				NewState: mocks.Order{ID: "order-1", Total: 100.0},
			}
			instance.Send(
				context.Background(),
				cmd,
			)
		}
	})
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
		evt models.Event[mocks.Order],
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
		evt models.Event[mocks.Order],
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

	defer func() {
		for _, id := range ids {
			instance.Unsubscribe(id)
		}
	}()

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
		evt models.Event[mocks.Order],
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

func BenchmarkShutdown_WithInFlight(
	b *testing.B,
) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		instance := newBenchInstance(b)
		for j := 0; j < 100; j++ {
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
		evt models.Event[mocks.Order],
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
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		start     = make(chan struct{})
	)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := instance.Send(
				ctx,
				mocks.CancelOrderCmd{ID: "X"},
			); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()
	return successes
}

func TestConcurrentCancel_AcceptsExactlyOne(
	t *testing.T,
) {
	for _, workers := range []int{1, 8} {
		if got := countConcurrentCancelSuccesses(
			t,
			workers,
			64,
		); got != 1 {
			t.Errorf(
				"WorkersPerShard=%d: %d cancels accepted, want exactly 1",
				workers,
				got,
			)
		}
	}
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
			WithDeliveryErrorHandler(func(
				_ context.Context,
				_ models.Event[mocks.Order],
				err error,
				_ int,
			) {
				if errors.Is(
					err,
					models.ErrDeliveryParked,
				) {
					parked <- err
				}
			}).
			WithSubscriber(
				"OrderUpdated",
				func(
					_ context.Context,
					e models.Event[mocks.Order],
				) error {
					if e.AggregateID == "bad" {
						return readModelDown
					}

					return nil
				},
			),
	)

	if _, err := inst.Send(
		ctx,
		upd(
			"bad",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

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
		models.ErrDeliveryParked,
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
		models.ErrDeliveryParked,
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
		models.DeliveryStatus{},
	) {
		t.Fatalf(
			"InspectDelivery of an unknown aggregate = %+v, want the zero status",
			other,
		)
	}
}
